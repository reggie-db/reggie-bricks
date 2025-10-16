import asyncio
import hashlib
import json
import os
import shutil
import signal
from pathlib import Path
from typing import Any, Sequence

import psutil
import sh
from reggie_core import logs, paths, strs
from sh import RunningCommand

from reggie_app_runner import app_runner, caddy, conda, docker, git
from reggie_app_runner.app_runner import AppRunnerConfig, AppRunnerSource

LOG = logs.logger(__file__)


async def run():
    configs = list(app_runner.read_configs())
    caddy_config = _build_caddy_config(configs)
    caddy_proc = caddy.run(caddy_config, _bg=True, _bg_exc=False)
    procs = [caddy_proc]
    try:
        for config in configs:
            app_dir = _app_dir(config)
            procs.append(start(app_dir, config))
        LOG.info(f"all processes started: {list(p.pid for p in procs)}")
        caddy_proc.wait()
    finally:
        for proc in procs:
            LOG.info(f"shutting down process {proc.pid}")
            _shutdown_process(proc)


async def start(app_dir: Path, config: AppRunnerConfig) -> RunningCommand:
    source_dir = prepare(app_dir, config)
    env_name = f"app_{config.name}"
    await asyncio.to_thread(
        conda.update,
        env_name,
        *config.dependencies,
        pip_dependencies=config.pip_dependencies,
    )

    def _build_env(env: dict):
        home_dir = app_dir / "home"
        home_dir.mkdir(parents=True, exist_ok=True)
        env["HOME"] = str(home_dir)
        return env

    if type(config) == AppRunnerSource.DOCKER:
        env = _build_env(config.env(databricks=True))
        env_args = []
        for key, value in env.items():
            env_args.append("-e")
            env_args.append(f"{key}={value}")
        docker_commands = ["run"]
        if docker.path():
            docker_commands = docker_commands + ["-p", f"{config.port}:{config.port}"]
        docker_commands = docker_commands + env_args + [config.source]
        command = docker.command().bake(*docker_commands)
    else:
        env = _build_env(config.env(databricks=True, os_environ=True))
        command = conda.run(env_name)
    proc = command(
        *config.commands,
        _bg=True,
        _bg_exc=False,
        _cwd=source_dir,
        _env=env,
        _async=True,
    )
    return proc


def type(config: AppRunnerConfig) -> AppRunnerSource:
    source = config.source.strip() if config.source else None
    if source:
        if git.is_url(config.source):
            return AppRunnerSource.GITHUB
        else:
            return AppRunnerSource.DOCKER
    return AppRunnerSource.LOCAL


def hash(config: AppRunnerConfig) -> str:
    source_type = type(config)
    if source_type == AppRunnerSource.GITHUB:
        return git.remote_commit_hash(config.source, config.github_token)
    elif source_type == AppRunnerSource.DOCKER:
        return docker.image_hash(config.source)
    else:
        data = {
            "dependencies": config.dependencies,
            "pip_dependencies": config.pip_dependencies,
        }
        s = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(s.encode()).hexdigest()


def prepare(app_dir: Path, config: AppRunnerConfig) -> Path:
    source_type = type(config)
    source_hash = hash(config)
    source_dir = app_dir / f"source_{source_type}_{source_hash}"
    if source_dir.exists():
        shutil.rmtree(source_dir)
    source_dir.mkdir(parents=True, exist_ok=True)
    if source_type == AppRunnerSource.GITHUB:
        git.clone(config.source, source_dir, token=config.github_token)
    elif source_type == AppRunnerSource.DOCKER:
        # docker.pull(config.source)
        pass
    return source_dir


def _app_dir(config: AppRunnerConfig):
    dir_name = "app_runner"
    if not os.environ.get("DATABRICKS_WORKSPACE_ID"):
        dir_name = f".{dir_name}"
    root_dir = paths.home() / dir_name
    root_dir.mkdir(parents=True, exist_ok=True)
    return root_dir / config.name


def _build_caddy_config(
    configs: Sequence[AppRunnerConfig],
) -> dict[str, Any]:
    routes = []
    configs = sorted(configs, key=lambda x: len(x.path) * -1)
    for config in configs:
        handle = []
        if config.strip_path_prefix:
            handle.append(
                {"handler": "rewrite", "strip_path_prefix": config.path},
            )
        handle.append(
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": f"localhost:{config.port}"}],
            },
        )
        route = {
            "match": [{"path": [f"{config.path}*"]}],
            "handle": handle,
        }
        routes.append(route)

    config = {
        "admin": {"disabled": True, "config": {"persist": False}},
        "apps": {
            "http": {
                "servers": {
                    "srv0": {
                        "listen": [
                            f":{os.environ[app_runner.DATABRICKS_APP_PORT_ENV_VAR]}"
                        ],
                        "routes": routes,
                    }
                }
            }
        },
    }
    return config


def _shutdown_process(
    pid_or_proc: int | sh.RunningCommand,
    timeout: float = 10.0,
    kill_children: bool = True,
):
    # Resolve pid and psutil.Process
    if isinstance(pid_or_proc, int):
        pid = pid_or_proc
    else:
        pid = pid_or_proc.pid
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        LOG.info(f"process not found {pid}")
        return

    # Compute groups
    try:
        target_pgid = os.getpgid(pid)
    except ProcessLookupError:
        LOG.info(f"process not found {pid}")
        return

    python_pgid = os.getpgrp()

    LOG.info(
        f"terminating pid={pid}, target_pgid={target_pgid}, python_pgid={python_pgid}"
    )

    # Optionally collect children first to handle mixed groups
    children = []
    if kill_children:
        try:
            children.extend(proc.children(recursive=True))
        except psutil.Error:
            pass

    def _signal(sig: int):
        sent = False
        # If the child is in a different group than us, prefer a group signal
        if target_pgid != python_pgid:
            try:
                os.killpg(target_pgid, sig)
                sent = True
                LOG.info(f"sent {signal.Signals(sig).name} to pgid {target_pgid}")
            except ProcessLookupError:
                pass
            except PermissionError as e:
                LOG.info(f"killpg permission error: {e}. Falling back to pid")
        # Always also target the main pid in case it changed groups or already exited
        try:
            os.kill(pid, sig)
            sent = True
            LOG.info(f"sent {signal.Signals(sig).name} to pid {pid}")
        except ProcessLookupError:
            pass

        # Fan out to any discovered children that may live outside the pgid
        for ch in children:
            try:
                os.kill(ch.pid, sig)
            except ProcessLookupError:
                pass
        return sent

    # TERM then wait
    if _signal(signal.SIGTERM):
        try:
            proc.wait(timeout=timeout)
        except psutil.TimeoutExpired:
            pass
        except psutil.NoSuchProcess:
            return

    # KILL then wait
    if _signal(signal.SIGKILL):
        try:
            proc.wait(timeout=timeout)
        except psutil.TimeoutExpired:
            LOG.info(
                f"pid {pid} did not exit after {signal.Signals(signal.SIGKILL).name}"
            )
        except psutil.NoSuchProcess:
            pass

    # Final sweep for children
    for ch in children:
        try:
            ch.wait(timeout=0.1)
        except psutil.NoSuchProcess:
            pass
        except psutil.TimeoutExpired:
            try:
                os.kill(ch.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


if __name__ == "__main__":
    logs.auto_config()
    logs.logger().warning("info")
    os.environ["DATABRICKS_APP_PORT"] = "8000"
    os.environ["DATABRICKS_APP_NAME"] = "reggie_guy"
    os.environ["DATABRICKS_APP_RUNNER__DEFAULT__GITHUB_TOKEN"] = "default_token"
    os.environ["TEST"] = "root_replaced"
    os.environ["DATABRICKS_APP_RUNNER_TEST"] = "nested_replaced"
    os.environ["DATABRICKS_APP_RUNNER_ENV__PYTHONUNBUFFERED"] = "@format {this.test}"
    os.environ["DATABRICKS_APP_RUNNER_DEPENDENCIES"] = "ttyd"
    os.environ["DATABRICKS_APP_RUNNER_STRIP_PATH_PREFIX"] = "true"
    os.environ["DATABRICKS_APP_RUNNER_SOURCE"] = ""
    os.environ["DATABRICKS_APP_RUNNER_PATH"] = "/ttyd"
    shell = "bash"
    commands = [
        "ttyd",
        "-p",
        "@format {this.databricks_app_port}",
        "-i",
        "0.0.0.0",
        "--writable",
        shell,
    ]
    os.environ["DATABRICKS_APP_RUNNER_COMMANDS"] = f"@json {json.dumps(commands)}"
    for name in ["APP_FUN", "APP_ABC_COOL0_1"]:
        tokenized_name = "_".join(strs.tokenize(name))
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__SOURCE"] = "ealen/echo-server"
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__COMMANDS"] = ""
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__TEST"] = "app_replaced"
        env = {"test": "test", "PORT": "@format {this.databricks_app_port}"}
        os.environ[f"DATABRICKS_APP_RUNNER__{name}__ENV"] = f"@json {json.dumps(env)}"
    idx = -1
    try:
        sh.pkill("-9", "caddy")
        LOG.info("killed caddy")
    except sh.ErrorReturnCode:
        LOG.info("caddy not found")
    run()
