import asyncio
import functools
import logging
import os
import platform
import re
import subprocess
from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict
from urllib.request import urlretrieve

import sh
import yaml
from reggie_core import logs, paths

from reggie_app_runner import docker

_CONDA_DIR_NAME = ".miniforge3"
_CONDA_ENV_DEFAULT = "base"
_CONDA_DEPENDENCY_PATTERN = re.compile(
    r"^(?:[A-Za-z0-9_.-]+::)?(?P<name>[A-Za-z0-9_.-]+)"
)


@functools.cache
def path() -> Path:
    """Return the conda executable path. Install Miniforge to ~/.miniforge3 if needed."""
    conda_dir = paths.home() / _CONDA_DIR_NAME
    bin_path = conda_dir / "bin" / "conda"

    def _bin_path_valid():
        return bin_path.is_file() and os.access(bin_path, os.X_OK)

    if not _bin_path_valid():
        installer_dir = _install_conda(conda_dir)
        if not _bin_path_valid():
            raise ValueError(
                f"Failed to install conda - path:{bin_path} installer_dir:{installer_dir}"
            )
    logging.getLogger().info("Conda executable path: %s", bin_path)
    return bin_path


def command(conda_command, *args, **kwargs) -> sh.Command:
    args = [conda_command] + list(args)
    return sh.Command(path()).bake(*args, **kwargs)


def update(
    *dependencies: str,
    env_name: str = _CONDA_ENV_DEFAULT,
    pip_dependencies: Iterable[str] = None,
):
    """Create a conda environment with the given name and dependencies."""
    dependencies = list(dependencies)
    if pip_dependencies:
        if not any(d == "pip" for d in dependencies):
            dependencies.append("pip")
        dependencies.append({"pip": pip_dependencies})
    conda_env = {
        "name": env_name,
        "dependencies": dependencies,
    }

    with NamedTemporaryFile(mode="w", suffix=".yml") as f:
        conda_env_content = yaml.dump(conda_env)
        f.write(conda_env_content)
        f.flush()
        update_command = command("env", "update", "-f", f.name, "--prune")
        update_command()


def env(env_name: str = _CONDA_ENV_DEFAULT) -> dict[str, str]:
    output = str(command("run", "-n", env_name, "env", "-0")())
    env_pairs = output.strip("\0").split("\0")
    return dict(pair.split("=", 1) for pair in env_pairs if "=" in pair)


def env_command(env_name: str = _CONDA_ENV_DEFAULT) -> sh.Command:
    conda_env = env(env_name)
    return sh.bash.bake(
        "-c",
        '"$0" "$@";',
        _env=conda_env,
    )


def start(
    *args,
    env_name: str = _CONDA_ENV_DEFAULT,
    aliases: Dict[str, str] = None,
    **kwargs,
):
    args = list(args)
    if args and aliases:
        bash_command = '"$0" "$@";'
        for k, v in reversed(aliases.items()):
            bash_command = f"{k}() " + "{ " + str(v) + ' "$@"; }; ' + bash_command
        args = [
            "bash",
            "-c",
            bash_command,
        ] + args
    args = ["run", "-n", env_name] + args
    log = logs.logger("conda_run")
    kwargs.setdefault("_out", lambda line: log.info(f"{env_name} | {line.rstrip()}"))
    kwargs.setdefault("_err", lambda line: log.warning(f"{env_name} | {line.rstrip()}"))
    kwargs.setdefault("_bg", True)
    run_command = command(*args, **kwargs)
    return run_command()


def dependency_name(dependency: str) -> str:
    if dependency:
        match = _CONDA_DEPENDENCY_PATTERN.match(dependency)
        if match:
            return match.group("name")
    return None


def _install_conda(dir: Path) -> Path:
    """Download and run the Miniforge installer into the given directory, cached by URL."""
    url = _install_url()
    log = logs.logger("conda_install")

    def _run_installer(path: Path):
        installer_path = path / "installer.sh"
        log.info(f"Downloading Conda - url:{url} path:{installer_path}")
        urlretrieve(url, installer_path)
        installer_path.chmod(0o755)
        log.info(f"Installing Conda - path:{installer_path}")
        subprocess.run([installer_path, "-b", "-p", dir], check=True, text=True)

    return paths.cache_store(url + "v4", _run_installer)


def _install_url() -> str:
    """Construct the Miniforge installer URL for the current platform and arch."""
    sysname = platform.system()
    arch = platform.machine()

    arch_map = {
        "x86_64": "amd64",
        "aarch64": "arm64",
    }

    for k, v in arch_map.items():
        if k.casefold() == arch.casefold():
            arch = v
            break

    os_map = {
        "MacOSX": "Darwin",
    }

    for k, v in os_map.items():
        if k.casefold() == sysname.casefold():
            sysname = v
            break

    return f"https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-{sysname}-{arch}.sh"


async def main():
    print(dependency_name("caddy"))
    print(dependency_name("caddy::caddy"))
    print(dependency_name("cool::caddy=12"))
    deps = [
        "caddy",
        "curl",
    ]
    aliases = {}
    if not docker.path():
        deps.append("udocker")
    aliases["docker"] = docker.path() or "udocker"
    update(*deps, env_name="cool")
    start("caddy", "--version", env_name="cool").wait()
    start(
        "docker",
        "run",
        "hello-world",
        env_name="cool",
        aliases=aliases,
    ).wait()
    update("openjdk", env_name="cool", pip_dependencies=["requests"])
    start("java", "--version", env_name="cool").wait()


if __name__ == "__main__":
    print(sh.python3("-V"))
    asyncio.run(main())

    # conda = path()
    # print(json.dumps(env(include_os=False), indent=2))
    # install("caddy", "curl", "openjdk")
    # for commands in [
    #     ["conda", "info"],
    #     ["caddy", "version"],
    #     ["curl", "--version"],
    #     ["java", "--version"],
    #     ["bash", "-c", "echo $JAVA_HOME"],
    # ]:
    #     subprocess.run(commands, env=env(), check=True)

    # print(f"Conda executable: {conda}")
    # start(["conda", "info"]).wait()
