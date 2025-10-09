import concurrent
import hashlib
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pytimeparse.timeparse import timeparse
from reggie_core import logs

from reggie_app_runner import caddy, conda

LOG = logs.logger(__name__)


CONFIG_FILE_PATTERN = re.compile(r"^DATABRICKS_APP_CONFIG(?:_(\d+))?_ENV$")
CONFIG_KEY_PATTERN = re.compile(r"^DATABRICKS_APP_CONFIG(?:_(\d+))?_(.+)$")

DEFAULT_POLL_SECONDS = 60
WORK_ROOT = Path(os.getenv("DATABRICKS_APP_WORKDIR", ".dev-local")).resolve()
CADDY_DIR = WORK_ROOT / "caddy"
CADDYFILE_PATH = CADDY_DIR / "Caddyfile"
CADDY_RUN_SH = CADDY_DIR / "run_caddy.sh"
CADDY_PORT = int(os.getenv("DATABRICKS_APP_CADDY_PORT", "8000"))
CADDY_LOG_LEVEL_PATTERN = re.compile(r'"level"\s*:\s*"(\S+?)"', re.IGNORECASE)


def parse_duration(s: Optional[str]) -> int:
    """
    Parse a human-friendly duration string (e.g. "10s", "5m", "2h", "1h30m")
    into seconds using pytimeparse. Falls back to DEFAULT_POLL_SECONDS if
    input is None or unparsable.
    """
    s = s.strip().lower() if s else None
    if not s:
        return DEFAULT_POLL_SECONDS

    result = timeparse(s)
    if result is None:
        raise ValueError(f"Invalid duration: {s}")
    return int(result)


def load_kv_file(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if (v.startswith('"') and v.endswith('"')) or (
            v.startswith("'") and v.endswith("'")
        ):
            v = v[1:-1]
        env[k] = v
    return env


def discover_config_files(base: Path) -> Dict[int, Path]:
    configs: Dict[int, Path] = {}
    for p in base.iterdir():
        if not p.is_file():
            continue
        m = CONFIG_FILE_PATTERN.match(p.name)
        if not m:
            continue
        idx = int(m.group(1)) if m.group(1) is not None else 0
        configs[idx] = p
    return configs


def discover_config_envs() -> Dict[int, Dict[str, str]]:
    grouped: Dict[int, Dict[str, str]] = {}
    for k, v in os.environ.items():
        m = CONFIG_KEY_PATTERN.match(k)
        if not m:
            continue
        idx = int(m.group(1)) if m.group(1) is not None else 0
        key = m.group(2)
        grouped.setdefault(idx, {})[key] = v
    return grouped


def merge_envs(base: Dict[str, str], overlay: Dict[str, str]) -> Dict[str, str]:
    merged = dict(base)
    merged.update(overlay)
    return merged


def ensure_git_available() -> None:
    try:
        subprocess.run(
            ["git", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        LOG.error("git is required but not found in PATH")
        sys.exit(2)


def parse_repo_target(source: str, branch: Optional[str]) -> Tuple[str, str]:
    if "#" in source and not branch:
        source, branch = source.split("#", 1)
    return source, branch or "main"


def run_cmd(cmd_idx: int, cmd: str, cwd: Path, env: Dict[str, str]) -> subprocess.Popen:
    args = shlex.split(cmd)
    LOG.info(
        f"starting process {cmd_idx}: {args} in {cwd} port: {env.get('DATABRICKS_APP_PORT')}"
    )
    proc = subprocess.Popen(
        args,
        cwd=str(cwd),
        env=env,
        bufsize=1,
    )
    return proc


def kill_gracefully(proc: subprocess.Popen, timeout: int = 20) -> None:
    if proc.poll() is not None:
        return
    LOG.info(f"sending SIGTERM to pid {proc.pid}")
    try:
        if os.name == "nt":
            proc.terminate()
        else:
            proc.send_signal(signal.SIGTERM)
    except Exception as e:
        LOG.warning(f"error sending SIGTERM: {e}")
    try:
        proc.wait(timeout=timeout)
        LOG.info("process terminated gracefully")
        return
    except subprocess.TimeoutExpired:
        LOG.warning("graceful shutdown timed out")
    LOG.warning(f"forcing kill for pid {proc.pid}")
    try:
        if os.name == "nt":
            proc.kill()
        else:
            proc.send_signal(signal.SIGKILL)
    except Exception as e:
        LOG.warning(f"error killing process: {e}")
    proc.wait()


def run_git(args: List[str], cwd: Path, token: Optional[str]) -> None:
    base = ["git"]
    if token:
        base += ["-c", "http.extraHeader=Authorization: Bearer " + token]
    subprocess.run(
        base + args,
        cwd=cwd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def git_output(args: List[str], cwd: Path, token: Optional[str]) -> str:
    base = ["git"]
    if token:
        base += ["-c", "http.extraHeader=Authorization: Bearer " + token]
    out = subprocess.check_output(base + args, cwd=cwd, text=True)
    return out


def git_clone_or_update(
    repo_url: str, branch: str, dest: Path, token: Optional[str]
) -> Tuple[bool, str]:
    changed = False
    dest.mkdir(parents=True, exist_ok=True)
    if not (dest / ".git").exists():
        LOG.info(f"cloning {repo_url} branch {branch} into {dest}")
        run_git(["init"], dest, token)
        run_git(["remote", "add", "origin", repo_url], dest, token)
        run_git(["fetch", "origin", branch, "--depth", "1"], dest, token)
        run_git(["checkout", "-B", branch, f"origin/{branch}"], dest, token)
        changed = True
    else:
        before = git_output(["rev-parse", "HEAD"], dest, token).strip()
        LOG.info(f"fetching latest for {repo_url} branch {branch}")
        run_git(["fetch", "origin", branch], dest, token)
        remote = git_output(["rev-parse", f"origin/{branch}"], dest, token).strip()
        if before != remote:
            LOG.info(f"updating working tree from {before[:7]} to {remote[:7]}")
            run_git(["checkout", branch], dest, token)
            run_git(["reset", "--hard", remote], dest, token)
            changed = True
    head = git_output(["rev-parse", "HEAD"], dest, token).strip()
    return changed, head


def pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def default_path_from_github(source: str) -> str:
    url = source
    if url.startswith("git@github.com:"):
        tail = url.split("git@github.com:", 1)[1]
    elif "github.com/" in url:
        tail = url.split("github.com/", 1)[1]
    else:
        tail = url
    tail = tail.split("#", 1)[0]
    tail = tail.split("?", 1)[0]
    tail = tail.strip("/")
    if tail.endswith(".git"):
        tail = tail[:-4]
    path = "/" + tail if "/" in tail else "/" + tail
    path = re.sub(r"[^a-zA-Z0-9/_\.]", "_", path)
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return path


def parse_csv_packages(csv_text: str) -> List[str]:
    if not csv_text:
        return []
    raw = [p.strip() for p in csv_text.replace("\n", ",").split(",")]
    seen = set()
    out = []
    for p in raw:
        if not p:
            continue
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def which(*names: str) -> Optional[str]:
    for n in names:
        p = shutil.which(n)
        if p:
            return p
    return None


def install_conda_packages(packages: Iterable[str]) -> None:
    pkgs = set([p for p in packages if p])
    pkgs.add("caddy")
    conda.install(*pkgs)


@dataclass
class AppConfig:
    index: int
    source: str
    command: str
    poll_seconds: int
    branch: str
    env_vars: Dict[str, str]
    github_token: Optional[str]
    path: str = field(default="")
    port: int = field(default=0)

    @staticmethod
    def from_dict(index: int, raw: Dict[str, str]) -> "AppConfig":
        d = {k.upper(): v for k, v in raw.items()}

        def req(k: str) -> str:
            if k not in d or not d[k]:
                raise ValueError(f"Config {index} missing required key {k}")
            return d[k]

        source = req("SOURCE")
        command = req("COMMAND")
        branch = d.get("BRANCH") or "main"
        poll_seconds = parse_duration(d.get("POLL_INTERVAL"))
        token = d.get("GITHUB_TOKEN") or d.get("_GITHUB_TOKEN")
        env_vars = {k[len("ENV_") :]: v for k, v in d.items() if k.startswith("ENV_")}
        path = d.get("PATH") or default_path_from_github(source)
        if not path.startswith("/"):
            path = "/" + path
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        return AppConfig(
            index=index,
            source=source,
            command=command,
            poll_seconds=poll_seconds,
            branch=branch,
            env_vars=env_vars,
            github_token=token,
            path=path,
            port=0,
        )


class AppWorker(threading.Thread):
    def __init__(self, cfg: AppConfig):
        super().__init__(name=f"app-{cfg.index}", daemon=True)
        self.cfg = cfg
        self.stop_evt = threading.Event()
        self.proc: Optional[subprocess.Popen] = None

    def run(self):
        try:
            self._run_loop()
        except Exception as e:
            LOG.exception(f"worker {self.cfg.index} crashed: {e}")

    def _run_loop(self):
        repo_url, branch = parse_repo_target(self.cfg.source, self.cfg.branch)
        repo_url_hash = hashlib.md5(repo_url.encode()).hexdigest()
        app_dir = WORK_ROOT / f"app_{repo_url_hash}"
        LOG.info(f"using directory {app_dir}")
        app_dir.mkdir(parents=True, exist_ok=True)
        last_head = None
        while not self.stop_evt.is_set():
            changed, head = git_clone_or_update(
                repo_url, branch, app_dir, self.cfg.github_token
            )
            if last_head is None:
                last_head = head
                LOG.info(f"{repo_url} at {head[:7]}")
            elif head != last_head:
                changed = True
                LOG.info(f"detected change to {head[:7]}")
            if self.proc is None or changed:
                if self.proc is not None:
                    LOG.info("redeploying due to change")
                    self._stop_proc()
                env = os.environ.copy()
                env.update(self.cfg.env_vars)
                env["DATABRICKS_APP_PORT"] = str(self.cfg.port)
                env["DATABRICKS_APP_PATH"] = self.cfg.path
                env.setdefault("PYTHONUNBUFFERED", "1")
                self.proc = run_cmd(self.cfg.index, self.cfg.command, app_dir, env)
                last_head = head
            slept = 0
            while slept < self.cfg.poll_seconds and not self.stop_evt.is_set():
                if self.proc and self.proc.poll() is not None:
                    LOG.warning(
                        f"process exited with code {self.proc.returncode}, will retry after {self.cfg.poll_seconds}s"
                    )
                    self.proc = None
                    break
                time.sleep(1)
                slept += 1
        self._stop_proc()

    def _stop_proc(self):
        if self.proc is not None:
            kill_gracefully(self.proc)
            self.proc = None

    def stop(self):
        self.stop_evt.set()


def build_caddy_file_content(apps: List[AppConfig]) -> Dict[str, Any]:
    routes = []
    for app in apps:
        route = {
            "match": [{"path": [f"{app.path}*"]}],
            "handle": [
                {
                    "handler": "reverse_proxy",
                    "upstreams": [{"dial": f"localhost:{app.port}"}],
                }
            ],
        }
        routes.append(route)

    config = {
        "admin": {"disabled": True, "config": {"persist": False}},
        "apps": {
            "http": {
                "servers": {
                    "srv0": {
                        "listen": [f":{CADDY_PORT}"],
                        "routes": routes,
                    }
                }
            }
        },
    }
    return config


def start_caddy(caddy_config) -> concurrent.futures.Future[int]:
    LOG.info(f"starting caddy on port {CADDY_PORT}")
    caddy_log = logs.logger()
    proc = caddy.CaddyWorker(
        caddy_config,
        cwd=str(CADDY_DIR),
        check=True,
        stdout_writer=caddy_log.info,
        stderr_writer=caddy_log.error,
    )

    return proc.run_threaded()


def load_all_configs() -> Dict[int, Dict[str, str]]:
    base = Path.cwd()
    files = discover_config_files(base)
    env_groups = discover_config_envs()
    configs: Dict[int, Dict[str, str]] = {}
    for idx, envs in env_groups.items():
        configs[idx] = dict(envs)
    for idx, path in files.items():
        file_env = load_kv_file(path)
        if idx in configs:
            configs[idx] = merge_envs(configs[idx], file_env)
        else:
            configs[idx] = file_env
    if not configs:
        default_file = base / "DATABRICKS_APP_CONFIG_ENV"
        if default_file.exists():
            configs[0] = load_kv_file(default_file)
    normalized: Dict[int, Dict[str, str]] = {}
    for idx, cfg in configs.items():
        norm: Dict[str, str] = {}
        for k, v in cfg.items():
            norm[k.upper()] = v
        normalized[idx] = norm
    return normalized


def collect_global_conda_packages(raw_configs: Dict[int, Dict[str, str]]) -> List[str]:
    vals: List[str] = []
    # env var takes precedence
    if os.getenv("DATABRICKS_APP_CONDA_PACKAGES"):
        vals.append(os.getenv("DATABRICKS_APP_CONDA_PACKAGES", ""))
    # also gather from any config blocks
    for cfg in raw_configs.values():
        v = cfg.get("DATABRICKS_APP_CONDA_PACKAGES") or cfg.get("CONDA_PACKAGES")
        if v:
            vals.append(v)
    csv_text = ",".join(vals)
    pkgs = parse_csv_packages(csv_text)
    if pkgs:
        LOG.info(f"conda packages requested: {', '.join(pkgs)}")
    return pkgs


def run():
    ensure_git_available()
    WORK_ROOT.mkdir(parents=True, exist_ok=True)

    raw_configs = load_all_configs()
    # install conda packages first
    conda_pkgs = collect_global_conda_packages(raw_configs)
    install_conda_packages(conda_pkgs)

    if not raw_configs:
        LOG.error(
            "no configs found. provide files like DATABRICKS_APP_CONFIG_0_ENV or env vars like DATABRICKS_APP_CONFIG_0_SOURCE"
        )
        sys.exit(1)

    app_configs: List[AppConfig] = []
    for idx, raw in sorted(raw_configs.items()):
        try:
            cfg = AppConfig.from_dict(idx, raw)
            app_configs.append(cfg)
        except Exception as e:
            LOG.error(f"skipping config {idx}: {e}")

    if not app_configs:
        LOG.error("no valid configs after parsing")
        sys.exit(1)

    seen_paths = set()
    for cfg in app_configs:
        cfg.port = pick_free_port()
        orig = cfg.path
        suffix = 1
        while cfg.path in seen_paths:
            cfg.path = f"{orig}-{suffix}"
            suffix += 1
        seen_paths.add(cfg.path)
        LOG.info(f"app {cfg.index} path {cfg.path} port {cfg.port}")

    caddyfile = build_caddy_file_content(app_configs)
    caddy_proc = start_caddy(caddyfile)

    workers: Dict[int, AppWorker] = {}
    for cfg in app_configs:
        w = AppWorker(cfg)
        workers[cfg.index] = w
        w.start()

    stop_all = threading.Event()

    def handle_signal(signum, frame):
        LOG.info(f"received signal {signum}. stopping workers")
        stop_all.set()
        for w in workers.values():
            w.stop()
        try:
            if caddy_proc.poll() is None:
                LOG.info("stopping caddy")
                if os.name == "nt":
                    caddy_proc.terminate()
                else:
                    caddy_proc.send_signal(signal.SIGTERM)
        except Exception:
            pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handle_signal)
        except Exception:
            pass

    try:
        while not stop_all.is_set():
            time.sleep(0.5)
    finally:
        for w in workers.values():
            w.stop()
        for w in workers.values():
            w.join(timeout=30)
        try:
            if caddy_proc.poll() is None:
                LOG.info("force killing caddy")
                if os.name == "nt":
                    caddy_proc.kill()
                else:
                    caddy_proc.send_signal(signal.SIGKILL)
                caddy_proc.wait(timeout=10)
        except Exception:
            pass
        LOG.info("all workers stopped")
