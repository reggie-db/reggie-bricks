import functools
import os
import platform
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Callable, Dict, Iterable
from urllib.request import urlretrieve

from reggie_core import logs, paths, projects
from reggie_core.procs import Worker, WorkerOutputConsumer

_CONDA_DIR_NAME = ".miniforge3"

LOG = logs.logger(__name__, __file__)


@functools.cache
def path() -> Path:
    path = paths.path("conda", exists=True)
    if path:
        return path
    conda_dir = paths.home() / _CONDA_DIR_NAME
    bin_path = conda_dir / "bin" / "conda"

    def _bin_path_valid():
        return bin_path.is_file() and os.access(bin_path, os.X_OK)

    if _bin_path_valid():
        return bin_path
    installer_dir = _install_conda(conda_dir)
    if not _bin_path_valid():
        raise ValueError(
            f"Failed to install conda - path:{bin_path} installer_dir:{installer_dir}"
        )
    return bin_path


def env_name() -> str:
    return projects.name()


def env(include_os: bool = True) -> Dict[str, str]:
    env = os.environ.copy() if include_os else {}
    env.update(_env())
    return env


def activate() -> Callable[[], None]:
    os_env = os.environ.copy()
    os.environ.update(env())

    def _deactivate():
        os.environ.clear()
        os.environ.update(os_env)

    return _deactivate


def install(*packages: Iterable[str]):
    _start("install", "-y", "-q", *packages).wait(check=True)


@functools.cache
def _env():
    script_content = """
        set -e

        CONDA_BIN="$1"
        ENV_NAME="$2"
        OUT_FILE="$3"

        eval "$("$CONDA_BIN" shell.posix hook)"

        if ! "$CONDA_BIN" list -n "$ENV_NAME" >/dev/null 2>&1; then
            "$CONDA_BIN" create -y -n "$ENV_NAME"
        fi

        "$CONDA_BIN" run -n "$ENV_NAME" env -0 > "$OUT_FILE"
        """
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        script_path = td / "script.sh"
        script_path.write_text(script_content)
        env_path = td / ".env"
        sh_path = paths.path(shutil.which("sh"), exists=True)
        output_consumers = [WorkerOutputConsumer.log(LOG, prefix="conda-env")]
        worker = Worker(
            [sh_path, script_path, path(), env_name(), env_path],
            output_consumers=output_consumers,
        )
        worker.wait()
        env_content = env_path.read_bytes()
    pairs = [line for line in env_content.split(b"\0") if line]
    env = {}
    for pair in pairs:
        if b"=" in pair:
            k, v = pair.split(b"=", 1)
            env[k.decode()] = v.decode()
    return env


def _start(*arguments: Iterable[str]):
    output_consumers = [WorkerOutputConsumer.log(LOG, prefix="conda")]
    return Worker(
        ["conda", *arguments],
        output_consumers=output_consumers,
        env=env(),
    )


def _install_url() -> str:
    """Pick the correct Miniforge installer URL for this platform."""
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


def _install_conda(dir: Path) -> Path:
    url = _install_url()

    def _run_installer(path: Path):
        installer_path = path / "installer.sh"
        LOG.info(f"Downloading Conda - url:{url} path:{installer_path}")
        urlretrieve(url, installer_path)
        installer_path.chmod(0o755)
        LOG.info(f"Installing Conda - path:{installer_path}")
        subprocess.run([installer_path, "-b", "-p", dir], check=True, text=True)

    return paths.cache_store(url + "v2", _run_installer)


if __name__ == "__main__":
    conda = path()
    # print(json.dumps(env(include_os=False), indent=2))
    install("caddy", "curl", "openjdk")
    for commands in [
        # ["conda", "info"],
        ["caddy", "version"],
        ["curl", "--version"],
        ["java", "--version"],
        ["bash", "-c", "echo $JAVA_HOME"],
    ]:
        worker = Worker(
            commands,
            env=env(),
            output_consumers=[WorkerOutputConsumer.log(LOG)],
        )
        worker.wait(check=True)
    # print(f"Conda executable: {conda}")
    # start(["conda", "info"]).wait()
