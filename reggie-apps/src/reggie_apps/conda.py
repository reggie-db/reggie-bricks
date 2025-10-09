import functools
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, Dict
from urllib.request import urlretrieve

from reggie_core import logs, paths, projects

_CONDA_DIR_NAME = ".miniforge3"

LOG = logs.logger(__name__, __file__)


@functools.cache
def path() -> Path:
    """Return the conda executable path. Install Miniforge to ~/.miniforge3 if needed."""
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
    """Return the default environment name based on the project name."""
    return projects.name() + "-dev-3"


def env(include_os: bool = False) -> Dict[str, str]:
    """Build a conda environment mapping. Optionally include the current OS environment."""
    env = os.environ.copy() if include_os else {}
    env.update(_env())
    return env


def activate() -> Callable[[], None]:
    """Activate the conda env for the current process. Returns a callable to deactivate."""
    os_env = os.environ.copy()
    os.environ.update(env())

    def _deactivate():
        os.environ.clear()
        os.environ.update(os_env)

    return _deactivate


def install(*packages: str):
    """Install one or more packages into the environment using conda install."""
    _run("install", "-y", "-q", *packages)


@functools.cache
def _env():
    """Create the env if missing and capture `conda run -n ENV env -0` into a dict."""
    script_content = """
        set -e

        CONDA_BIN="$1"
        ENV_NAME="$2"
        PYTHON_VERSION="$3"
        OUT_FILE="$4"

        echo "Enabling Conda - conda_bin:$CONDA_BIN env_name:$ENV_NAME python_version:$PYTHON_VERSION out_file:$OUT_FILE"

        eval "$("$CONDA_BIN" shell.posix hook)"

        if ! "$CONDA_BIN" list -n "$ENV_NAME" >/dev/null 2>&1; then
            "$CONDA_BIN" create -y -n "$ENV_NAME" python="$PYTHON_VERSION"
        fi

        "$CONDA_BIN" run -n "$ENV_NAME" env -0 > "$OUT_FILE"
        """
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        script_path = td / "script.sh"
        script_path.write_text(script_content)
        env_path = td / ".env"
        sh_path = paths.path(shutil.which("sh"), exists=True)
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        commands = [sh_path, script_path, path(), env_name(), python_version, env_path]
        subprocess.run(commands, check=True)
        env_content = env_path.read_bytes()
    pairs = [line for line in env_content.split(b"\0") if line]
    env = {}
    for pair in pairs:
        if b"=" in pair:
            k, v = pair.split(b"=", 1)
            env[k.decode()] = v.decode()
    return env


def _run(*arguments: str):
    """Run a conda subcommand in the configured environment."""
    arguments = ["conda", *arguments]
    subprocess.run(arguments, env=env(), check=True)


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


def _install_conda(dir: Path) -> Path:
    """Download and run the Miniforge installer into the given directory, cached by URL."""
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
    print(json.dumps(env(include_os=False), indent=2))
    install("caddy", "curl", "openjdk")
    for commands in [
        ["conda", "info"],
        ["caddy", "version"],
        ["curl", "--version"],
        ["java", "--version"],
        ["bash", "-c", "echo $JAVA_HOME"],
    ]:
        subprocess.run(commands, env=env(), check=True)

    # print(f"Conda executable: {conda}")
    # start(["conda", "info"]).wait()
