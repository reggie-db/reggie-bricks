import functools
import json
import os
import platform
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict
from urllib.request import urlretrieve

from reggie_core.procs import StreamType, Worker, WorkerOutputConsumer

_ENV_NAME = "reggie-bricks"
_MINICONDA_DIR_NAME = ".miniconda"
_WINDOWS_SUPPORT = False


@functools.cache
def path() -> Path:
    target_dir = Path.home() / _MINICONDA_DIR_NAME
    path = _find_existing_conda(target_dir)
    if not path:
        path = _install_miniconda(target_dir)
    return path


def env(include_os: bool = True) -> Dict[str, str]:
    env = os.environ.copy() if include_os else {}
    env.update(_conda_env())
    return env


def start(args: list[str], **kwargs) -> Worker:
    args = _conda_args(args)
    _conda_env_config(kwargs)
    output_consumers_key = "output_consumers"
    if output_consumers_key not in kwargs:
        kwargs[output_consumers_key] = [WorkerOutputConsumer.log(prefix="conda")]
    worker = Worker(args, **kwargs)
    return worker


def run(args: list[str], **kwargs) -> Dict[str, Any]:
    args = _conda_args(args)
    _conda_env_config(kwargs)
    json_arg = "--json"
    if json_arg not in args:
        args.insert(1, json_arg)
    kwargs.setdefault("text", True)
    kwargs.setdefault("capture_output", True)
    process = subprocess.run(args, **kwargs)
    exit_code = process.returncode

    stream_types = (
        [StreamType.STDOUT, StreamType.STDERR]
        if exit_code == 0
        else [StreamType.STDERR, StreamType.STDOUT]
    )

    outputs: Dict[StreamType, str] = {}

    def _parse_json(output: str):
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            return None

    for stream_type in stream_types:
        stream = stream_type.process_stream(process)
        output = stream.read()
        if not output:
            continue
        outputs[stream_type] = output
        result = _parse_json(output)
        if result is None or (isinstance(result, dict) and len(result) == 0):
            continue
        return result

    raise ValueError(f"Failed to parse conda output: {outputs}")


def install(*packages: list[str]):
    if not packages:
        return
    packages = set(packages)
    _install_conda_anaconda_tos()
    _start_install(*packages).wait()


def _conda_args(args: list[str]) -> list[str]:
    conda_command = path()
    conda_command = "conda"
    if not args:
        args = [conda_command]
    elif args[0] == "conda":
        args[0] = conda_command
    else:
        args.insert(0, conda_command)
    return args


def _conda_env_config(kwargs):
    env = kwargs.get("env") or os.environ
    env = env.copy()
    env.update(_conda_env())
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("CONDA_PLUGINS_AUTO_ACCEPT_TOS", "yes")
    kwargs["env"] = env


@functools.cache
def _conda_env():
    activate_script = f"""
        eval "$('{path()}' shell.bash hook)"
        if ! conda env list | grep -q "{_ENV_NAME}"; then
            conda create -y -n -q "{_ENV_NAME}" 
        fi
        conda activate "{_ENV_NAME}"
        printenv
    """
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".sh", delete=False) as f:
        f.write(activate_script)
        f.flush()
        env_dump = subprocess.run(
            ["bash", f.name],
            capture_output=True,
            text=True,
            check=True,
        ).stdout

    env = {}
    for pair in env_dump.split("\n"):
        if not pair:
            continue
        k, _, v = pair.partition("=")
        env[k] = v
    return env


@functools.cache
def _install_conda_anaconda_tos():
    _start_install("conda-anaconda-tos").wait()


def _start_install(*packages: str) -> Worker:
    prefix = f"conda-install-[{'-'.join(packages)}]"
    return start(
        ["install", "--verbose", "--yes", *packages],
        output_consumers=[WorkerOutputConsumer.log(prefix=prefix)],
    )


def _is_windows() -> bool:
    if platform.system() == "Windows":
        if not _WINDOWS_SUPPORT:
            raise RuntimeError("Windows support is not enabled")
        return True
    return False


def _miniconda_url() -> str:
    """Pick the correct Miniconda installer URL for this platform."""
    sysname = platform.system()
    arch = platform.machine().lower()

    if sysname == "Darwin":
        if arch in {"arm64", "aarch64"}:
            fname = "Miniconda3-latest-MacOSX-arm64.sh"
        else:
            fname = "Miniconda3-latest-MacOSX-x86_64.sh"
        return f"https://repo.anaconda.com/miniconda/{fname}"

    if sysname == "Linux":
        if arch in {"aarch64", "arm64"}:
            fname = "Miniconda3-latest-Linux-aarch64.sh"
        else:
            fname = "Miniconda3-latest-Linux-x86_64.sh"
        return f"https://repo.anaconda.com/miniconda/{fname}"

    if _is_windows():
        # Miniconda provides x86_64 and arm64 installers. Default to x86_64 unless arm64 is detected.
        if arch in {"arm64", "aarch64"}:
            fname = "Miniconda3-latest-Windows-arm64.exe"
        else:
            fname = "Miniconda3-latest-Windows-x86_64.exe"
        return f"https://repo.anaconda.com/miniconda/{fname}"

    raise RuntimeError(f"Unsupported platform: {sysname} {arch}")


def _candidate_conda_paths(install_dir: Path) -> list[Path]:
    """Return likely conda executables inside install_dir."""
    if _is_windows():
        return [
            install_dir / "condabin" / "conda.bat",
            install_dir / "Scripts" / "conda.exe",
            install_dir / "Scripts" / "conda.bat",
        ]
    else:
        return [
            install_dir / "bin" / "conda",
            install_dir / "condabin" / "conda",
        ]


def _find_existing_conda(install_dir: Path | None = None) -> Path | None:
    """Check PATH first, then a provided install_dir."""
    # 1. PATH
    p = shutil.which("conda")
    if p:
        return Path(p)

    # 2. Known locations under install_dir if given
    if install_dir:
        for c in _candidate_conda_paths(install_dir):
            if c.exists() and os.access(c, os.X_OK):
                return c

    # 3. Common home install
    home = Path.home()
    for guess in (
        home / "miniconda",
        home / "miniconda3",
        home / "anaconda3",
    ):
        for c in _candidate_conda_paths(guess):
            if c.exists() and os.access(c, os.X_OK):
                return c

    return None


def _install_miniconda(install_dir: Path) -> Path:
    """
    Download and run the Miniconda installer into install_dir.
    Returns a Path to the conda executable.
    """
    install_dir = install_dir.expanduser().resolve()

    url = _miniconda_url()
    with tempfile.TemporaryDirectory() as td:
        tdir = Path(td)
        installer_path = tdir / Path(url).name
        print(f"Downloading Miniconda from {url} to {installer_path}")
        urlretrieve(url, installer_path)

        def _run(cmd: list[str], cwd: Path | None = None) -> None:
            subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)

        if _is_windows():
            # Silent install for user, set target directory
            # The path must use backslashes for the installer
            target = str(install_dir)
            # Conda installer expects backslashes and no trailing backslash
            target = target.rstrip("\\")
            print(f"Installing Miniconda to {target}")
            _run([str(installer_path), "/S", f"/D={target}"])
        else:
            # Make executable, then run silently with prefix
            installer_path.chmod(0o755)
            print(f"Installing Miniconda to {install_dir}")
            _run([str(installer_path), "-b", "-p", str(install_dir)])

    conda_path = _find_existing_conda(install_dir)
    if not conda_path:
        raise RuntimeError("Miniconda installation completed but conda was not found")
    return conda_path


if __name__ == "__main__":
    conda = path()
    print(json.dumps(env(include_os=False), indent=2))
    # print(f"Conda executable: {conda}")
    # start(["conda", "info"]).wait()
    install("conda-forge::caddy")
    version_worker = Worker(
        ["conda", "--version"],
        text=True,
        env=env(),
        output_consumers=[WorkerOutputConsumer.log()],
    )
    version_worker.wait()
    caddy_worker = Worker(
        ["caddy", "--version"],
        text=True,
        env=env(),
        output_consumers=[WorkerOutputConsumer.log()],
    )
    caddy_worker.wait()
