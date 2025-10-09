import pathlib
import subprocess


def repo_root() -> pathlib.Path:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
        if out:
            return pathlib.Path(out)
    except Exception:
        pass
    return pathlib.Path(__file__).resolve().parents[1]
