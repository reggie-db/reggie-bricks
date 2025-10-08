import functools
import importlib
import subprocess
from types import ModuleType

import tomllib

from reggie_core import paths


def name(input=None, default: str = None, git_origin: bool = True) -> str:
    if input is None and default is None:
        return _project_name_default()
    name = None
    if input is not None:
        if isinstance(input, ModuleType):
            name = input.__package__ or input.__name__
        name = str(name).split(".")[0]
    if name:
        try:
            dist = importlib.metadata.distribution(name)
            project_name = dist.metadata["Name"] if dist else None
            if project_name:
                return project_name
        except Exception:
            pass
    if isinstance(input, ModuleType):
        path = paths.path(input.__file__, exists=True)
    else:
        path = paths.path(input, exists=True)
    if path and git_origin:
        remote_origin_url = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout.strip()
        if remote_origin_url:
            if project_name := remote_origin_url.split("/")[-1].split(".")[0]:
                return project_name
    project_name = default
    while path:
        path = path.parent
        path_project_name = None
        if pyproject := paths.path(path / "pyproject.toml", exists=True):
            try:
                if data := tomllib.loads(pyproject.read_text()):
                    path_project_name = data.get("project", {}).get("name", None)
            except Exception:
                pass

        if not path_project_name and project_name is not None:
            break
        elif path_project_name:
            project_name = path_project_name
    if project_name:
        return project_name
    raise ValueError(f"Could not determine project name for {input}")


@functools.cache
def _project_name_default() -> str:
    pn = name(__file__, "")
    if not pn:
        pn = name(__name__, "")
    return pn or "reggie-bricks"


if __name__ == "__main__":
    print(name())
    print(name())
    print(name(paths))
    print(name(__file__))
