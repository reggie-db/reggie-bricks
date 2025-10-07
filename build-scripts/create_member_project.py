import pathlib
import re
import sys

import tomli_w
import tomllib

PYPROJECT_FILE_NAME = "pyproject.toml"


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


if len(sys.argv) < 2:
    die(f"Usage: {pathlib.Path(sys.argv[0]).name} <project-name>")

raw = sys.argv[1]

# tokenize: split on non alphanumeric, lowercase, remove empties
parts = [p.lower() for p in re.split(r"[^A-Za-z0-9]+", raw) if p]
if not parts:
    die("Project name must contain alphanumeric characters")

name_dash = "-".join(parts)  # project name and directory: name-part-example
name_us = "_".join(parts)  # package dir: name_part_example

root = pathlib.Path.cwd()
proj_dir = root / name_dash
if proj_dir.exists():
    die(f"Project already exists: {proj_dir}")

# create structure
pkg_dir = proj_dir / "src" / name_us
pkg_dir.mkdir(parents=True, exist_ok=False)
(pkg_dir / "__init__.py").write_text("", encoding="utf-8")

# write project pyproject.toml
py_content = {
    "build-system": {
        "requires": ["uv_build>=0.8.23,<0.9.0"],
        "build-backend": "uv_build",
    },
    "project": {
        "name": name_dash,
        "version": "0.0.1",
        "requires-python": ">=3.12",
        "dependencies": [],
    },
}
(proj_dir / PYPROJECT_FILE_NAME).write_text(tomli_w.dumps(py_content), encoding="utf-8")

# add to root workspace members
root_py_path = root / PYPROJECT_FILE_NAME
if not root_py_path.exists():
    die(f"Root {PYPROJECT_FILE_NAME} not found at {root_py_path}")

root_data = tomllib.loads(root_py_path.read_text(encoding="utf-8"))
tool = root_data.setdefault("tool", {})
uv = tool.setdefault("uv", {})
ws = uv.setdefault("workspace", {})
members = ws.setdefault("members", [])

# append if not already present (string equality)
if name_dash not in members:
    members.append(name_dash)

root_py_path.write_text(tomli_w.dumps(root_data), encoding="utf-8")

print(f"Created project: {proj_dir}")
print(f"Package module:  {name_us}")
print(f"Added to workspace members: {name_dash}")
