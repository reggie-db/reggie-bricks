import pathlib
import subprocess

import tomllib

DEFAULT_VERSION = "0.0.1"


def compute_version() -> str:
    try:
        # Prefer the short commit hash
        rev = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=pathlib.Path(__file__).resolve().parents[1],
            text=True,
        ).strip()
        if rev:
            return f"{DEFAULT_VERSION}+g{rev}"
    except Exception:
        pass
    return DEFAULT_VERSION


VERSION = compute_version()

root = pathlib.Path(__file__).resolve().parents[1]
pyproject = tomllib.loads((root / "pyproject.toml").read_text())

members = (
    pyproject.get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
)
if not members:
    raise SystemExit("No workspace members found under [tool.uv.workspace].")


def candidate_projects(member_pattern: str) -> list[pathlib.Path]:
    paths = []
    for p in root.glob(member_pattern):
        if not p.is_dir():
            continue
        pj = p / "pyproject.toml"
        if pj.exists():
            paths.append(p)
        else:
            for child in p.iterdir():
                if child.is_dir() and (child / "pyproject.toml").exists():
                    paths.append(child)
    return paths


projects: list[pathlib.Path] = []
seen = set()
for m in members:
    for proj in candidate_projects(m):
        key = proj.resolve()
        if key not in seen:
            seen.add(key)
            projects.append(proj)

for proj in projects:
    subprocess.run(
        ["uv", "--project", str(proj), "version", VERSION, "--no-sync"],
        check=True,
    )
