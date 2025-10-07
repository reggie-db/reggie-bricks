# reggie-bricks

Utilities and sample applications for Databricks projects, packaged as a
multi-module workspace. The goal is to centralize shared helpers (sessions,
catalog helpers, configuration loaders) so individual jobs can avoid repeating
boilerplate or pulling in inconsistent dependencies.

## Repository Layout

- `reggie-tools/` – core Databricks helpers that expose Spark and Workspace
  clients, configuration resolution, and catalog utilities.
- `reggie-apps/` – example service modules that consume `reggie-tools` inside
  the same uv workspace.
- `build-scripts/` – tooling for keeping the workspace tidy (see *Build
  scripts* below).

`pyproject.toml` at the root declares the uv workspace. Each member project has
its own `pyproject.toml`, but relies on the root for shared `tool.uv.sources` so
workspace dependencies resolve without duplicating configuration.

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)

## Local Setup

Synchronize the workspace once to resolve all project dependencies:

```bash
uv sync --workspace
```

You can then execute any module script in-place. For example, to run the
`reggie-apps` smoke script that imports `reggie-tools`:

```bash
uv run --project reggie-apps python -m reggie_apps.test_workspace
```

Or to exercise the configuration helpers directly:

```bash
uv run --project reggie-tools python -m reggie_tools.test_configs
```

Because both modules live in the same uv workspace, local changes in
`reggie-tools` are immediately visible to dependents without publishing wheels
or editing `PYTHONPATH`.

## Build Scripts

The `build-scripts/` directory contains small utilities that automate common
workspace maintenance tasks:

- `member_project.py` – scaffolds a new workspace member. It can create the
  directory structure, initialize a uv-aware `pyproject.toml`, wire up
  dependencies on other workspace members, and update the root workspace list.
  Example: `python build-scripts/member_project.py analytics-core reggie-tools`.
- `sync_versions.py` – computes a build metadata version (default `0.0.1`
  suffixed with the current Git revision) and writes that version into every
  workspace member so releases stay aligned.

These scripts expect to run from the repository root and only touch files under
version control.

## Additional Notes

- Use `uv run --project <member>` for ad-hoc commands inside a specific module.
- Keep Databricks-specific logic inside `reggie-tools` so apps stay lean and
  avoid redundant third-party dependencies.

