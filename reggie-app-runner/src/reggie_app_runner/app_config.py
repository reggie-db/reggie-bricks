# app_config.py
import os
import re
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Dict, List, Optional

from pytimeparse.timeparse import timeparse

CONFIG_FILE_PATTERN = re.compile(r"^DATABRICKS_APP_CONFIG(?:_(\d+))?_ENV$")
CONFIG_KEY_PATTERN = re.compile(r"^DATABRICKS_APP_CONFIG(?:_(\d+))?_(.+)$")

DEFAULT_POLL_SECONDS = 60

# Field aliasing for inputs that use different config names than the dataclass field
# Example: POLL_SECONDS field is configured by POLL_INTERVAL
EXCLUDE_ENV_KEYS = ["PATH"]
FIELD_ALIASES: Dict[str, List[str]] = {
    "POLL_SECONDS": ["POLL_INTERVAL"],
}


def _parse_duration(input) -> int:
    if input is None or isinstance(input, int):
        return input
    input = str(input).strip().lower() if input else None
    if not input:
        return DEFAULT_POLL_SECONDS
    result = timeparse(input)
    if result is None:
        raise ValueError(f"Invalid duration: {input}")
    return int(result)


def _load_kv_file(path: Path) -> Dict[str, str]:
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


def _discover_config_files(base: Path) -> Dict[int, Path]:
    # Treat no index as slot 0
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


def _discover_config_envs() -> Dict[int, Dict[str, str]]:
    # Treat no index as slot 0
    grouped: Dict[int, Dict[str, str]] = {}
    for k, v in os.environ.items():
        m = CONFIG_KEY_PATTERN.match(k)
        if not m:
            continue
        idx = int(m.group(1)) if m.group(1) is not None else 0
        key = m.group(2)
        grouped.setdefault(idx, {})[key] = v
    return grouped


def _merge_envs(*envs: Dict[str, str]) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for e in envs:
        if not e:
            continue
        merged.update({k.upper(): v for k, v in e.items()})
    return merged


def _default_path_from_github(source: str) -> str:
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


@dataclass
class AppConfig:
    index: int
    source: str
    command: str
    poll_seconds: int
    branch: str
    path: str
    github_token: Optional[str]
    env_vars: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, index: int, raw: Dict[str, str]) -> "AppConfig":
        d = {k.upper(): v for k, v in raw.items()}
        environ = {
            k: v for k, v in os.environ.items() if k.upper() not in EXCLUDE_ENV_KEYS
        }

        def resolve_value(
            field_name: str, *defaults, transform=None, required=False
        ) -> Optional[str]:
            """
            Resolution order for a given dataclass field:
              1) Slot config using any aliases for this field
              2) Slot config using the field name itself
              3) Root environment using any aliases for this field
              4) Root environment using the field name itself
            """

            def _to_value(value, skip_transform=False):
                if callable(value):
                    return _to_value(value())
                if isinstance(value, str):
                    value = value.strip() or None
                if not skip_transform and transform:
                    return _to_value(transform(value), True)
                return value

            field_name_key = field_name.upper()
            for source in [d, environ]:
                for key in [field_name_key, *FIELD_ALIASES.get(field_name_key, [])]:
                    if key in source:
                        if value := _to_value(source[key]):
                            return value
            if defaults:
                for default in defaults:
                    if value := _to_value(default):
                        return value
            if required:
                raise ValueError(
                    f"{AppConfig.__name__} {index} missing required key {field_name}"
                )
            return None

        source = resolve_value("SOURCE", required=True)
        command = resolve_value("COMMAND", required=True)

        config = {
            "index": index,
            "source": source,
            "command": command,
        }
        for f in fields(AppConfig):
            field_name = f.name
            if field_name in config:
                continue
            if "branch" == field_name:
                value = resolve_value(field_name, "main")
            elif "path" == field_name:
                value = resolve_value(
                    field_name,
                    lambda: _default_path_from_github(source),
                    lambda: f"app-{index}",
                )
                if value:
                    if not value.startswith("/"):
                        value = "/" + value
                    if value != "/" and value.endswith("/"):
                        value = value[:-1]
            elif "poll_seconds" == field_name:
                value = resolve_value(
                    field_name, DEFAULT_POLL_SECONDS, transform=_parse_duration
                )
            elif "env_vars" == field_name:
                value = resolve_value(
                    field_name,
                    transform=lambda x: {
                        k[len("ENV_") :]: v
                        for k, v in x.items()
                        if k.startswith("ENV_")
                    },
                )
            else:
                value = resolve_value(field_name)
            if value is not None:
                config[field_name] = value

        return cls(**config)


def read(cwd: Optional[Path] = None) -> List[AppConfig]:
    base = cwd or Path.cwd()
    files = _discover_config_files(base)
    env_groups = _discover_config_envs()

    combined: Dict[int, Dict[str, str]] = {}
    for idx, envs in env_groups.items():
        combined[idx] = dict(envs)
    for idx, path in files.items():
        file_env = _load_kv_file(path)
        if idx in combined:
            combined[idx] = _merge_envs(combined[idx], file_env)
        else:
            combined[idx] = _merge_envs(file_env)

    if not combined:
        default_file = base / "DATABRICKS_APP_CONFIG_ENV"
        if default_file.exists():
            combined[0] = _load_kv_file(default_file)

    configs: List[AppConfig] = []
    for idx, raw in sorted(combined.items()):
        cfg = AppConfig.from_dict(idx, raw)
        configs.append(cfg)

    return configs


if __name__ == "__main__":
    from pprint import pprint

    # Root defaults
    os.environ.update(
        {
            "GITHUB_TOKEN": "root-token-xyz",
            "DATABRICKS_APP_CONFIG_POLL_INTERVAL": "45s",
            "DATABRICKS_APP_CONFIG_ENV_COMMON_FLAG": "on",
        }
    )

    # Slot overrides and definitions
    os.environ.update(
        {
            # App 0 fully specified and overrides root poll interval
            "DATABRICKS_APP_CONFIG_0_SOURCE": "https://github.com/example/app0.git",
            "DATABRICKS_APP_CONFIG_0_COMMAND": "python main.py",
            "DATABRICKS_APP_CONFIG_0_BRANCH": "dev",
            "DATABRICKS_APP_CONFIG_0_POLL_INTERVAL": "30s",
            "DATABRICKS_APP_CONFIG_0_ENV_DEBUG": "true",
            "DATABRICKS_APP_CONFIG_0_ENV_API_KEY": "abc123",
            # App 1 missing GITHUB_TOKEN and POLL_INTERVAL, will inherit root
            "DATABRICKS_APP_CONFIG_1_SOURCE": "git@github.com:example/app1.git#main",
            "DATABRICKS_APP_CONFIG_1_COMMAND": "uvicorn app:app --port 8080",
            "DATABRICKS_APP_CONFIG_1_ENV_MODE": "production",
            "DATABRICKS_APP_CONFIG_1_ENV_FEATURE_FLAG": "enabled",
            # App 1 missing GITHUB_TOKEN and POLL_INTERVAL, will inherit root
            "DATABRICKS_APP_CONFIG_2_SOURCE": " xyz",
            "DATABRICKS_APP_CONFIG_2_COMMAND": "uvicorn app:app --port 8080",
            "DATABRICKS_APP_CONFIG_2_ENV_MODE": "production",
            "DATABRICKS_APP_CONFIG_2_ENV_FEATURE_FLAG": "enabled",
        }
    )
    cfgs = read()
    for cfg in cfgs:
        pprint(cfg.__dict__)
