import functools
import json
import logging
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Union

import sh
from reggie_core import logs, objects, paths

from reggie_app_runner import conda

LOG = logs.logger("caddy")

_CONDA_ENV_NAME = "_caddy"
_CONDA_PACKAGE_NAME = "caddy"
_LOG_LEVEL_PATTERN = re.compile(r'"level"\s*:\s*"(\S+?)"', re.IGNORECASE)
_EXIT_CODE_PATTERN = re.compile(r'"exit_code"\s*:\s*(\d+)', re.IGNORECASE)


def command() -> sh.Command:
    return conda.run(_conda_env_name()).bake("caddy")


def run(config: Union[Path, Dict[str, Any], str], *args, **kwargs) -> sh.RunningCommand:
    config_file = _to_caddy_file(config)

    def _done(*_):
        try:
            config_file.unlink()
            LOG.info(f"Deleted config file {config_file}")
        except FileNotFoundError:
            pass

    def _out(error, line):
        levelno = logging.ERROR if error else logging.INFO
        line = line.rstrip()
        if line:
            for match in _LOG_LEVEL_PATTERN.finditer(line):
                line_levelno, _ = logs.get_level(match.group(1))
                if line_levelno:
                    levelno = line_levelno
                    break
            for match in _EXIT_CODE_PATTERN.finditer(line):
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(msg, dict) and msg.get("exit_code") is not None:
                    _done()

        LOG.log(levelno, line)

    args = list(args)

    proc = command()(
        "run",
        "--config",
        config_file,
        *args,
        _out=lambda x: _out(False, x),
        _err=lambda x: _out(True, x),
        _done=_done,
        **kwargs,
    )

    return proc


@functools.cache
def _conda_env_name():
    conda.update(_CONDA_ENV_NAME, _CONDA_PACKAGE_NAME)
    return _CONDA_ENV_NAME


def _to_caddy_file(config: Union[str, Path, Dict[str, Any]]) -> Path:
    """Materialize a Caddy configuration to a file and return its path."""
    if isinstance(config, Path):
        return config
    elif isinstance(config, Dict):
        config_content = objects.to_json(config, indent=2)
        config_extension = "json"
    else:
        config = str(config)
        if path := paths.path(config, exists=True):
            return path
        config_content = config
        config_extension = "caddyfile"
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=f".{config_extension}", delete=False
    ) as caddy_file:
        caddy_file.write(config_content)
        caddy_file.flush()
        return paths.path(caddy_file.name, absolute=True)


if __name__ == "__main__":
    log_line = ' {"level":"info","ts":1760473445.142467,"msg":"shutdown complete","signal":"SIGINT","exit_code":0}'
    caddy = run(
        """

    :8080 {
        log {
            output stdout
        }
        respond "Hello, world!"
    }
    """,
    )
    caddy.wait()
