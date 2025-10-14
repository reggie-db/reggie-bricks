import functools
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Union

import sh
from reggie_core import jsons, logs, paths

from reggie_app_runner import conda

LOG = logs.logger("caddy")

_CONDA_ENV_NAME = "_caddy"
_CONDA_PACKAGE_NAME = "caddy"
_LOG_LEVEL_PATTERN = re.compile(r'"level"\s*:\s*"(\S+?)"', re.IGNORECASE)


def command(*args, **kwargs) -> sh.Command:
    conda_env_name = _conda_env_name()
    return conda.run_command(conda_env_name).bake("caddy", *args, **kwargs)


def start(
    config: Union[Path, Dict[str, Any], str], *args, **kwargs
) -> sh.RunningCommand:
    config_file = _to_caddy_file(config)

    def _out(error, line):
        levelno = logging.ERROR if error else logging.INFO
        line = line.rstrip()
        if line:
            for match in _LOG_LEVEL_PATTERN.finditer(line):
                line_levelno, _ = logs.get_level(match.group(1))
                if line_levelno:
                    levelno = line_levelno
                    break
        LOG.log(levelno, line)

    cmd = command(
        "run",
        "--config",
        config_file,
        *args,
        _bg=True,
        _out=lambda x: _out(False, x),
        _err=lambda x: _out(True, x),
        **kwargs,
    )

    def _done(*_):
        print("done")
        os.unlink(config_file)

    proc = cmd(
        _done=_done,
    )
    setattr(proc, "config_file", config_file)
    return proc


@functools.cache
def _conda_env_name():
    conda.update(_CONDA_PACKAGE_NAME, env_name=_CONDA_ENV_NAME)
    return _CONDA_ENV_NAME


def _to_caddy_file(config: Union[str, Path, Dict[str, Any]]) -> Path:
    """Materialize a Caddy configuration to a file and return its path."""
    if isinstance(config, Path):
        return config
    elif isinstance(config, Dict):
        config_content = jsons.dumps(config, indent=2)
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
        return Path(os.path.abspath(caddy_file.name))


if __name__ == "__main__":
    caddy = start(
        """

    :8080 {
        log {
            output stdout
        }
        respond "Hello, world!"
    }
    """,
    )
    print(caddy.config_file)
    try:
        caddy.wait()
    finally:
        caddy.kill_group()
        try:
            caddy.wait()
        except BaseException:
            pass
        raise
