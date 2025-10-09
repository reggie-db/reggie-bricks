import asyncio
import functools
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Union

from reggie_core import funcs, jsons, logs
from reggie_core.procs import Worker, WorkerFuture

from reggie_app_runner import conda

LOG = logs.logger(__name__)

_CONDA_PACKAGE_NAME = "caddy"
_LOG_LEVEL_PATTERN = re.compile(r'"level"\s*:\s*"(\S+?)"', re.IGNORECASE)


class CaddyWorker(Worker):
    """Worker specialization that runs a Caddy process with a provided config."""

    def __init__(
        self,
        config: Union[Path, Dict[str, Any], str],
        **kwargs,
    ):
        """Store config and pass remaining kwargs to the base Worker."""
        super().__init__(**kwargs)
        self.config = config

    def _run_args(self, fut: WorkerFuture):
        """Build the command-line for Caddy and schedule temp-file cleanup on completion."""
        commands = super()._run_args(fut)
        caddy_file = _to_caddy_file(self.config)
        fut.add_done_callback(lambda: caddy_file.unlink())
        commands.extend(
            [
                "caddy",
                "run",
                "--config",
                os.path.abspath(caddy_file),
            ]
        )
        return commands

    def _run_kwargs(self, fut: WorkerFuture):
        """Augment kwargs with environment variables required to run Caddy via Conda."""
        kwargs = super()._run_kwargs(fut)
        kwargs["env"] = funcs.merge(kwargs.get("env", None), _env(), update=False)
        return kwargs

    def _output_write(
        self, process: asyncio.subprocess.Process, error: bool, line_str: str
    ):
        """Parse Caddy JSON logs for level and route to stdout/stderr accordingly."""
        m = _LOG_LEVEL_PATTERN.search(line_str)
        if m:
            level, _ = logs.get_level(
                m.group(1), logging.ERROR if error else logging.INFO
            )
            error = level > logging.INFO
        line_str = f"caddy_{process.pid} | {line_str}"
        super()._output_write(process, error, line_str)


@functools.cache
def _env():
    """Ensure Caddy is installed and return the Conda-augmented environment mapping."""
    conda.install(_CONDA_PACKAGE_NAME)
    return conda.env()


def _to_caddy_file(config: Union[str, Path, Dict[str, Any]]) -> Path:
    """Materialize a Caddy configuration to a file and return its path."""
    if isinstance(config, Path):
        return config
    elif isinstance(config, Dict):
        config_content = jsons.dumps(config, indent=2)
        config_extension = "json"
    else:
        config = str(config)
        try:
            return Path(config).resolve(strict=True)
        except (FileNotFoundError, PermissionError, OSError):
            pass
        config_content = config
        config_extension = "caddyfile"
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=f".{config_extension}", delete=False
    ) as caddy_file:
        caddy_file.write(config_content)
        caddy_file.flush()
        return Path(caddy_file.name)


if __name__ == "__main__":
    log = logs.logger(__name__)
    worker = CaddyWorker(config={}, stdout_writer=log.info, stderr_writer=log.error)
    worker.run_threaded(daemon=False).result()
