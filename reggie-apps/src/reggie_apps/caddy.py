import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Union

from reggie_core import jsons, logs
from reggie_core.procs import Worker, WorkerOutputConsumer

LOG = logs.logger(__name__)

_LOG_LEVEL_PATTERN = re.compile(r'"level"\s*:\s*"(\S+?)"', re.IGNORECASE)


class CaddyWorker(Worker):
    def __init__(self, config: Union[Path, Dict[str, Any], str], **kwargs):
        self.caddy_file = CaddyWorker._to_caddy_file(config)
        kwargs.setdefault("bufsize", 1)
        super().__init__(
            [
                "caddy",
                "run",
                "--config",
                os.path.abspath(self.caddy_file),
            ],
            output_consumers=[CaddyWorker._log_handler()],
            **kwargs,
        )

    def stop(self, timeout: int = 10):
        super().stop(timeout)
        self.caddy_file.delete()

    @staticmethod
    def _to_caddy_file(config: Union[str, Path, Dict[str, Any]]) -> Path:
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
        caddy_file = tempfile.NamedTemporaryFile(
            mode="w+", suffix=f".{config_extension}", delete=False
        )
        caddy_file.write(config_content)
        caddy_file.flush()
        return Path(caddy_file.name)

    @staticmethod
    def _log_handler():
        def _log(_: Worker, error: bool, line: str):
            if error:
                level = logging.ERROR
                m = _LOG_LEVEL_PATTERN.search(line)
                if m:
                    level, _ = logs.get_level(m.group(1), default=(logging.ERROR, None))
            else:
                level = logging.INFO
            LOG.log(level, f"caddy | {line}")

        return WorkerOutputConsumer.create(handler=_log)
