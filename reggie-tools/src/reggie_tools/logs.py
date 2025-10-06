import logging
import os
import sys
from typing import Optional

from reggie_tools import runtimes


def logger(
    name: Optional[str] = None,
    file: Optional[str] = None,
) -> logging.Logger:
    """
    Get a configured logger that routes < WARNING to stdout and >= WARNING to stderr.

    The logger is initialized once per name and cached on the logging module.
    """
    name = _logger_name(name, file)
    log = logging.getLogger(name)
    if not log.handlers:
        log.propagate = False
        log.setLevel(logging.DEBUG)
        handler_levels = {sys.stdout: logging.INFO, sys.stderr: logging.WARNING}
        if not runtimes.version():
            print(
                f"logger created - name:{name} stdout_level:{handler_levels[sys.stdout]} stderr_level:{handler_levels[sys.stderr]}"
            )
        fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        for stream, level in handler_levels.items():
            handler = logging.StreamHandler(stream)
            handler.setFormatter(fmt)
            handler.setLevel(level)
            if sys.stdout == stream:
                handler.addFilter(
                    lambda record: record.levelno < handler_levels[sys.stderr]
                )
            log.addHandler(handler)

    return log


def _logger_name(name: Optional[str], file: Optional[str]) -> Optional[str]:
    if name and name != "__main__":
        return name
    if file:
        file_name = os.path.splitext(os.path.basename(file))[0]
        return _logger_name(file_name, None)
    return __name__
