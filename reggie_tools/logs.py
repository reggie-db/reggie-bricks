import logging
import sys
from typing import Optional
from reggie_tools import runtimes


def logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger that routes < WARNING to stdout and >= WARNING to stderr.

    The logger is initialized once per name and cached on the logging module.
    """
    if not name:
        name = __name__
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
                handler.addFilter(lambda record: record.levelno < handler_levels[sys.stderr])
            log.addHandler(handler)

    return log

