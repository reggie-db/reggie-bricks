import logging
import os
import sys
import threading
from typing import Iterable

_LOGGING_AUTO_CONFIG_LOCK = threading.Lock()
_LOGGING_AUTO_CONFIG_MARK = object()


def logging_auto_config():
    print("logging_auto_config check")
    if _log_empty(logging.getLogger()):
        with _LOGGING_AUTO_CONFIG_LOCK:
            if _log_empty(logging.getLogger()):
                _logging_auto_config()


def _log_empty(log):
    if log.handlers:
        for handler in log.handlers:
            handler_level = handler.level
            if handler_level != logging.NOTSET:
                return False
    return True


def _logging_auto_config():
    print("logging_auto_config")
    logging.basicConfig(level=logging.INFO, handlers=list(_auto_config_handlers()))

    def _has_auto_config_handlers():
        if handlers := logging.getLogger().handlers:
            for handler in handlers:
                if _LOGGING_AUTO_CONFIG_MARK is getattr(
                    handler, "auto_config_mark", None
                ):
                    return True
        return False

    if not _has_auto_config_handlers():
        return

    logging_basic_config = logging.basicConfig

    def logging_basic_config_wrapper(*args, **kwargs):
        kwargs.get("logging_basic_config_wrapper")
        if not kwargs.get("force") and _has_auto_config_handlers():
            kwargs["force"] = True
        logging_basic_config(*args, **kwargs)

    logging.basicConfig = logging_basic_config_wrapper


def _auto_config_handlers() -> Iterable[logging.Handler]:
    for error in [False, True]:
        stream = sys.stdout if not error else sys.stderr
        yield _auto_config_handler(stream, error)


def _auto_config_handler(stream, error: bool) -> logging.Handler:
    from reggie_core.logs import Formatter, Handler

    handler = Handler(stream=stream)
    handler.auto_config_mark = _LOGGING_AUTO_CONFIG_MARK
    handler.setFormatter(Formatter(stream=stream))
    if error:
        handler.setLevel(logging.WARN)
    else:
        handler.addFilter(lambda record: record.levelno < logging.WARNING)
    return handler


if __name__ == "__main__":
    logging_auto_config()
    for i in range(4):
        if i > 1:
            fmt = str(i) + " - [%(levelname)s]%(message)s"
            logging.basicConfig(
                level=logging.DEBUG,
                format=fmt,
            )
        log = logging.getLogger()
        if i > 0:
            log.setLevel(logging.DEBUG)
        log.debug(f"debug {i}")
        log.info(f"info {i}")
        log.warning(f"warning {i}")
        log.error(f"error {i}")
