import logging
import sys
import threading
from typing import Iterable

_LOGGING_AUTO_CONFIG_LOCK = threading.Lock()
_LOGGING_AUTO_CONFIG_MARK = object()


def logging_auto_config():
    if not logging.getLogger().handlers:
        with _LOGGING_AUTO_CONFIG_LOCK:
            if not logging.getLogger().handlers:
                _logging_auto_config()


def _logging_auto_config():
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


logging_auto_config()
