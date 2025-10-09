import functools
import logging
import os
import sys
from typing import Dict, Iterable, Optional, Tuple

from reggie_core import paths

_DEFAULT_LOGGER_NAME = __name__
_THREAD_NAME = False
_MAIN_THREAD_NAMES = [y.casefold() for y in ("MainThread", "main")]


def logger(
    name: Optional[str] = None,
    file: Optional[str] = None,
) -> logging.Logger:
    """
    Create or fetch a cached logger that sends levels below WARNING to stdout and WARNING or higher to stderr.
    The logger is initialized once per resolved name and does not propagate to root handlers.
    """
    name = _logger_name(name, file)
    log = logging.getLogger(name)
    if not log.handlers:
        log.propagate = False
        log.setLevel(logging.DEBUG)
        handler_levels = {sys.stdout: logging.INFO, sys.stderr: logging.WARNING}
        fmt = _Formatter()
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


def get_levels() -> Iterable[Tuple[int, str]]:
    """Return available logging levels as (value, name) pairs."""
    return [(val, name) for name, val in logging._nameToLevel.items()]


def get_level(level, default=None) -> Tuple[int, str]:
    """
    Resolve a level to (value, name). Accepts int or str. Matches exact names first then case-insensitive and prefix matches.
    If default is provided it is used when resolution fails.
    """
    if isinstance(level, int):
        name = logging._levelToName.get(level, None)
        if name:
            return level, name
    else:
        level = str(level).strip()
        val = logging._nameToLevel.get(level, None)
        if val:
            return val, level
    level_names = get_levels()
    for val, name in level_names:
        match = False
        if isinstance(level, int):
            match = val == level
        else:
            match = name.casefold() == level.casefold()
        if match:
            return val, name
    if not isinstance(level, int):
        matched: Dict[str, int] = {}
        for val, name in level_names:
            if name.casefold().startswith(level.casefold()):
                matched.setdefault(name, val)
        if len(matched) == 1:
            name, val = matched.popitem()
            return val, name
        elif len(matched) > 1:
            raise ValueError(f"Ambiguous level: {level}")
    if default is not None:
        return get_level(default)
    raise ValueError(f"Invalid level: {level}")


def _logger_name(name: Optional[str], file: Optional[str]) -> Optional[str]:
    """
    Resolve the logger name. Use explicit name unless it is __main__. Otherwise derive from file path or fall back to default.
    """
    if name and name != "__main__":
        return name
    if file:
        if file_path := paths.path(file):
            file_name = os.path.splitext(os.path.basename(file_path.name))[0]
            return _logger_name(file_name, None)
    return _DEFAULT_LOGGER_NAME


class _Formatter(logging.Formatter):
    """Formatter that pads level names and optionally includes logger name and thread name."""

    def __init__(self):
        """Configure base format with padded level, optional logger and thread parts."""
        super().__init__(
            "%(asctime)s %(level_bracketed)s%(name_part)s%(thread_part)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )

    def format(self, record):
        """Inject derived fields into the record and delegate to base formatter."""
        level_bracketed = _Formatter._format_level_name_bracketed(record.levelname)
        pad = _Formatter._level_name_bracketed_max_len() - len(level_bracketed)
        if pad > 0:
            level_bracketed = f"{level_bracketed}{' ' * pad}"
        record.level_bracketed = level_bracketed
        logger_name = record.name
        if not logger_name or logger_name == _DEFAULT_LOGGER_NAME:
            record.name_part = ""
        else:
            record.name_part = f" | {logger_name}"
        thread_name = record.threadName
        if (
            not _THREAD_NAME
            or not thread_name
            or thread_name.casefold() in _MAIN_THREAD_NAMES
        ):
            record.thread_part = ""
        else:
            record.thread_part = f" | {record.threadName}"
        return super().format(record)

    @staticmethod
    def _format_level_name(level) -> str:
        """Normalize level names to short forms like WARN CRIT NONE."""
        _, name = get_level(level)
        if name == logging.getLevelName(logging.WARNING):
            return "WARN"
        elif name == logging.getLevelName(logging.CRITICAL):
            return "CRIT"
        elif name == logging.getLevelName(logging.NOTSET):
            return "NONE"
        else:
            return name

    @staticmethod
    def _format_level_name_bracketed(level) -> str:
        """Return the bracketed level string such as [INFO]."""
        return f"[{_Formatter._format_level_name(level)}]"

    @staticmethod
    @functools.cache
    def _level_name_bracketed_max_len() -> int:
        """Compute the maximum width of bracketed level names for padding."""
        _bracketed_names = [
            _Formatter._format_level_name_bracketed(val) for val, _ in get_levels()
        ]
        max_bracketed_name = max(_bracketed_names, key=len)
        return len(max_bracketed_name)


if __name__ == "__main__":
    log = logger()
    log.info("suh")
    log = logger(None, __file__)
    log.info("suh2")
    print(get_level("crit"))
