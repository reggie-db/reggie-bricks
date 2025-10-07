import functools
import logging
import os
import sys
from typing import Dict, Iterable, Optional, Tuple

_DEFAULT_LOGGER_NAME = "reggie_bricks"


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
        fmt = _BracketedLevelFormatter(
            "%(asctime)s %(level_bracketed)s | %(name)s | %(threadName)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
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
    return [(val, name) for name, val in logging._nameToLevel.items()]


def get_level(level, default: Optional[Tuple[int, str]] = None) -> Tuple[int, str]:
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
        return default
    raise ValueError(f"Invalid level: {level}")


def _logger_name(name: Optional[str], file: Optional[str]) -> Optional[str]:
    if name and name != "__main__":
        return name
    if file:
        file_name = os.path.splitext(os.path.basename(file))[0]
        return _logger_name(file_name, None)
    return _DEFAULT_LOGGER_NAME


class _BracketedLevelFormatter(logging.Formatter):
    def format(self, record):
        level_bracketed = _BracketedLevelFormatter._format_level_name_bracketed(
            record.levelname
        )
        pad = _BracketedLevelFormatter._level_name_bracketed_max_len() - len(
            level_bracketed
        )
        if pad > 0:
            level_bracketed = f"{level_bracketed}{' ' * pad}"
        record.level_bracketed = level_bracketed
        return super().format(record)

    @staticmethod
    def _format_level_name(level) -> str:
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
        return f"[{_BracketedLevelFormatter._format_level_name(level)}]"

    @staticmethod
    @functools.cache
    def _level_name_bracketed_max_len() -> int:
        _bracketed_names = [
            _BracketedLevelFormatter._format_level_name_bracketed(val)
            for val, _ in get_levels()
        ]
        max_bracketed_name = max(_bracketed_names, key=len)
        return len(max_bracketed_name)


if __name__ == "__main__":
    print(get_level("crit"))
