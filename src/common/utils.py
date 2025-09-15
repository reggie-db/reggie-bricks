import logging
import os
import sys
from typing import Optional, List




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

def select_choice(title: str, choices: List[str], skip_single_choice: bool = True) -> Optional[str]:
    """
    Display a numbered list of choices and prompt the user to select one.

    Returns:
        The selected choice string, or None if input was invalid or cancelled.
    """
    if not choices or not (sys.stdin.isatty() or "PYCHARM_HOSTED" in os.environ):
        return None
    elif skip_single_choice and len(choices) == 1:
        return choices[0]
    print(title)
    for idx, choice in enumerate(choices, 1):
        print(f"  {idx}. {choice}")
    while True:
        raw = input(f"Select a number [1-{len(choices)}] (or press Enter to cancel): ").strip()
        if raw == "":
            return None
        if raw.isdigit():
            num = int(raw)
            if 1 <= num <= len(choices):
                return choices[num - 1]
        print(f"invalid selection - choice:{raw}")
