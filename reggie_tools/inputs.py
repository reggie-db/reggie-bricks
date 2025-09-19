import os
import sys
from typing import List, Optional


def is_interactive() -> bool:
    return sys.stdin.isatty() or "PYCHARM_HOSTED" in os.environ


def select_choice(
    title: str, choices: List[str], skip_single_choice: bool = True
) -> Optional[str]:
    """
    Display a numbered list of choices and prompt the user to select one.

    Returns:
        The selected choice string, or None if input was invalid or cancelled.
    """
    if not choices or not is_interactive():
        return None
    elif skip_single_choice and len(choices) == 1:
        return choices[0]
    print(title)
    for idx, choice in enumerate(choices, 1):
        print(f"  {idx}. {choice}")
    while True:
        raw = input(
            f"Select a number [1-{len(choices)}] (or press Enter to cancel): "
        ).strip()
        if raw == "":
            return None
        if raw.isdigit():
            num = int(raw)
            if 1 <= num <= len(choices):
                return choices[num - 1]
        print(f"invalid selection - choice:{raw}")
