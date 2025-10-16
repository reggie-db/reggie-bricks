import inspect
from collections.abc import Iterable, Sequence
from dataclasses import asdict, is_dataclass
from typing import Any, TypeVar

T = TypeVar("T")


def merge(
    *dicts: dict[Any, Any], update: bool = True, mutate: bool = False
) -> dict[Any, Any]:
    """
    Merge multiple dictionaries into one.

    Args:
        *dicts: Dictionaries to merge.
        update: If True, later values overwrite earlier ones.
        mutate: If True, modify the first non-null dict in place.

    Returns:
        A merged dictionary.
    """
    if not dicts:
        return {}
    result: dict[Any, Any] | None = None
    for d in dicts:
        if d is None:
            continue
        if result is None:
            result = dict(d) if not mutate else d
            continue
        for k, v in d.items():
            if update or k not in result:
                result[k] = v
    return result


def to_dict(obj: Any, recursive: bool = True) -> dict[Any, Any]:
    def _try_to_dict(value: Any) -> dict[Any, Any]:
        if value is None:
            return None
        if isinstance(value, dict):
            d = value
        elif recursive and is_dataclass(value):
            d = asdict(value)
        else:
            d = getattr(value, "__dict__", None)
            if not isinstance(d, dict):
                return value
            d = d.copy()
        for member_name, member_value in inspect.getmembers(value.__class__):
            if isinstance(member_value, property):
                d[member_name] = member_value.fget(value)
        if recursive:
            for k, v in d.items():
                d[k] = _try_to_dict(v)
        return d

    data = _try_to_dict(obj)
    if data is None:
        return None
    elif not isinstance(data, dict):
        raise TypeError(f"Dict conversion failed, got {type(data)}")
    return data


def to_iter(*value: Iterable[T] | T | None) -> Iterable[T]:
    """
    Ensure a value or values are returned as an iterable.

    Args:
        *value: One or more values or iterables.

    Returns:
        An iterable wrapping the input(s).
    """
    if len(value) == 0:
        return []
    elif len(value) == 1:
        first = value[0]
        if isinstance(first, Iterable) and not isinstance(
            first, (str, bytes, bytearray)
        ):
            return first
    return value


def to_seq(*value: Sequence[T] | T | None) -> Sequence[T]:
    """
    Ensure a value or values are returned as a sequence.

    Args:
        *value: One or more values or sequences.

    Returns:
        A sequence wrapping the input(s).
    """
    iter = to_iter(*value)
    return iter if isinstance(iter, Sequence) else list(iter)
