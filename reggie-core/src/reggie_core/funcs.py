from typing import Any, Iterable, Sequence, TypeVar, Union

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


def to_iter(*value: Union[Iterable[T], T, None]) -> Iterable[T]:
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


def to_seq(*value: Union[Sequence[T], T, None]) -> Sequence[T]:
    """
    Ensure a value or values are returned as a sequence.

    Args:
        *value: One or more values or sequences.

    Returns:
        A sequence wrapping the input(s).
    """
    iter = to_iter(*value)
    return iter if isinstance(iter, Sequence) else list(iter)
