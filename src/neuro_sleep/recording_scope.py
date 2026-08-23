from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TypeVar


T = TypeVar("T")


def select_recording_scope(
    items: Sequence[T],
    requested_keys: tuple[str, ...],
    *,
    key: Callable[[T], str],
    scope_name: str,
) -> tuple[T, ...]:
    values = tuple(items)

    if not requested_keys:
        return values

    if len(requested_keys) != len(set(requested_keys)):
        raise RuntimeError(
            f"{scope_name} contains duplicate recording keys"
        )

    by_key: dict[str, T] = {}
    duplicate_keys: set[str] = set()

    for item in values:
        recording_key = key(item)
        if recording_key in by_key:
            duplicate_keys.add(recording_key)
            continue
        by_key[recording_key] = item

    if duplicate_keys:
        raise RuntimeError(
            f"{scope_name} has duplicate available recording keys: "
            + ", ".join(sorted(duplicate_keys))
        )

    missing_keys = [
        recording_key
        for recording_key in requested_keys
        if recording_key not in by_key
    ]
    if missing_keys:
        raise RuntimeError(
            f"{scope_name} contains unavailable recording keys: "
            + ", ".join(missing_keys)
        )

    return tuple(
        by_key[recording_key]
        for recording_key in requested_keys
    )
