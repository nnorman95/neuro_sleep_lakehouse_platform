from __future__ import annotations

from dataclasses import dataclass

from neuro_sleep.recording_scope import select_recording_scope


@dataclass(frozen=True)
class Fixture:
    recording_key: str


def run_smoke_test() -> None:
    items = (
        Fixture("SC4001E"),
        Fixture("SC4002E"),
        Fixture("SC4011E"),
    )

    unchanged = select_recording_scope(
        items,
        (),
        key=lambda item: item.recording_key,
        scope_name="signal_feature_recording_scope",
    )
    if unchanged != items:
        raise RuntimeError("Empty recording scope must preserve all items")

    selected = select_recording_scope(
        items,
        ("SC4002E", "SC4001E"),
        key=lambda item: item.recording_key,
        scope_name="signal_feature_recording_scope",
    )
    if tuple(item.recording_key for item in selected) != (
        "SC4002E",
        "SC4001E",
    ):
        raise RuntimeError("Recording scope did not preserve requested order")

    missing_blocked = False
    try:
        select_recording_scope(
            items,
            ("SC9999E",),
            key=lambda item: item.recording_key,
            scope_name="signal_feature_recording_scope",
        )
    except RuntimeError as exc:
        missing_blocked = "SC9999E" in str(exc) and "unavailable" in str(exc)
    if not missing_blocked:
        raise RuntimeError("Unavailable recording scope key was not blocked")

    duplicate_request_blocked = False
    try:
        select_recording_scope(
            items,
            ("SC4001E", "SC4001E"),
            key=lambda item: item.recording_key,
            scope_name="signal_feature_recording_scope",
        )
    except RuntimeError as exc:
        duplicate_request_blocked = "duplicate" in str(exc)
    if not duplicate_request_blocked:
        raise RuntimeError("Duplicate requested recording key was not blocked")

    duplicate_available_blocked = False
    try:
        select_recording_scope(
            (Fixture("SC4001E"), Fixture("SC4001E")),
            ("SC4001E",),
            key=lambda item: item.recording_key,
            scope_name="signal_feature_recording_scope",
        )
    except RuntimeError as exc:
        duplicate_available_blocked = "duplicate available" in str(exc)
    if not duplicate_available_blocked:
        raise RuntimeError("Duplicate available recording key was not blocked")

    print("recording_scope_empty_preserves_all=true")
    print("recording_scope_requested_order=true")
    print("recording_scope_missing_key_blocked=true")
    print("recording_scope_duplicate_request_blocked=true")
    print("recording_scope_duplicate_available_blocked=true")
    print("recording_scope_smoke_status=success")


if __name__ == "__main__":
    run_smoke_test()
