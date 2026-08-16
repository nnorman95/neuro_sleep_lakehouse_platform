from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from neuro_sleep.identifiers import new_uuid7
from neuro_sleep.streaming.device_event import (
    DEVICE_EVENT_SCHEMA_VERSION,
    DEVICE_EVENT_SOURCE_SYSTEM,
    DeviceEvent,
)


def _valid_event_dict() -> dict:
    return {
        "event_id": str(new_uuid7()),
        "schema_version": DEVICE_EVENT_SCHEMA_VERSION,
        "source_system": DEVICE_EVENT_SOURCE_SYSTEM,
        "device_id": "bci-device-001",
        "session_id": str(new_uuid7()),
        "event_type": "signal_quality",
        "event_time": (
            datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        ),
        "sequence_number": 7,
        "payload": {
            "quality_score": 0.97,
        },
    }


def _expect_failure(
    value: dict,
    expected_exception: type[Exception],
) -> bool:
    try:
        DeviceEvent.from_dict(value)
    except expected_exception:
        return True

    return False


def main() -> None:
    original = _valid_event_dict()
    event = DeviceEvent.from_dict(original)
    round_trip = event.to_dict()

    if round_trip != original:
        raise AssertionError(
            "Device event round trip changed the payload."
        )

    missing_event_id = deepcopy(original)
    missing_event_id.pop("event_id")

    unsupported_schema = deepcopy(original)
    unsupported_schema["schema_version"] = "2.0.0"

    negative_sequence = deepcopy(original)
    negative_sequence["sequence_number"] = -1

    non_utc_time = deepcopy(original)
    non_utc_time["event_time"] = "2026-08-16T10:00:00+04:00"

    unexpected_field = deepcopy(original)
    unexpected_field["unknown_field"] = "blocked"

    invalid_payload = deepcopy(original)
    invalid_payload["payload"] = []

    checks = {
        "device_event_round_trip": round_trip == original,
        "device_event_missing_field_blocked": _expect_failure(
            missing_event_id,
            ValueError,
        ),
        "device_event_schema_version_blocked": _expect_failure(
            unsupported_schema,
            ValueError,
        ),
        "device_event_negative_sequence_blocked": _expect_failure(
            negative_sequence,
            ValueError,
        ),
        "device_event_non_utc_time_blocked": _expect_failure(
            non_utc_time,
            ValueError,
        ),
        "device_event_unexpected_field_blocked": _expect_failure(
            unexpected_field,
            ValueError,
        ),
        "device_event_invalid_payload_blocked": _expect_failure(
            invalid_payload,
            TypeError,
        ),
    }

    for name, passed in checks.items():
        print(f"{name}={str(passed).lower()}")

        if not passed:
            raise AssertionError(
                f"Device event smoke check failed: {name}"
            )

    print("device_event_contract_smoke_status=success")


if __name__ == "__main__":
    main()
