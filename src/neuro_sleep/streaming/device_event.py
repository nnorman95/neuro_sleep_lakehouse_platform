from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID


DEVICE_EVENT_SCHEMA_VERSION = "1.0.0"
DEVICE_EVENT_SOURCE_SYSTEM = "simulated_bci_device"

DeviceEventType = Literal[
    "session_started",
    "signal_quality",
    "battery_status",
    "session_ended",
]

ALLOWED_DEVICE_EVENT_TYPES = {
    "session_started",
    "signal_quality",
    "battery_status",
    "session_ended",
}


def _require_non_empty(
    value: str,
    field_name: str,
) -> str:
    cleaned = value.strip()

    if not cleaned:
        raise ValueError(
            f"{field_name} cannot be empty"
        )

    return cleaned


def _require_utc(
    value: datetime,
    field_name: str,
) -> datetime:
    if value.tzinfo is None:
        raise ValueError(
            f"{field_name} must be timezone-aware"
        )

    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(
            f"{field_name} must be UTC"
        )

    return value


@dataclass(frozen=True)
class DeviceEvent:
    event_id: UUID
    schema_version: str
    source_system: str
    device_id: str
    session_id: UUID
    event_type: DeviceEventType
    event_time: datetime
    sequence_number: int
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        if self.schema_version != DEVICE_EVENT_SCHEMA_VERSION:
            raise ValueError(
                "Unsupported device event schema_version: "
                f"{self.schema_version}"
            )

        if self.source_system != DEVICE_EVENT_SOURCE_SYSTEM:
            raise ValueError(
                "Unsupported device event source_system: "
                f"{self.source_system}"
            )

        _require_non_empty(
            value=self.device_id,
            field_name="device_id",
        )

        if self.event_type not in ALLOWED_DEVICE_EVENT_TYPES:
            raise ValueError(
                "Unsupported device event event_type: "
                f"{self.event_type}"
            )

        _require_utc(
            value=self.event_time,
            field_name="event_time",
        )

        if self.sequence_number < 0:
            raise ValueError(
                "sequence_number cannot be negative"
            )

        if not isinstance(self.payload, dict):
            raise TypeError(
                "payload must be a dictionary"
            )

    @classmethod
    def from_dict(
        cls,
        value: dict[str, Any],
    ) -> "DeviceEvent":
        expected_fields = {
            "event_id",
            "schema_version",
            "source_system",
            "device_id",
            "session_id",
            "event_type",
            "event_time",
            "sequence_number",
            "payload",
        }

        actual_fields = set(value)
        missing_fields = expected_fields - actual_fields
        unexpected_fields = actual_fields - expected_fields

        if missing_fields:
            missing = ", ".join(sorted(missing_fields))
            raise ValueError(
                f"Missing device event fields: {missing}"
            )

        if unexpected_fields:
            unexpected = ", ".join(
                sorted(unexpected_fields)
            )
            raise ValueError(
                "Unexpected device event fields: "
                f"{unexpected}"
            )

        raw_event_type = value["event_type"]

        if not isinstance(raw_event_type, str):
            raise TypeError(
                "event_type must be a string"
            )

        raw_event_time = value["event_time"]

        if not isinstance(raw_event_time, str):
            raise TypeError(
                "event_time must be a string"
            )

        parsed_event_time = datetime.fromisoformat(
            raw_event_time.replace("Z", "+00:00")
        )

        raw_sequence_number = value["sequence_number"]

        if (
            not isinstance(raw_sequence_number, int)
            or isinstance(raw_sequence_number, bool)
        ):
            raise TypeError(
                "sequence_number must be an integer"
            )

        raw_payload = value["payload"]

        if not isinstance(raw_payload, dict):
            raise TypeError(
                "payload must be a dictionary"
            )

        return cls(
            event_id=UUID(str(value["event_id"])),
            schema_version=str(value["schema_version"]),
            source_system=str(value["source_system"]),
            device_id=str(value["device_id"]),
            session_id=UUID(str(value["session_id"])),
            event_type=raw_event_type,
            event_time=parsed_event_time,
            sequence_number=raw_sequence_number,
            payload=raw_payload,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id),
            "schema_version": self.schema_version,
            "source_system": self.source_system,
            "device_id": self.device_id,
            "session_id": str(self.session_id),
            "event_type": self.event_type,
            "event_time": (
                self.event_time
                .astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            ),
            "sequence_number": self.sequence_number,
            "payload": self.payload,
        }
