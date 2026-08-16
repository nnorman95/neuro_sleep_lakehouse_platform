from __future__ import annotations

from datetime import datetime, timedelta, timezone
from random import Random

from neuro_sleep.identifiers import new_uuid7
from neuro_sleep.streaming.device_event import (
    DEVICE_EVENT_SCHEMA_VERSION,
    DEVICE_EVENT_SOURCE_SYSTEM,
    DeviceEvent,
)


SIMULATED_FIRMWARE_VERSION = "sim-1.0.0"
SIMULATED_SAMPLING_RATE_HZ = 256


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def generate_simulated_device_session(
    *,
    device_id: str,
    signal_quality_events: int,
    seed: int,
    start_time: datetime | None = None,
) -> list[DeviceEvent]:
    if not device_id.strip():
        raise ValueError("device_id cannot be empty")

    if signal_quality_events <= 0:
        raise ValueError(
            "signal_quality_events must be a positive integer"
        )

    session_start = start_time or _utc_now()

    if session_start.tzinfo is None:
        raise ValueError(
            "start_time must be timezone-aware"
        )

    session_start = session_start.astimezone(timezone.utc)

    random = Random(seed)
    session_id = new_uuid7()
    events: list[DeviceEvent] = []
    sequence_number = 0
    event_time = session_start
    battery_percent = random.randint(82, 98)

    def append_event(
        event_type: str,
        payload: dict,
    ) -> None:
        nonlocal sequence_number

        events.append(
            DeviceEvent(
                event_id=new_uuid7(),
                schema_version=DEVICE_EVENT_SCHEMA_VERSION,
                source_system=DEVICE_EVENT_SOURCE_SYSTEM,
                device_id=device_id,
                session_id=session_id,
                event_type=event_type,
                event_time=event_time,
                sequence_number=sequence_number,
                payload=payload,
            )
        )
        sequence_number += 1

    append_event(
        "session_started",
        {
            "firmware_version": SIMULATED_FIRMWARE_VERSION,
            "sampling_rate_hz": SIMULATED_SAMPLING_RATE_HZ,
        },
    )

    for index in range(signal_quality_events):
        event_time += timedelta(seconds=5)

        append_event(
            "signal_quality",
            {
                "quality_score": round(
                    random.uniform(0.82, 0.99),
                    3,
                ),
                "impedance_kohm": round(
                    random.uniform(4.0, 12.0),
                    2,
                ),
            },
        )

        if (index + 1) % 2 == 0:
            event_time += timedelta(seconds=1)
            battery_percent = max(
                0,
                battery_percent - random.randint(0, 1),
            )

            append_event(
                "battery_status",
                {
                    "battery_percent": battery_percent,
                    "charging": False,
                },
            )

    event_time += timedelta(seconds=5)

    append_event(
        "session_ended",
        {
            "reason": "normal",
            "duration_seconds": int(
                (event_time - session_start).total_seconds()
            ),
        },
    )

    _validate_generated_session(events)
    return events


def _validate_generated_session(
    events: list[DeviceEvent],
) -> None:
    if not events:
        raise ValueError(
            "Generated device session cannot be empty"
        )

    session_ids = {
        event.session_id
        for event in events
    }
    device_ids = {
        event.device_id
        for event in events
    }

    if len(session_ids) != 1:
        raise ValueError(
            "Generated events must share one session_id"
        )

    if len(device_ids) != 1:
        raise ValueError(
            "Generated events must share one device_id"
        )

    expected_sequences = list(range(len(events)))
    actual_sequences = [
        event.sequence_number
        for event in events
    ]

    if actual_sequences != expected_sequences:
        raise ValueError(
            "Generated event sequence is not contiguous"
        )

    event_times = [
        event.event_time
        for event in events
    ]

    if event_times != sorted(event_times):
        raise ValueError(
            "Generated event_time values are not ordered"
        )

    if events[0].event_type != "session_started":
        raise ValueError(
            "Generated session must start with session_started"
        )

    if events[-1].event_type != "session_ended":
        raise ValueError(
            "Generated session must end with session_ended"
        )
