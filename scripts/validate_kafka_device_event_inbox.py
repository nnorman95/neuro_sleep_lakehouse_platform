#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from neuro_sleep.streaming.device_event import (
    DEVICE_EVENT_SCHEMA_VERSION,
    DEVICE_EVENT_SOURCE_SYSTEM,
    DeviceEvent,
)
from neuro_sleep.streaming.device_event_inbox import (
    DeviceEventIdentityConflict,
    delete_inbox_event_for_smoke_test,
    get_inbox_event,
    persist_consumed_device_event,
)
from neuro_sleep.streaming.kafka_consumer import (
    ConsumedDeviceEvent,
)


def build_consumed_event(
    *,
    event: DeviceEvent,
    offset: int,
) -> ConsumedDeviceEvent:
    return ConsumedDeviceEvent(
        event=event,
        topic="neurosleep.smoke.device-events.v1",
        partition=0,
        offset=offset,
        kafka_timestamp_ms=int(
            event.event_time.timestamp() * 1000
        ),
        ingested_at=datetime.now(timezone.utc),
        key=event.device_id,
        headers=(
            (
                "schema_version",
                event.schema_version,
            ),
            (
                "event_type",
                event.event_type,
            ),
        ),
    )


def main() -> None:
    event = DeviceEvent(
        event_id=uuid4(),
        schema_version=DEVICE_EVENT_SCHEMA_VERSION,
        source_system=DEVICE_EVENT_SOURCE_SYSTEM,
        device_id="bci-device-inbox-smoke",
        session_id=uuid4(),
        event_type="signal_quality",
        event_time=datetime.now(timezone.utc),
        sequence_number=7,
        payload={
            "quality_score": 0.95,
            "impedance_kohm": 7.25,
        },
    )

    base_offset = (
        uuid4().int
        % 1_000_000_000_000
    )

    first = build_consumed_event(
        event=event,
        offset=base_offset,
    )
    duplicate = build_consumed_event(
        event=event,
        offset=base_offset + 1,
    )
    conflict = build_consumed_event(
        event=replace(
            event,
            payload={
                "quality_score": 0.10,
                "impedance_kohm": 99.0,
            },
        ),
        offset=base_offset + 2,
    )

    delete_inbox_event_for_smoke_test(
        event.event_id
    )

    try:
        first_result = persist_consumed_device_event(
            first
        )

        if (
            first_result.status != "inserted"
            or first_result.delivery_count != 1
        ):
            raise AssertionError(
                "First inbox write contract failed"
            )

        duplicate_result = (
            persist_consumed_device_event(
                duplicate
            )
        )

        if (
            duplicate_result.status != "duplicate"
            or duplicate_result.delivery_count != 2
        ):
            raise AssertionError(
                "Duplicate inbox write contract failed"
            )

        row = get_inbox_event(
            event.event_id
        )

        if row[12] != base_offset:
            raise AssertionError(
                "First Kafka offset was not preserved"
            )

        if row[14] != base_offset + 1:
            raise AssertionError(
                "Last Kafka offset was not refreshed"
            )

        conflict_blocked = False

        try:
            persist_consumed_device_event(
                conflict
            )
        except DeviceEventIdentityConflict:
            conflict_blocked = True

        if not conflict_blocked:
            raise AssertionError(
                "Conflicting event_id reuse "
                "was not blocked"
            )

        row_after_conflict = get_inbox_event(
            event.event_id
        )

        if row_after_conflict[19] != 2:
            raise AssertionError(
                "Conflicting duplicate changed "
                "delivery_count"
            )

        if row_after_conflict[8] != event.to_dict():
            raise AssertionError(
                "Conflicting duplicate changed raw_event"
            )

        print(
            "kafka_inbox_first_write=inserted"
        )
        print(
            "kafka_inbox_duplicate_write=duplicate"
        )
        print(
            "kafka_inbox_delivery_count=2"
        )
        print(
            "kafka_inbox_first_coordinate_preserved=true"
        )
        print(
            "kafka_inbox_last_coordinate_refreshed=true"
        )
        print(
            "kafka_inbox_identity_conflict_blocked=true"
        )
        print(
            "kafka_inbox_event_id_deduplication=success"
        )
        print(
            "kafka_inbox_smoke_status=success"
        )
    finally:
        delete_inbox_event_for_smoke_test(
            event.event_id
        )


if __name__ == "__main__":
    main()
