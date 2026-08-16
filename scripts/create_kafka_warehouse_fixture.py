#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from neuro_sleep.streaming.device_event import (
    DEVICE_EVENT_SCHEMA_VERSION,
    DEVICE_EVENT_SOURCE_SYSTEM,
    DeviceEvent,
)
from neuro_sleep.streaming.device_event_inbox import (
    delete_inbox_event_for_smoke_test,
    persist_consumed_device_event,
)
from neuro_sleep.streaming.kafka_consumer import (
    ConsumedDeviceEvent,
)


def main() -> None:
    event = DeviceEvent(
        event_id=uuid4(),
        schema_version=DEVICE_EVENT_SCHEMA_VERSION,
        source_system=DEVICE_EVENT_SOURCE_SYSTEM,
        device_id="bci-device-warehouse-smoke",
        session_id=uuid4(),
        event_type="signal_quality",
        event_time=datetime.now(timezone.utc),
        sequence_number=0,
        payload={
            "quality_score": 0.98,
            "impedance_kohm": 5.75,
        },
    )

    offset = uuid4().int % 1_000_000_000_000

    consumed = ConsumedDeviceEvent(
        event=event,
        topic="neurosleep.smoke.warehouse-device-events.v1",
        partition=0,
        offset=offset,
        kafka_timestamp_ms=int(
            event.event_time.timestamp() * 1000
        ),
        ingested_at=datetime.now(timezone.utc),
        key=event.device_id,
        headers=(
            ("schema_version", event.schema_version),
            ("event_type", event.event_type),
        ),
    )

    delete_inbox_event_for_smoke_test(event.event_id)

    result = persist_consumed_device_event(consumed)

    if result.status != "inserted":
        raise AssertionError(
            "Warehouse fixture was not inserted."
        )

    print(f"kafka_warehouse_fixture_event_id={event.event_id}")
    print("kafka_warehouse_fixture_status=success")


if __name__ == "__main__":
    main()
