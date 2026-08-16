#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from uuid import uuid4

from confluent_kafka import (
    Consumer,
    Producer,
    TopicPartition,
)
from dotenv import load_dotenv

from neuro_sleep.paths import PROJECT_ROOT
from neuro_sleep.streaming.device_event import (
    DEVICE_EVENT_SCHEMA_VERSION,
    DEVICE_EVENT_SOURCE_SYSTEM,
    DeviceEvent,
)
from neuro_sleep.streaming.device_event_inbox import (
    DEVICE_EVENT_ARRIVAL_CLASSIFICATION_VERSION,
    DEVICE_EVENT_LATE_THRESHOLD_MS,
    delete_inbox_event_for_smoke_test,
    get_inbox_event,
    persist_consumed_device_event,
)
from neuro_sleep.streaming.kafka_consumer import (
    KafkaDeviceEventConsumer,
    get_topic_end_offsets,
)
from neuro_sleep.streaming.kafka_producer import (
    load_device_event_topic,
)


def committed_offset(
    *,
    bootstrap_servers: str,
    group_id: str,
    topic_name: str,
    partition: int,
) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )

    try:
        result = consumer.committed(
            [TopicPartition(topic_name, partition)],
            timeout=10.0,
        )

        if len(result) != 1:
            raise AssertionError(
                "Unexpected committed offset result"
            )

        item = result[0]

        if item.error is not None:
            raise RuntimeError(
                "Committed offset lookup failed: "
                f"{item.error}"
            )

        return item.offset
    finally:
        consumer.close()


def produce_in_arrival_order(
    *,
    bootstrap_servers: str,
    topic_name: str,
    events: list[DeviceEvent],
) -> dict[str, tuple[int, int]]:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": (
                "neurosleep-arrival-classification-smoke"
            ),
            "enable.idempotence": True,
            "acks": "all",
            "allow.auto.create.topics": False,
            "partitioner": "murmur2_random",
        }
    )

    delivered: dict[str, tuple[int, int]] = {}
    errors: list[str] = []

    for event in events:
        serialized = json.dumps(
            event.to_dict(),
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

        def on_delivery(
            error,
            message,
            *,
            event_id: str = str(event.event_id),
        ) -> None:
            if error is not None:
                errors.append(
                    f"{event_id}: {error}"
                )
                return

            delivered[event_id] = (
                message.partition(),
                message.offset(),
            )

        producer.produce(
            topic=topic_name,
            key=event.device_id.encode("utf-8"),
            value=serialized,
            timestamp=int(
                event.event_time.timestamp() * 1000
            ),
            headers=[
                (
                    "schema_version",
                    event.schema_version,
                ),
                (
                    "event_type",
                    event.event_type,
                ),
            ],
            on_delivery=on_delivery,
        )

        producer.poll(0)

    undelivered = producer.flush(20.0)

    if undelivered:
        raise RuntimeError(
            "Arrival-classification fixture "
            "was not fully delivered"
        )

    if errors:
        raise RuntimeError(
            "Arrival-classification fixture "
            "delivery failed: "
            + "; ".join(errors)
        )

    if len(delivered) != len(events):
        raise AssertionError(
            "Arrival fixture receipt count mismatch"
        )

    return delivered


def main() -> None:
    load_dotenv(
        PROJECT_ROOT / ".env",
        override=False,
    )

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    ).strip()

    topic = load_device_event_topic()

    start_offsets = get_topic_end_offsets(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )

    now = datetime.now(timezone.utc)
    device_id = "bci-device-arrival-smoke"
    session_id = uuid4()

    event_0 = DeviceEvent(
        event_id=uuid4(),
        schema_version=DEVICE_EVENT_SCHEMA_VERSION,
        source_system=DEVICE_EVENT_SOURCE_SYSTEM,
        device_id=device_id,
        session_id=session_id,
        event_type="session_started",
        event_time=now - timedelta(seconds=10),
        sequence_number=0,
        payload={
            "firmware_version": "smoke",
            "sampling_rate_hz": 256,
        },
    )

    event_2 = DeviceEvent(
        event_id=uuid4(),
        schema_version=DEVICE_EVENT_SCHEMA_VERSION,
        source_system=DEVICE_EVENT_SOURCE_SYSTEM,
        device_id=device_id,
        session_id=session_id,
        event_type="signal_quality",
        event_time=now - timedelta(seconds=5),
        sequence_number=2,
        payload={
            "quality_score": 0.97,
            "impedance_kohm": 6.5,
        },
    )

    event_1_late = DeviceEvent(
        event_id=uuid4(),
        schema_version=DEVICE_EVENT_SCHEMA_VERSION,
        source_system=DEVICE_EVENT_SOURCE_SYSTEM,
        device_id=device_id,
        session_id=session_id,
        event_type="battery_status",
        event_time=now - timedelta(minutes=5),
        sequence_number=1,
        payload={
            "battery_percent": 91,
            "charging": False,
        },
    )

    arrival_order = [
        event_0,
        event_2,
        event_1_late,
    ]
    event_ids = [
        str(event.event_id)
        for event in arrival_order
    ]

    for event_id in event_ids:
        delete_inbox_event_for_smoke_test(
            event_id
        )

    group_id = (
        "neurosleep-arrival-classification-smoke-"
        f"{uuid4()}"
    )

    try:
        receipts = produce_in_arrival_order(
            bootstrap_servers=bootstrap_servers,
            topic_name=topic.topic_name,
            events=arrival_order,
        )

        partitions = {
            partition
            for partition, _ in receipts.values()
        }

        if len(partitions) != 1:
            raise AssertionError(
                "Arrival fixture must use one partition"
            )

        partition = next(iter(partitions))
        start_offset = start_offsets[partition]

        actual_offsets = [
            receipts[str(event.event_id)][1]
            for event in arrival_order
        ]
        expected_offsets = [
            start_offset,
            start_offset + 1,
            start_offset + 2,
        ]

        if actual_offsets != expected_offsets:
            raise AssertionError(
                "Arrival fixture Kafka offsets "
                "do not match transport order: "
                f"expected={expected_offsets}, "
                f"actual={actual_offsets}"
            )

        def invalid_handler(
            message,
            error,
        ) -> None:
            raise AssertionError(
                "Valid late/out-of-order event "
                f"was treated as invalid: {error}"
            )

        consumer = KafkaDeviceEventConsumer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            auto_offset_reset="earliest",
        )

        processing_result = (
            consumer
            .process_events_resilient_from_offsets(
                start_offsets={
                    partition: start_offset,
                },
                processor=(
                    persist_consumed_device_event
                ),
                invalid_message_handler=(
                    invalid_handler
                ),
                max_messages=3,
                timeout_seconds=20.0,
            )
        )

        if processing_result.valid_events != 3:
            raise AssertionError(
                "Expected three valid events"
            )

        if (
            processing_result.quarantined_messages
            != 0
        ):
            raise AssertionError(
                "Late/out-of-order valid events "
                "must not be quarantined"
            )

        row_0 = get_inbox_event(
            event_0.event_id
        )
        row_2 = get_inbox_event(
            event_2.event_id
        )
        row_1 = get_inbox_event(
            event_1_late.event_id
        )

        if row_0[20] != (
            DEVICE_EVENT_ARRIVAL_CLASSIFICATION_VERSION
        ):
            raise AssertionError(
                "Arrival classification version missing"
            )

        if row_0[22] != (
            DEVICE_EVENT_LATE_THRESHOLD_MS
        ):
            raise AssertionError(
                "Late threshold was not persisted"
            )

        if row_0[23] is not False:
            raise AssertionError(
                "Fresh first event was marked late"
            )

        if row_0[24] is not False:
            raise AssertionError(
                "First event was marked out of order"
            )

        if row_2[23] is not False:
            raise AssertionError(
                "Fresh sequence-2 event was marked late"
            )

        if row_2[24] is not False:
            raise AssertionError(
                "Forward sequence gap alone must not "
                "be classified as out of order"
            )

        if row_2[26] != 0:
            raise AssertionError(
                "Sequence-2 event did not preserve "
                "previous max sequence 0"
            )

        if row_1[21] <= (
            DEVICE_EVENT_LATE_THRESHOLD_MS
        ):
            raise AssertionError(
                "Late fixture did not exceed threshold"
            )

        if row_1[23] is not True:
            raise AssertionError(
                "Late event was not classified as late"
            )

        if row_1[24] is not True:
            raise AssertionError(
                "Backward arrival was not classified "
                "as out of order"
            )

        if row_1[25] != (
            "sequence_and_event_time"
        ):
            raise AssertionError(
                "Unexpected out-of-order reason: "
                f"{row_1[25]}"
            )

        if row_1[26] != 2:
            raise AssertionError(
                "Late event did not preserve "
                "previous max sequence 2"
            )

        if row_1[27] != event_2.event_time:
            raise AssertionError(
                "Late event did not preserve "
                "previous max event_time"
            )

        final_committed = committed_offset(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_name=topic.topic_name,
            partition=partition,
        )

        if final_committed != (
            start_offset + 3
        ):
            raise AssertionError(
                "Arrival-classification flow "
                "did not commit all offsets"
            )

        print(
            "kafka_arrival_fixture_events=3"
        )
        print(
            "kafka_arrival_valid_events=3"
        )
        print(
            "kafka_arrival_quarantined_messages=0"
        )
        print(
            "kafka_arrival_forward_gap_allowed="
            "success"
        )
        print(
            "kafka_arrival_late_detection=success"
        )
        print(
            "kafka_arrival_out_of_order_detection="
            "success"
        )
        print(
            "kafka_arrival_reason="
            "sequence_and_event_time"
        )
        print(
            "kafka_arrival_classification_persisted="
            "success"
        )
        print(
            "kafka_arrival_offset_commit=success"
        )
        print(
            "kafka_arrival_classification_status="
            "success"
        )
    finally:
        for event_id in event_ids:
            delete_inbox_event_for_smoke_test(
                event_id
            )


if __name__ == "__main__":
    main()
