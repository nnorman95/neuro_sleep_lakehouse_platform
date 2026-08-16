#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

from confluent_kafka import (
    Consumer,
    TopicPartition,
)
from dotenv import load_dotenv

from neuro_sleep.paths import PROJECT_ROOT
from neuro_sleep.streaming.device_event_inbox import (
    delete_inbox_event_for_smoke_test,
    get_inbox_event,
    persist_consumed_device_event,
)
from neuro_sleep.streaming.kafka_consumer import (
    ConsumedDeviceEvent,
    KafkaDeviceEventConsumer,
    get_topic_end_offsets,
)
from neuro_sleep.streaming.kafka_producer import (
    KafkaDeviceEventProducer,
    load_device_event_topic,
)
from neuro_sleep.streaming.simulated_bci import (
    generate_simulated_device_session,
)


class IntentionalProcessingFailure(RuntimeError):
    pass


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
            [
                TopicPartition(
                    topic_name,
                    partition,
                )
            ],
            timeout=10.0,
        )

        if len(result) != 1:
            raise AssertionError(
                "Unexpected committed offset result"
            )

        item = result[0]

        if item.error is not None:
            raise RuntimeError(
                "Kafka committed offset lookup failed: "
                f"{item.error}"
            )

        return item.offset
    finally:
        consumer.close()


def inbox_exists(
    event_id: str,
) -> bool:
    try:
        get_inbox_event(event_id)
    except ValueError:
        return False

    return True


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

    events = generate_simulated_device_session(
        device_id=(
            "bci-device-durable-ingestion-smoke"
        ),
        signal_quality_events=1,
        seed=41,
        start_time=datetime.now(timezone.utc),
    )

    event_ids = [
        str(event.event_id)
        for event in events
    ]

    for event_id in event_ids:
        delete_inbox_event_for_smoke_test(
            event_id
        )

    group_id = (
        "neurosleep-device-event-ingestion-smoke-"
        f"{uuid4()}"
    )

    try:
        producer = KafkaDeviceEventProducer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )
        receipts = producer.produce_events(
            events
        )

        partitions = {
            receipt.partition
            for receipt in receipts
        }

        if len(partitions) != 1:
            raise AssertionError(
                "Smoke fixture must use one partition"
            )

        partition = next(iter(partitions))
        start_offset = start_offsets[partition]

        ordered_receipts = sorted(
            receipts,
            key=lambda item: item.sequence_number,
        )
        actual_offsets = [
            receipt.offset
            for receipt in ordered_receipts
        ]
        expected_offsets = [
            start_offset,
            start_offset + 1,
            start_offset + 2,
        ]

        if actual_offsets != expected_offsets:
            raise AssertionError(
                "Unexpected Kafka fixture offsets: "
                f"expected={expected_offsets}, "
                f"actual={actual_offsets}"
            )

        first_consumer = KafkaDeviceEventConsumer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            auto_offset_reset="earliest",
        )

        first_processed = (
            first_consumer.process_events_from_offsets(
                start_offsets={
                    partition: start_offset,
                },
                processor=(
                    persist_consumed_device_event
                ),
                max_messages=2,
                timeout_seconds=20.0,
            )
        )

        if len(first_processed) != 2:
            raise AssertionError(
                "First ingestion pass did not "
                "process two events"
            )

        committed_after_two = committed_offset(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_name=topic.topic_name,
            partition=partition,
        )

        if committed_after_two != (
            start_offset + 2
        ):
            raise AssertionError(
                "Kafka offset was not committed "
                "after durable writes: "
                f"expected={start_offset + 2}, "
                f"actual={committed_after_two}"
            )

        if not all(
            inbox_exists(event_id)
            for event_id in event_ids[:2]
        ):
            raise AssertionError(
                "Durably processed events are "
                "missing from inbox"
            )

        if inbox_exists(event_ids[2]):
            raise AssertionError(
                "Third event was persisted too early"
            )

        failing_consumer = KafkaDeviceEventConsumer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            auto_offset_reset="earliest",
        )

        failure_seen = False

        def fail_before_persist(
            consumed: ConsumedDeviceEvent,
        ) -> None:
            raise IntentionalProcessingFailure(
                str(consumed.event.event_id)
            )

        try:
            failing_consumer.process_events_from_offsets(
                start_offsets={
                    partition: committed_after_two,
                },
                processor=fail_before_persist,
                max_messages=1,
                timeout_seconds=20.0,
            )
        except IntentionalProcessingFailure:
            failure_seen = True

        if not failure_seen:
            raise AssertionError(
                "Intentional pre-persist failure "
                "was not observed"
            )

        committed_after_failure = committed_offset(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_name=topic.topic_name,
            partition=partition,
        )

        if (
            committed_after_failure
            != committed_after_two
        ):
            raise AssertionError(
                "Kafka offset advanced despite "
                "failed durable processing"
            )

        if inbox_exists(event_ids[2]):
            raise AssertionError(
                "Failed event unexpectedly exists "
                "in durable inbox"
            )

        restart_consumer = KafkaDeviceEventConsumer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            auto_offset_reset="earliest",
        )

        restart_processed = (
            restart_consumer.process_events_from_offsets(
                start_offsets={
                    partition: (
                        committed_after_failure
                    ),
                },
                processor=(
                    persist_consumed_device_event
                ),
                max_messages=1,
                timeout_seconds=20.0,
            )
        )

        if len(restart_processed) != 1:
            raise AssertionError(
                "Restart pass did not process "
                "the remaining event"
            )

        committed_final = committed_offset(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_name=topic.topic_name,
            partition=partition,
        )

        if committed_final != (
            start_offset + 3
        ):
            raise AssertionError(
                "Final Kafka committed offset "
                "is incorrect"
            )

        if not all(
            inbox_exists(event_id)
            for event_id in event_ids
        ):
            raise AssertionError(
                "Not all fixture events reached "
                "the durable inbox"
            )

        print(
            "kafka_ingestion_fixture_events=3"
        )
        print(
            "kafka_ingestion_first_pass_processed=2"
        )
        print(
            "kafka_ingestion_commit_after_durable_write="
            "success"
        )
        print(
            "kafka_ingestion_failure_before_persist="
            "observed"
        )
        print(
            "kafka_ingestion_offset_unchanged_on_failure="
            "success"
        )
        print(
            "kafka_ingestion_restart_processed=1"
        )
        print(
            "kafka_ingestion_restart_resume="
            "success"
        )
        print(
            "kafka_ingestion_all_events_durable="
            "success"
        )
        print(
            "kafka_ingestion_at_least_once="
            "success"
        )
        print(
            "kafka_ingestion_smoke_status=success"
        )
    finally:
        for event_id in event_ids:
            delete_inbox_event_for_smoke_test(
                event_id
            )


if __name__ == "__main__":
    main()
