#!/usr/bin/env python3
from __future__ import annotations

import os
from uuid import uuid4

from confluent_kafka import (
    Consumer,
    Producer,
    TopicPartition,
)
from dotenv import load_dotenv

from neuro_sleep.paths import PROJECT_ROOT
from neuro_sleep.quality.quarantine import (
    delete_quarantine_record_for_smoke_test,
    get_quarantine_record,
)
from neuro_sleep.streaming.kafka_consumer import (
    KafkaDeviceEventConsumer,
    get_topic_end_offsets,
)
from neuro_sleep.streaming.kafka_producer import (
    load_device_event_topic,
)
from neuro_sleep.streaming.kafka_quarantine import (
    INVALID_DEVICE_EVENT_ERROR_CODE,
    kafka_message_record_key,
    quarantine_invalid_device_event_message,
)


class IntentionalQuarantineFailure(
    RuntimeError
):
    pass


def get_committed_offset(
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
                "Unexpected committed-offset result"
            )

        item = result[0]

        if item.error is not None:
            raise RuntimeError(
                "Committed-offset lookup failed: "
                f"{item.error}"
            )

        return item.offset
    finally:
        consumer.close()


def produce_invalid_message(
    *,
    bootstrap_servers: str,
    topic_name: str,
) -> tuple[int, int]:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": (
                "neurosleep-invalid-message-smoke"
            ),
            "enable.idempotence": True,
            "acks": "all",
            "allow.auto.create.topics": False,
        }
    )

    delivered: list[tuple[int, int]] = []
    errors: list[str] = []

    def on_delivery(
        error,
        message,
    ) -> None:
        if error is not None:
            errors.append(str(error))
            return

        delivered.append(
            (
                message.partition(),
                message.offset(),
            )
        )

    producer.produce(
        topic=topic_name,
        key=b"bci-device-invalid-smoke",
        value=b'{"event_id":"not-a-valid-uuid"}',
        headers=[
            ("schema_version", "1.0.0"),
            ("event_type", "signal_quality"),
        ],
        on_delivery=on_delivery,
    )

    undelivered = producer.flush(20.0)

    if undelivered:
        raise RuntimeError(
            "Invalid-message fixture was "
            "not fully delivered"
        )

    if errors:
        raise RuntimeError(
            "Invalid-message fixture delivery failed: "
            + "; ".join(errors)
        )

    if len(delivered) != 1:
        raise AssertionError(
            "Expected exactly one delivered fixture"
        )

    return delivered[0]


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

    partition, offset = produce_invalid_message(
        bootstrap_servers=bootstrap_servers,
        topic_name=topic.topic_name,
    )

    if offset != start_offsets[partition]:
        raise AssertionError(
            "Invalid fixture offset does not match "
            "the pre-produce high watermark"
        )

    record_key = (
        f"kafka://{topic.topic_name}/"
        f"{partition}/{offset}"
    )

    delete_quarantine_record_for_smoke_test(
        source_system="simulated_bci_device",
        record_key=record_key,
        error_code=(
            INVALID_DEVICE_EVENT_ERROR_CODE
        ),
    )

    failing_group = (
        "neurosleep-invalid-quarantine-fail-"
        f"{uuid4()}"
    )

    failing_consumer = KafkaDeviceEventConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=failing_group,
        auto_offset_reset="earliest",
    )

    failure_seen = False

    def fail_quarantine(
        message,
        error,
    ) -> None:
        raise IntentionalQuarantineFailure(
            str(error)
        )

    try:
        failing_consumer.process_events_resilient_from_offsets(
            start_offsets={
                partition: offset,
            },
            processor=lambda consumed: None,
            invalid_message_handler=fail_quarantine,
            max_messages=1,
            timeout_seconds=20.0,
        )
    except IntentionalQuarantineFailure:
        failure_seen = True

    if not failure_seen:
        raise AssertionError(
            "Intentional quarantine failure "
            "was not observed"
        )

    failed_commit = get_committed_offset(
        bootstrap_servers=bootstrap_servers,
        group_id=failing_group,
        topic_name=topic.topic_name,
        partition=partition,
    )

    if failed_commit >= 0:
        raise AssertionError(
            "Kafka offset advanced despite "
            "failed quarantine persistence"
        )

    success_group = (
        "neurosleep-invalid-quarantine-ok-"
        f"{uuid4()}"
    )
    quarantine_ids = []

    def persist_quarantine(
        message,
        error,
    ) -> None:
        quarantine_ids.append(
            quarantine_invalid_device_event_message(
                message,
                error,
            )
        )

    success_consumer = KafkaDeviceEventConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=success_group,
        auto_offset_reset="earliest",
    )

    result = (
        success_consumer
        .process_events_resilient_from_offsets(
            start_offsets={
                partition: offset,
            },
            processor=lambda consumed: None,
            invalid_message_handler=(
                persist_quarantine
            ),
            max_messages=1,
            timeout_seconds=20.0,
        )
    )

    if result.messages_handled != 1:
        raise AssertionError(
            "Expected one handled Kafka message"
        )

    if result.valid_events != 0:
        raise AssertionError(
            "Invalid fixture was treated as valid"
        )

    if result.quarantined_messages != 1:
        raise AssertionError(
            "Invalid fixture was not quarantined"
        )

    if len(quarantine_ids) != 1:
        raise AssertionError(
            "Expected one quarantine id"
        )

    quarantine_id = quarantine_ids[0]
    row = get_quarantine_record(
        quarantine_id
    )

    if row[1] != "simulated_bci_device":
        raise AssertionError(
            "Unexpected quarantine source_system"
        )

    if row[3] != record_key:
        raise AssertionError(
            "Unexpected quarantine record_key"
        )

    if row[5] != (
        INVALID_DEVICE_EVENT_ERROR_CODE
    ):
        raise AssertionError(
            "Unexpected quarantine error_code"
        )

    raw_payload = row[4]

    if raw_payload["topic"] != topic.topic_name:
        raise AssertionError(
            "Quarantine payload lost topic"
        )

    if raw_payload["partition"] != partition:
        raise AssertionError(
            "Quarantine payload lost partition"
        )

    if raw_payload["offset"] != offset:
        raise AssertionError(
            "Quarantine payload lost offset"
        )

    if "base64_preview" not in raw_payload["value"]:
        raise AssertionError(
            "Quarantine payload lost raw value"
        )

    committed = get_committed_offset(
        bootstrap_servers=bootstrap_servers,
        group_id=success_group,
        topic_name=topic.topic_name,
        partition=partition,
    )

    if committed != offset + 1:
        raise AssertionError(
            "Kafka offset was not committed "
            "after durable quarantine"
        )

    second_group = (
        "neurosleep-invalid-quarantine-repeat-"
        f"{uuid4()}"
    )
    repeated_ids = []

    repeat_consumer = KafkaDeviceEventConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=second_group,
        auto_offset_reset="earliest",
    )

    repeat_consumer.process_events_resilient_from_offsets(
        start_offsets={
            partition: offset,
        },
        processor=lambda consumed: None,
        invalid_message_handler=(
            lambda message, error:
            repeated_ids.append(
                quarantine_invalid_device_event_message(
                    message,
                    error,
                )
            )
        ),
        max_messages=1,
        timeout_seconds=20.0,
    )

    if repeated_ids != [quarantine_id]:
        raise AssertionError(
            "Repeated invalid delivery created "
            "a second active quarantine record"
        )

    print(
        "kafka_invalid_fixture_messages=1"
    )
    print(
        "kafka_invalid_quarantine_failure="
        "observed"
    )
    print(
        "kafka_invalid_offset_unchanged_on_"
        "quarantine_failure=success"
    )
    print(
        "kafka_invalid_message_quarantined="
        "success"
    )
    print(
        "kafka_invalid_raw_payload_preserved="
        "success"
    )
    print(
        "kafka_invalid_offset_commit_after_"
        "quarantine=success"
    )
    print(
        "kafka_invalid_quarantine_upsert_"
        "idempotency=success"
    )
    print(
        "kafka_invalid_message_flow_status="
        "success"
    )

    delete_quarantine_record_for_smoke_test(
        source_system="simulated_bci_device",
        record_key=record_key,
        error_code=(
            INVALID_DEVICE_EVENT_ERROR_CODE
        ),
    )


if __name__ == "__main__":
    main()
