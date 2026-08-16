#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
import os

from dotenv import load_dotenv

from neuro_sleep.paths import PROJECT_ROOT
from neuro_sleep.streaming.device_event_inbox import (
    InboxWriteResult,
    persist_consumed_device_event,
)
from neuro_sleep.streaming.kafka_consumer import (
    ConsumedDeviceEvent,
    KafkaDeviceEventConsumer,
)
from neuro_sleep.streaming.kafka_producer import (
    load_device_event_topic,
)
from neuro_sleep.streaming.kafka_quarantine import (
    quarantine_invalid_device_event_message,
)


DEFAULT_GROUP_ID = "neurosleep-device-event-inbox-v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consume validated Kafka device events, "
            "persist them durably, then commit offsets."
        )
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=10,
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
    )
    parser.add_argument(
        "--offset-reset",
        choices=("earliest", "latest"),
        default="earliest",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    load_dotenv(
        PROJECT_ROOT / ".env",
        override=False,
    )

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    ).strip()

    topic = load_device_event_topic()

    consumer = KafkaDeviceEventConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=args.group_id,
        auto_offset_reset=args.offset_reset,
    )

    write_results: list[InboxWriteResult] = []

    def persist(
        consumed: ConsumedDeviceEvent,
    ) -> None:
        write_results.append(
            persist_consumed_device_event(
                consumed
            )
        )

    processing_result = (
        consumer.process_events_resilient(
            processor=persist,
            invalid_message_handler=(
                quarantine_invalid_device_event_message
            ),
            max_messages=args.max_messages,
            timeout_seconds=args.timeout_seconds,
        )
    )

    counts = Counter(
        result.status
        for result in write_results
    )

    print(
        f"kafka_ingestion_topic={topic.topic_name}"
    )
    print(
        f"kafka_ingestion_group_id={args.group_id}"
    )
    print(
        "kafka_ingestion_messages_processed="
        f"{processing_result.messages_handled}"
    )
    print(
        "kafka_ingestion_inbox_inserted="
        f"{counts.get('inserted', 0)}"
    )
    print(
        "kafka_ingestion_inbox_duplicates="
        f"{counts.get('duplicate', 0)}"
    )
    print(
        "kafka_ingestion_quarantined_messages="
        f"{processing_result.quarantined_messages}"
    )
    print(
        "kafka_ingestion_offset_commit_policy="
        "after_durable_write"
    )
    print(
        "kafka_ingestion_delivery_guarantee="
        "at_least_once"
    )
    print("kafka_ingestion_status=success")


if __name__ == "__main__":
    main()
