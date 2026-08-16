#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from neuro_sleep.paths import PROJECT_ROOT
from neuro_sleep.streaming.kafka_consumer import (
    KafkaDeviceEventConsumer,
)
from neuro_sleep.streaming.kafka_producer import (
    load_device_event_topic,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read and validate simulated BCI "
            "device events from Kafka."
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
        default=20.0,
    )
    parser.add_argument(
        "--group-id",
        default=(
            "neurosleep-device-event-inspection-v1"
        ),
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

    consumed = consumer.consume_events(
        max_messages=args.max_messages,
        timeout_seconds=args.timeout_seconds,
    )

    event_ids = {
        str(item.event.event_id)
        for item in consumed
    }
    devices = {
        item.event.device_id
        for item in consumed
    }
    partitions = {
        item.partition
        for item in consumed
    }

    first_event_time = min(
        item.event.event_time
        for item in consumed
    )
    last_event_time = max(
        item.event.event_time
        for item in consumed
    )
    first_ingested_at = min(
        item.ingested_at
        for item in consumed
    )
    last_ingested_at = max(
        item.ingested_at
        for item in consumed
    )

    print(
        f"kafka_consumer_topic={topic.topic_name}"
    )
    print(
        "kafka_consumer_bootstrap_servers="
        f"{bootstrap_servers}"
    )
    print(
        f"kafka_consumer_group_id={args.group_id}"
    )
    print(
        f"kafka_consumer_messages={len(consumed)}"
    )
    print(
        "kafka_consumer_unique_event_ids="
        f"{len(event_ids)}"
    )
    print(
        f"kafka_consumer_devices={len(devices)}"
    )
    print(
        "kafka_consumer_partitions_seen="
        f"{','.join(str(value) for value in sorted(partitions))}"
    )
    print(
        "kafka_consumer_first_event_time="
        f"{first_event_time.isoformat()}"
    )
    print(
        "kafka_consumer_last_event_time="
        f"{last_event_time.isoformat()}"
    )
    print(
        "kafka_consumer_first_ingested_at="
        f"{first_ingested_at.isoformat()}"
    )
    print(
        "kafka_consumer_last_ingested_at="
        f"{last_ingested_at.isoformat()}"
    )
    print(
        "kafka_consumer_key_contract=success"
    )
    print(
        "kafka_consumer_header_contract=success"
    )
    print(
        "kafka_consumer_timestamp_contract=success"
    )
    print(
        "kafka_consumer_offset_commit_policy=disabled"
    )
    print("kafka_consumer_status=success")


if __name__ == "__main__":
    main()
