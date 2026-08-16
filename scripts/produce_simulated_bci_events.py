#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import os

from dotenv import load_dotenv

from neuro_sleep.paths import PROJECT_ROOT
from neuro_sleep.streaming.kafka_producer import (
    KafkaDeviceEventProducer,
    load_device_event_topic,
)
from neuro_sleep.streaming.simulated_bci import (
    generate_simulated_device_session,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Produce simulated BCI device events "
            "to the Phase 11 Kafka topic."
        )
    )
    parser.add_argument(
        "--devices",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--signal-quality-events",
        type=int,
        default=4,
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=11,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.devices <= 0:
        raise ValueError(
            "--devices must be a positive integer"
        )

    if args.signal_quality_events <= 0:
        raise ValueError(
            "--signal-quality-events must be positive"
        )

    load_dotenv(
        PROJECT_ROOT / ".env",
        override=False,
    )

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    ).strip()

    topic = load_device_event_topic()
    producer = KafkaDeviceEventProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )

    batch_start = datetime.now(timezone.utc)
    all_events = []

    for index in range(args.devices):
        device_id = f"bci-device-{index + 1:03d}"

        events = generate_simulated_device_session(
            device_id=device_id,
            signal_quality_events=(
                args.signal_quality_events
            ),
            seed=args.seed + index,
            start_time=(
                batch_start
                + timedelta(milliseconds=index)
            ),
        )
        all_events.extend(events)

    receipts = producer.produce_events(
        all_events
    )

    partitions_by_device: dict[
        str,
        set[int],
    ] = {}

    for receipt in receipts:
        partitions_by_device.setdefault(
            receipt.device_id,
            set(),
        ).add(receipt.partition)

    print(
        f"kafka_producer_topic={topic.topic_name}"
    )
    print(
        "kafka_producer_bootstrap_servers="
        f"{bootstrap_servers}"
    )
    print(
        f"simulated_bci_devices={args.devices}"
    )
    print(
        f"simulated_bci_events={len(all_events)}"
    )
    print(
        "kafka_producer_delivery_success="
        f"{len(receipts)}"
    )

    for device_id in sorted(
        partitions_by_device
    ):
        partitions = sorted(
            partitions_by_device[device_id]
        )
        print(
            "kafka_producer_device_partition="
            f"{device_id}:{partitions[0]}"
        )

    print(
        "kafka_producer_device_partition_invariant="
        "success"
    )
    print("kafka_producer_status=success")


if __name__ == "__main__":
    main()
