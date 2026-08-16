#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

from dotenv import load_dotenv

from neuro_sleep.paths import PROJECT_ROOT
from neuro_sleep.streaming.kafka_consumer import (
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
        device_id="bci-device-consumer-smoke",
        signal_quality_events=1,
        seed=31,
        start_time=datetime.now(timezone.utc),
    )

    producer = KafkaDeviceEventProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )
    receipts = producer.produce_events(events)

    consumer = KafkaDeviceEventConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=(
            "neurosleep-device-event-consumer-fixture-"
            f"{uuid4()}"
        ),
        auto_offset_reset="earliest",
    )

    consumed = consumer.consume_events_from_offsets(
        start_offsets=start_offsets,
        max_messages=len(events),
        timeout_seconds=20.0,
    )

    produced_event_ids = {
        str(event.event_id)
        for event in events
    }
    consumed_event_ids = {
        str(item.event.event_id)
        for item in consumed
    }

    if produced_event_ids != consumed_event_ids:
        raise AssertionError(
            "Consumer fixture did not read exactly "
            "the newly produced events"
        )

    if len(receipts) != len(events):
        raise AssertionError(
            "Producer receipt count mismatch"
        )

    offset_text = ",".join(
        f"{partition}:{offset}"
        for partition, offset
        in sorted(start_offsets.items())
    )

    print(
        "kafka_consumer_fixture_start_offsets="
        f"{offset_text}"
    )
    print(
        "kafka_consumer_fixture_produced="
        f"{len(events)}"
    )
    print(
        "kafka_consumer_fixture_consumed="
        f"{len(consumed)}"
    )
    print(
        "kafka_consumer_fixture_exact_event_ids="
        "success"
    )
    print(
        "kafka_consumer_fixture_offset_isolation="
        "success"
    )
    print(
        "kafka_consumer_fixture_status=success"
    )


if __name__ == "__main__":
    main()
