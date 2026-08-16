from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

from confluent_kafka import KafkaError, Message, Producer

from neuro_sleep.paths import PROJECT_ROOT
from neuro_sleep.streaming.device_event import DeviceEvent


TOPIC_CONTRACT_PATH = (
    PROJECT_ROOT
    / "contracts"
    / "kafka"
    / "simulated_bci_device_events_v1.topic.json"
)


@dataclass(frozen=True)
class DeviceEventTopic:
    topic_name: str
    key_field: str


@dataclass(frozen=True)
class DeliveryReceipt:
    event_id: str
    device_id: str
    sequence_number: int
    partition: int
    offset: int


def load_device_event_topic(
    path: Path = TOPIC_CONTRACT_PATH,
) -> DeviceEventTopic:
    raw = json.loads(
        path.read_text(encoding="utf-8")
    )

    topic_name = raw.get("topic_name")
    key_field = raw.get("key_field")

    if not isinstance(topic_name, str) or not topic_name:
        raise ValueError(
            "Kafka topic contract has invalid topic_name"
        )

    if key_field != "device_id":
        raise ValueError(
            "Kafka device event topic must use device_id as key"
        )

    return DeviceEventTopic(
        topic_name=topic_name,
        key_field=key_field,
    )


class KafkaDeviceEventProducer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: DeviceEventTopic,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError(
                "bootstrap_servers cannot be empty"
            )

        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": (
                    "neurosleep-simulated-bci-producer"
                ),
                "enable.idempotence": True,
                "acks": "all",
                "allow.auto.create.topics": False,
                "partitioner": "murmur2_random",
            }
        )

    def produce_events(
        self,
        events: Iterable[DeviceEvent],
        *,
        flush_timeout_seconds: float = 30.0,
    ) -> list[DeliveryReceipt]:
        event_list = list(events)

        if not event_list:
            raise ValueError(
                "At least one DeviceEvent is required"
            )

        receipts: list[DeliveryReceipt] = []
        delivery_errors: list[str] = []

        for event in event_list:
            serialized = json.dumps(
                event.to_dict(),
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")

            def on_delivery(
                error: KafkaError | None,
                message: Message,
                *,
                current_event: DeviceEvent = event,
            ) -> None:
                if error is not None:
                    delivery_errors.append(
                        f"{current_event.event_id}: {error}"
                    )
                    return

                receipts.append(
                    DeliveryReceipt(
                        event_id=str(
                            current_event.event_id
                        ),
                        device_id=(
                            current_event.device_id
                        ),
                        sequence_number=(
                            current_event.sequence_number
                        ),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
                )

            while True:
                try:
                    self._producer.produce(
                        topic=self._topic.topic_name,
                        key=event.device_id.encode("utf-8"),
                        value=serialized,
                        timestamp=int(
                            event.event_time.timestamp()
                            * 1000
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
                    break
                except BufferError:
                    self._producer.poll(0.1)

            self._producer.poll(0)

        undelivered = self._producer.flush(
            flush_timeout_seconds
        )

        if undelivered:
            raise RuntimeError(
                "Kafka producer flush timed out with "
                f"{undelivered} undelivered message(s)"
            )

        if delivery_errors:
            raise RuntimeError(
                "Kafka delivery failed: "
                + "; ".join(delivery_errors)
            )

        if len(receipts) != len(event_list):
            raise RuntimeError(
                "Kafka delivery receipt count mismatch: "
                f"expected={len(event_list)}, "
                f"actual={len(receipts)}"
            )

        self._validate_partition_invariant(
            receipts
        )

        return receipts

    @staticmethod
    def _validate_partition_invariant(
        receipts: list[DeliveryReceipt],
    ) -> None:
        by_device: dict[
            str,
            list[DeliveryReceipt],
        ] = defaultdict(list)

        for receipt in receipts:
            by_device[receipt.device_id].append(
                receipt
            )

        for device_id, device_receipts in (
            by_device.items()
        ):
            partitions = {
                receipt.partition
                for receipt in device_receipts
            }

            if len(partitions) != 1:
                raise RuntimeError(
                    "Kafka partition invariant failed "
                    f"for {device_id}: "
                    f"partitions={sorted(partitions)}"
                )

            ordered = sorted(
                device_receipts,
                key=lambda receipt: (
                    receipt.sequence_number
                ),
            )
            offsets = [
                receipt.offset
                for receipt in ordered
            ]

            if any(
                current >= following
                for current, following
                in zip(offsets, offsets[1:])
            ):
                raise RuntimeError(
                    "Kafka offset order failed "
                    f"for {device_id}: "
                    f"offsets={offsets}"
                )
