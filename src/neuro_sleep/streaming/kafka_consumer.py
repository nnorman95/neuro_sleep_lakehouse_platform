from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import time

from confluent_kafka import (
    Consumer,
    KafkaException,
    Message,
    TopicPartition,
)

from neuro_sleep.streaming.device_event import (
    DeviceEvent,
)
from neuro_sleep.streaming.kafka_producer import (
    DeviceEventTopic,
)


@dataclass(frozen=True)
class ConsumedDeviceEvent:
    event: DeviceEvent
    topic: str
    partition: int
    offset: int
    kafka_timestamp_ms: int
    ingested_at: datetime
    key: str
    headers: tuple[tuple[str, str | None], ...]


def _decode_text(
    value: str | bytes | None,
    *,
    field_name: str,
) -> str:
    if value is None:
        raise ValueError(
            f"Kafka {field_name} cannot be null"
        )

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"Kafka {field_name} is not valid UTF-8"
            ) from exc

    if isinstance(value, str):
        return value

    raise TypeError(
        f"Kafka {field_name} must be str or bytes"
    )


def _decode_headers(
    message: Message,
) -> tuple[tuple[str, str | None], ...]:
    raw_headers = message.headers()

    if raw_headers is None:
        raise ValueError(
            "Kafka device event headers are required"
        )

    decoded: list[tuple[str, str | None]] = []

    for key, value in raw_headers:
        decoded_value: str | None

        if value is None:
            decoded_value = None
        elif isinstance(value, bytes):
            try:
                decoded_value = value.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError(
                    "Kafka header value is not valid UTF-8: "
                    f"{key}"
                ) from exc
        elif isinstance(value, str):
            decoded_value = value
        else:
            raise TypeError(
                "Kafka header value must be "
                "str, bytes, or None"
            )

        decoded.append(
            (
                str(key),
                decoded_value,
            )
        )

    return tuple(decoded)


def _header_map(
    headers: tuple[tuple[str, str | None], ...],
) -> dict[str, str | None]:
    values: dict[str, str | None] = {}

    for key, value in headers:
        if key in values:
            raise ValueError(
                "Duplicate Kafka header is not allowed "
                f"by the device event contract: {key}"
            )

        values[key] = value

    return values


def decode_device_event_message(
    message: Message,
    *,
    expected_topic: DeviceEventTopic,
) -> ConsumedDeviceEvent:
    error = message.error()

    if error is not None:
        raise KafkaException(error)

    topic = message.topic()

    if topic != expected_topic.topic_name:
        raise ValueError(
            "Unexpected Kafka topic: "
            f"expected={expected_topic.topic_name}, "
            f"actual={topic}"
        )

    partition = message.partition()
    offset = message.offset()

    if partition < 0:
        raise ValueError(
            "Kafka partition cannot be negative"
        )

    if offset < 0:
        raise ValueError(
            "Kafka offset cannot be negative"
        )

    key = _decode_text(
        message.key(),
        field_name="message key",
    )
    value_text = _decode_text(
        message.value(),
        field_name="message value",
    )

    try:
        raw_event = json.loads(value_text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Kafka message value is not valid JSON"
        ) from exc

    if not isinstance(raw_event, dict):
        raise TypeError(
            "Kafka device event JSON must be an object"
        )

    event = DeviceEvent.from_dict(raw_event)

    if key != event.device_id:
        raise ValueError(
            "Kafka key/device_id mismatch: "
            f"key={key}, "
            f"device_id={event.device_id}"
        )

    headers = _decode_headers(message)
    headers_by_name = _header_map(headers)

    expected_headers = {
        "schema_version": event.schema_version,
        "event_type": event.event_type,
    }

    for header_name, expected_value in (
        expected_headers.items()
    ):
        actual_value = headers_by_name.get(
            header_name
        )

        if actual_value != expected_value:
            raise ValueError(
                "Kafka header contract mismatch: "
                f"{header_name} "
                f"expected={expected_value}, "
                f"actual={actual_value}"
            )

    timestamp_type, kafka_timestamp_ms = (
        message.timestamp()
    )

    if kafka_timestamp_ms is None:
        raise ValueError(
            "Kafka message timestamp is unavailable"
        )

    expected_timestamp_ms = int(
        event.event_time.timestamp() * 1000
    )

    if kafka_timestamp_ms != expected_timestamp_ms:
        raise ValueError(
            "Kafka timestamp/event_time mismatch: "
            f"expected={expected_timestamp_ms}, "
            f"actual={kafka_timestamp_ms}, "
            f"timestamp_type={timestamp_type}"
        )

    ingested_at = datetime.now(timezone.utc)

    return ConsumedDeviceEvent(
        event=event,
        topic=topic,
        partition=partition,
        offset=offset,
        kafka_timestamp_ms=kafka_timestamp_ms,
        ingested_at=ingested_at,
        key=key,
        headers=headers,
    )


def get_topic_end_offsets(
    *,
    bootstrap_servers: str,
    topic: DeviceEventTopic,
) -> dict[int, int]:
    if not bootstrap_servers.strip():
        raise ValueError(
            "bootstrap_servers cannot be empty"
        )

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": (
                "neurosleep-device-event-offset-snapshot"
            ),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )

    try:
        metadata = consumer.list_topics(
            topic.topic_name,
            timeout=10.0,
        )

        topic_metadata = metadata.topics.get(
            topic.topic_name
        )

        if topic_metadata is None:
            raise RuntimeError(
                "Kafka topic metadata is unavailable: "
                f"{topic.topic_name}"
            )

        if topic_metadata.error is not None:
            raise KafkaException(
                topic_metadata.error
            )

        end_offsets: dict[int, int] = {}

        for partition_id in sorted(
            topic_metadata.partitions
        ):
            _, high = consumer.get_watermark_offsets(
                TopicPartition(
                    topic.topic_name,
                    partition_id,
                ),
                timeout=10.0,
                cached=False,
            )
            end_offsets[partition_id] = high

        if not end_offsets:
            raise RuntimeError(
                "Kafka topic has no partitions"
            )

        return end_offsets
    finally:
        consumer.close()


class KafkaDeviceEventConsumer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: DeviceEventTopic,
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError(
                "bootstrap_servers cannot be empty"
            )

        if not group_id.strip():
            raise ValueError(
                "group_id cannot be empty"
            )

        if auto_offset_reset not in {
            "earliest",
            "latest",
        }:
            raise ValueError(
                "auto_offset_reset must be "
                "earliest or latest"
            )

        self._topic = topic
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "client.id": (
                    "neurosleep-device-event-consumer"
                ),
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": auto_offset_reset,
                "allow.auto.create.topics": False,
            }
        )

    def _poll_decoded(
        self,
        *,
        max_messages: int,
        timeout_seconds: float,
    ) -> list[ConsumedDeviceEvent]:
        consumed: list[ConsumedDeviceEvent] = []
        deadline = time.monotonic() + timeout_seconds

        while len(consumed) < max_messages:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                break

            message = self._consumer.poll(
                min(1.0, remaining)
            )

            if message is None:
                continue

            consumed.append(
                decode_device_event_message(
                    message,
                    expected_topic=self._topic,
                )
            )

        if len(consumed) != max_messages:
            raise RuntimeError(
                "Kafka consumer timed out before "
                "receiving the requested messages: "
                f"expected={max_messages}, "
                f"actual={len(consumed)}"
            )

        return consumed

    def consume_events_from_offsets(
        self,
        *,
        start_offsets: dict[int, int],
        max_messages: int,
        timeout_seconds: float,
    ) -> list[ConsumedDeviceEvent]:
        if not start_offsets:
            raise ValueError(
                "start_offsets cannot be empty"
            )

        for partition, offset in (
            start_offsets.items()
        ):
            if partition < 0:
                raise ValueError(
                    "Kafka partition cannot be negative"
                )

            if offset < 0:
                raise ValueError(
                    "Kafka start offset cannot be negative"
                )

        if max_messages <= 0:
            raise ValueError(
                "max_messages must be positive"
            )

        if timeout_seconds <= 0:
            raise ValueError(
                "timeout_seconds must be positive"
            )

        assignments = [
            TopicPartition(
                self._topic.topic_name,
                partition,
                offset,
            )
            for partition, offset
            in sorted(start_offsets.items())
        ]

        self._consumer.assign(assignments)

        try:
            return self._poll_decoded(
                max_messages=max_messages,
                timeout_seconds=timeout_seconds,
            )
        finally:
            self._consumer.close()

    def consume_events(
        self,
        *,
        max_messages: int,
        timeout_seconds: float,
    ) -> list[ConsumedDeviceEvent]:
        if max_messages <= 0:
            raise ValueError(
                "max_messages must be positive"
            )

        if timeout_seconds <= 0:
            raise ValueError(
                "timeout_seconds must be positive"
            )

        self._consumer.subscribe(
            [self._topic.topic_name]
        )

        try:
            return self._poll_decoded(
                max_messages=max_messages,
                timeout_seconds=timeout_seconds,
            )
        finally:
            self._consumer.close()
