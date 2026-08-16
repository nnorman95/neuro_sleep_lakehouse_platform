from __future__ import annotations

import base64
import hashlib
import json
from typing import Any
from uuid import UUID

from confluent_kafka import Message

from neuro_sleep.quality.quarantine import (
    upsert_active_quarantine_record,
)
from neuro_sleep.streaming.device_event import (
    DEVICE_EVENT_SOURCE_SYSTEM,
)


INVALID_DEVICE_EVENT_ERROR_CODE = (
    "KAFKA_DEVICE_EVENT_CONTRACT_INVALID"
)
MAX_QUARANTINE_PREVIEW_BYTES = 64 * 1024


def kafka_message_record_key(
    message: Message,
) -> str:
    return (
        f"kafka://{message.topic()}/"
        f"{message.partition()}/"
        f"{message.offset()}"
    )


def _bytes_preview(
    value: bytes | None,
) -> dict[str, Any] | None:
    if value is None:
        return None

    preview = value[
        :MAX_QUARANTINE_PREVIEW_BYTES
    ]

    result: dict[str, Any] = {
        "size_bytes": len(value),
        "sha256": hashlib.sha256(
            value
        ).hexdigest(),
        "truncated": len(preview) != len(value),
        "base64_preview": base64.b64encode(
            preview
        ).decode("ascii"),
    }

    try:
        text = preview.decode("utf-8")
    except UnicodeDecodeError:
        return result

    result["utf8_preview"] = text

    try:
        result["json_preview"] = json.loads(
            text
        )
    except json.JSONDecodeError:
        pass

    return result


def _headers_payload(
    message: Message,
) -> list[dict[str, Any]]:
    raw_headers = message.headers()

    if raw_headers is None:
        return []

    result: list[dict[str, Any]] = []

    for key, value in raw_headers:
        encoded_value = (
            value.encode("utf-8")
            if isinstance(value, str)
            else value
        )

        result.append(
            {
                "key": str(key),
                "value": _bytes_preview(
                    encoded_value
                ),
            }
        )

    return result


def raw_kafka_message_payload(
    message: Message,
) -> dict[str, Any]:
    timestamp_type, timestamp_ms = (
        message.timestamp()
    )

    key = message.key()
    value = message.value()

    if isinstance(key, str):
        key = key.encode("utf-8")

    if isinstance(value, str):
        value = value.encode("utf-8")

    return {
        "transport": "kafka",
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "timestamp_type": timestamp_type,
        "timestamp_ms": timestamp_ms,
        "key": _bytes_preview(key),
        "value": _bytes_preview(value),
        "headers": _headers_payload(message),
    }


def quarantine_invalid_device_event_message(
    message: Message,
    error: Exception,
) -> UUID:
    record_key = kafka_message_record_key(
        message
    )

    return upsert_active_quarantine_record(
        source_system=DEVICE_EVENT_SOURCE_SYSTEM,
        record_key=record_key,
        error_code=(
            INVALID_DEVICE_EVENT_ERROR_CODE
        ),
        error_message=(
            f"{type(error).__name__}: {error}"
        ),
        severity="error",
        raw_payload=(
            raw_kafka_message_payload(message)
        ),
    )
