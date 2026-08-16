from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from uuid import UUID

from psycopg.types.json import Jsonb

from neuro_sleep.db.postgres import (
    get_postgres_connection,
)
from neuro_sleep.streaming.kafka_consumer import (
    ConsumedDeviceEvent,
)


class DeviceEventIdentityConflict(RuntimeError):
    """One event_id was reused with different event content."""


@dataclass(frozen=True)
class InboxWriteResult:
    event_id: UUID
    status: str
    delivery_count: int


def device_event_fingerprint(
    consumed: ConsumedDeviceEvent,
) -> str:
    canonical_event = json.dumps(
        consumed.event.to_dict(),
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

    return hashlib.sha256(
        canonical_event
    ).hexdigest()


def _headers_to_json(
    consumed: ConsumedDeviceEvent,
) -> dict[str, str | None]:
    headers: dict[str, str | None] = {}

    for key, value in consumed.headers:
        if key in headers:
            raise ValueError(
                "Duplicate Kafka header cannot be "
                f"persisted in inbox: {key}"
            )

        headers[key] = value

    return headers


def persist_consumed_device_event(
    consumed: ConsumedDeviceEvent,
) -> InboxWriteResult:
    event = consumed.event
    fingerprint = device_event_fingerprint(
        consumed
    )

    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into ops.kafka_device_event_inbox (
                    event_id,
                    source_system,
                    schema_version,
                    device_id,
                    session_id,
                    event_type,
                    event_time,
                    sequence_number,
                    raw_event,
                    event_fingerprint_sha256,
                    kafka_topic,
                    first_kafka_partition,
                    first_kafka_offset,
                    last_kafka_partition,
                    last_kafka_offset,
                    kafka_timestamp_ms,
                    kafka_headers,
                    first_ingested_at,
                    last_ingested_at,
                    delivery_count
                )
                values (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, 1
                )
                on conflict (event_id)
                do update set
                    last_kafka_partition =
                        excluded.last_kafka_partition,
                    last_kafka_offset =
                        excluded.last_kafka_offset,
                    last_ingested_at =
                        excluded.last_ingested_at,
                    delivery_count =
                        ops.kafka_device_event_inbox.delivery_count
                        + 1,
                    updated_at = now()
                where
                    ops.kafka_device_event_inbox
                        .event_fingerprint_sha256
                    =
                    excluded.event_fingerprint_sha256
                returning
                    event_id,
                    delivery_count;
                """,
                (
                    event.event_id,
                    event.source_system,
                    event.schema_version,
                    event.device_id,
                    event.session_id,
                    event.event_type,
                    event.event_time,
                    event.sequence_number,
                    Jsonb(event.to_dict()),
                    fingerprint,
                    consumed.topic,
                    consumed.partition,
                    consumed.offset,
                    consumed.partition,
                    consumed.offset,
                    consumed.kafka_timestamp_ms,
                    Jsonb(_headers_to_json(consumed)),
                    consumed.ingested_at,
                    consumed.ingested_at,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise DeviceEventIdentityConflict(
                    "event_id was already persisted with "
                    "different event content: "
                    f"{event.event_id}"
                )

            event_id, delivery_count = row

            return InboxWriteResult(
                event_id=event_id,
                status=(
                    "inserted"
                    if delivery_count == 1
                    else "duplicate"
                ),
                delivery_count=delivery_count,
            )


def get_inbox_event(
    event_id: UUID | str,
) -> tuple:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    event_id,
                    source_system,
                    schema_version,
                    device_id,
                    session_id,
                    event_type,
                    event_time,
                    sequence_number,
                    raw_event,
                    event_fingerprint_sha256,
                    kafka_topic,
                    first_kafka_partition,
                    first_kafka_offset,
                    last_kafka_partition,
                    last_kafka_offset,
                    kafka_timestamp_ms,
                    kafka_headers,
                    first_ingested_at,
                    last_ingested_at,
                    delivery_count
                from ops.kafka_device_event_inbox
                where event_id = %s;
                """,
                (event_id,),
            )

            row = cursor.fetchone()

            if row is None:
                raise ValueError(
                    "Kafka device event inbox row "
                    f"not found: {event_id}"
                )

            return row


def delete_inbox_event_for_smoke_test(
    event_id: UUID | str,
) -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                delete from ops.kafka_device_event_inbox
                where event_id = %s;
                """,
                (event_id,),
            )
