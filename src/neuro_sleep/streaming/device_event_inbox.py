from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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


DEVICE_EVENT_ARRIVAL_CLASSIFICATION_VERSION = "1.0.0"
DEVICE_EVENT_LATE_THRESHOLD_MS = 60_000


@dataclass(frozen=True)
class InboxWriteResult:
    event_id: UUID
    status: str
    delivery_count: int
    arrival_classification_version: str | None
    ingestion_delay_ms: int | None
    is_late: bool | None
    is_out_of_order: bool | None
    out_of_order_reason: str | None


@dataclass(frozen=True)
class DeviceEventArrivalClassification:
    ingestion_delay_ms: int
    is_late: bool
    is_out_of_order: bool
    out_of_order_reason: str | None
    previous_max_sequence_number: int | None
    previous_max_event_time: datetime | None


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


def _classify_event_arrival(
    *,
    cursor,
    consumed: ConsumedDeviceEvent,
) -> DeviceEventArrivalClassification:
    event = consumed.event

    cursor.execute(
        """
        select
            max(sequence_number),
            max(event_time)
        from ops.kafka_device_event_inbox
        where device_id = %s
          and session_id = %s
          and event_id <> %s;
        """,
        (
            event.device_id,
            event.session_id,
            event.event_id,
        ),
    )

    row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            "Arrival classification query "
            "returned no aggregate row"
        )

    (
        previous_max_sequence_number,
        previous_max_event_time,
    ) = row

    ingestion_delay_ms = int(
        (
            consumed.ingested_at
            - event.event_time
        ).total_seconds()
        * 1000
    )

    is_late = (
        ingestion_delay_ms
        > DEVICE_EVENT_LATE_THRESHOLD_MS
    )

    sequence_out_of_order = (
        previous_max_sequence_number is not None
        and event.sequence_number
        <= previous_max_sequence_number
    )

    event_time_out_of_order = (
        previous_max_event_time is not None
        and event.event_time
        < previous_max_event_time
    )

    if (
        sequence_out_of_order
        and event_time_out_of_order
    ):
        reason = "sequence_and_event_time"
    elif sequence_out_of_order:
        reason = "sequence"
    elif event_time_out_of_order:
        reason = "event_time"
    else:
        reason = None

    return DeviceEventArrivalClassification(
        ingestion_delay_ms=ingestion_delay_ms,
        is_late=is_late,
        is_out_of_order=(
            sequence_out_of_order
            or event_time_out_of_order
        ),
        out_of_order_reason=reason,
        previous_max_sequence_number=(
            previous_max_sequence_number
        ),
        previous_max_event_time=(
            previous_max_event_time
        ),
    )


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

            if delivery_count == 1:
                classification = (
                    _classify_event_arrival(
                        cursor=cursor,
                        consumed=consumed,
                    )
                )

                cursor.execute(
                    """
                    update ops.kafka_device_event_inbox
                    set
                        arrival_classification_version = %s,
                        ingestion_delay_ms = %s,
                        late_threshold_ms = %s,
                        is_late = %s,
                        is_out_of_order = %s,
                        out_of_order_reason = %s,
                        previous_max_sequence_number = %s,
                        previous_max_event_time = %s
                    where event_id = %s
                    returning
                        arrival_classification_version,
                        ingestion_delay_ms,
                        is_late,
                        is_out_of_order,
                        out_of_order_reason;
                    """,
                    (
                        DEVICE_EVENT_ARRIVAL_CLASSIFICATION_VERSION,
                        classification.ingestion_delay_ms,
                        DEVICE_EVENT_LATE_THRESHOLD_MS,
                        classification.is_late,
                        classification.is_out_of_order,
                        classification.out_of_order_reason,
                        classification.previous_max_sequence_number,
                        classification.previous_max_event_time,
                        event_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    select
                        arrival_classification_version,
                        ingestion_delay_ms,
                        is_late,
                        is_out_of_order,
                        out_of_order_reason
                    from ops.kafka_device_event_inbox
                    where event_id = %s;
                    """,
                    (event_id,),
                )

            classification_row = cursor.fetchone()

            if classification_row is None:
                raise RuntimeError(
                    "Kafka inbox arrival classification "
                    "row is unavailable"
                )

            (
                arrival_classification_version,
                ingestion_delay_ms,
                is_late,
                is_out_of_order,
                out_of_order_reason,
            ) = classification_row

            return InboxWriteResult(
                event_id=event_id,
                status=(
                    "inserted"
                    if delivery_count == 1
                    else "duplicate"
                ),
                delivery_count=delivery_count,
                arrival_classification_version=(
                    arrival_classification_version
                ),
                ingestion_delay_ms=(
                    ingestion_delay_ms
                ),
                is_late=is_late,
                is_out_of_order=is_out_of_order,
                out_of_order_reason=(
                    out_of_order_reason
                ),
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
                    delivery_count,
                    arrival_classification_version,
                    ingestion_delay_ms,
                    late_threshold_ms,
                    is_late,
                    is_out_of_order,
                    out_of_order_reason,
                    previous_max_sequence_number,
                    previous_max_event_time
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
