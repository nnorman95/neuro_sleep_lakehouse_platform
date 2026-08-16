create table if not exists ops.kafka_device_event_inbox (
    event_id uuid primary key,
    source_system text not null,
    schema_version text not null,
    device_id text not null,
    session_id uuid not null,
    event_type text not null,
    event_time timestamptz not null,
    sequence_number bigint not null,
    raw_event jsonb not null,
    event_fingerprint_sha256 text not null,
    kafka_topic text not null,
    first_kafka_partition integer not null,
    first_kafka_offset bigint not null,
    last_kafka_partition integer not null,
    last_kafka_offset bigint not null,
    kafka_timestamp_ms bigint not null,
    kafka_headers jsonb not null,
    first_ingested_at timestamptz not null,
    last_ingested_at timestamptz not null,
    delivery_count integer not null default 1,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint kafka_device_event_inbox_source_system_check
        check (source_system = 'simulated_bci_device'),

    constraint kafka_device_event_inbox_event_type_check
        check (
            event_type in (
                'session_started',
                'signal_quality',
                'battery_status',
                'session_ended'
            )
        ),

    constraint kafka_device_event_inbox_sequence_number_check
        check (sequence_number >= 0),

    constraint kafka_device_event_inbox_first_partition_check
        check (first_kafka_partition >= 0),

    constraint kafka_device_event_inbox_last_partition_check
        check (last_kafka_partition >= 0),

    constraint kafka_device_event_inbox_first_offset_check
        check (first_kafka_offset >= 0),

    constraint kafka_device_event_inbox_last_offset_check
        check (last_kafka_offset >= 0),

    constraint kafka_device_event_inbox_timestamp_check
        check (kafka_timestamp_ms >= 0),

    constraint kafka_device_event_inbox_delivery_count_check
        check (delivery_count > 0),

    constraint kafka_device_event_inbox_fingerprint_check
        check (
            event_fingerprint_sha256 ~ '^[0-9a-f]{64}$'
        ),

    constraint kafka_device_event_inbox_first_coordinate_uidx
        unique (
            kafka_topic,
            first_kafka_partition,
            first_kafka_offset
        )
);

create index if not exists kafka_device_event_inbox_device_time_idx
    on ops.kafka_device_event_inbox (
        device_id,
        event_time
    );

create index if not exists kafka_device_event_inbox_session_sequence_idx
    on ops.kafka_device_event_inbox (
        session_id,
        sequence_number
    );

create index if not exists kafka_device_event_inbox_last_seen_idx
    on ops.kafka_device_event_inbox (
        last_ingested_at
    );
