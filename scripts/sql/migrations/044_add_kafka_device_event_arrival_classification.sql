alter table ops.kafka_device_event_inbox
    add column if not exists arrival_classification_version text,
    add column if not exists ingestion_delay_ms bigint,
    add column if not exists late_threshold_ms bigint,
    add column if not exists is_late boolean,
    add column if not exists is_out_of_order boolean,
    add column if not exists out_of_order_reason text,
    add column if not exists previous_max_sequence_number bigint,
    add column if not exists previous_max_event_time timestamptz;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname =
            'kafka_device_event_inbox_late_threshold_check'
    ) then
        alter table ops.kafka_device_event_inbox
            add constraint kafka_device_event_inbox_late_threshold_check
            check (
                late_threshold_ms is null
                or late_threshold_ms > 0
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname =
            'kafka_device_event_inbox_out_of_order_reason_check'
    ) then
        alter table ops.kafka_device_event_inbox
            add constraint kafka_device_event_inbox_out_of_order_reason_check
            check (
                out_of_order_reason is null
                or out_of_order_reason in (
                    'sequence',
                    'event_time',
                    'sequence_and_event_time'
                )
            );
    end if;
end
$$;

comment on column
    ops.kafka_device_event_inbox.ingestion_delay_ms
is
    'Signed difference between consumer ingested_at and source event_time.';

comment on column
    ops.kafka_device_event_inbox.is_late
is
    'True when ingestion_delay_ms exceeds the recorded late_threshold_ms.';

comment on column
    ops.kafka_device_event_inbox.is_out_of_order
is
    'True when first arrival regresses relative to prior session sequence or event_time.';
