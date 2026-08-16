insert into governance.column_classification (
    table_schema,
    table_name,
    column_name,
    data_layer,
    classification_level,
    contains_personal_data,
    contains_health_data,
    contains_direct_identifier,
    sensitivity_reason,
    access_policy,
    masking_policy
)
select
    'warehouse',
    'fact_device_event',
    column_name,
    'warehouse',
    classification_level,
    contains_personal_data,
    contains_health_data,
    false,
    sensitivity_reason,
    access_policy,
    masking_policy
from (
    values
        ('device_event_sk', 'confidential', true, true, 'Pseudonymous Warehouse device-event identity linked to health telemetry.', 'restricted', 'none'),
        ('event_id', 'confidential', true, true, 'Stable pseudonymous source event identity.', 'restricted', 'none'),
        ('source_system', 'internal', false, false, 'Streaming source-system identifier.', 'team_only', 'none'),
        ('schema_version', 'internal', false, false, 'Source event contract version.', 'team_only', 'none'),
        ('device_id', 'confidential', true, true, 'Pseudonymous device identity associated with health telemetry.', 'restricted', 'none'),
        ('session_id', 'confidential', true, true, 'Pseudonymous device-session identity associated with health telemetry.', 'restricted', 'none'),
        ('event_type', 'confidential', false, true, 'Health-device event category.', 'restricted', 'none'),
        ('event_time', 'confidential', false, true, 'Timestamp of health-device activity.', 'restricted', 'none'),
        ('sequence_number', 'internal', false, true, 'Session ordering metadata for health-device events.', 'team_only', 'none'),
        ('payload', 'confidential', true, true, 'Validated event-specific health-device payload.', 'restricted', 'none'),
        ('event_fingerprint_sha256', 'internal', false, true, 'Deterministic event-content identity used for lineage.', 'team_only', 'none'),
        ('arrival_classification_version', 'internal', false, false, 'Version of streaming arrival classification logic.', 'team_only', 'none'),
        ('ingestion_delay_ms', 'internal', false, true, 'Observed event ingestion delay.', 'team_only', 'none'),
        ('late_threshold_ms', 'internal', false, false, 'Configured threshold used for late classification.', 'team_only', 'none'),
        ('is_late', 'internal', false, true, 'Streaming arrival-quality flag.', 'team_only', 'none'),
        ('is_out_of_order', 'internal', false, true, 'Streaming ordering-quality flag.', 'team_only', 'none'),
        ('out_of_order_reason', 'internal', false, true, 'Reason for streaming ordering classification.', 'team_only', 'none'),
        ('first_ingested_at', 'internal', false, true, 'First durable consumer-ingestion timestamp.', 'team_only', 'none'),
        ('last_ingested_at', 'internal', false, true, 'Most recent durable delivery timestamp.', 'team_only', 'none'),
        ('delivery_count', 'internal', false, false, 'Count of durable deliveries observed for event_id.', 'team_only', 'none'),
        ('kafka_topic', 'internal', false, false, 'Kafka lineage topic.', 'team_only', 'none'),
        ('first_kafka_partition', 'internal', false, false, 'First Kafka lineage partition.', 'team_only', 'none'),
        ('first_kafka_offset', 'internal', false, false, 'First Kafka lineage offset.', 'team_only', 'none'),
        ('last_kafka_partition', 'internal', false, false, 'Most recent Kafka lineage partition.', 'team_only', 'none'),
        ('last_kafka_offset', 'internal', false, false, 'Most recent Kafka lineage offset.', 'team_only', 'none'),
        ('inbox_created_at', 'internal', false, false, 'Durable inbox creation timestamp.', 'team_only', 'none'),
        ('inbox_updated_at', 'internal', false, false, 'Durable inbox last-update timestamp.', 'team_only', 'none')
) as classification_data (
    column_name,
    classification_level,
    contains_personal_data,
    contains_health_data,
    sensitivity_reason,
    access_policy,
    masking_policy
)
on conflict (
    table_schema,
    table_name,
    column_name
)
do update set
    data_layer = excluded.data_layer,
    classification_level = excluded.classification_level,
    contains_personal_data = excluded.contains_personal_data,
    contains_health_data = excluded.contains_health_data,
    contains_direct_identifier = excluded.contains_direct_identifier,
    sensitivity_reason = excluded.sensitivity_reason,
    access_policy = excluded.access_policy,
    masking_policy = excluded.masking_policy,
    updated_at = now();
