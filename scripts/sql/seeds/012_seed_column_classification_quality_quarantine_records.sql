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
    'quality' as table_schema,
    'quarantine_records' as table_name,
    column_name,
    'quality' as data_layer,
    classification_level,
    false as contains_personal_data,
    contains_health_data,
    false as contains_direct_identifier,
    sensitivity_reason,
    access_policy,
    masking_policy
from (
    values
        (
            'quarantine_id',
            'internal',
            false,
            'Operational identifier for a quarantined record.',
            'team_only',
            'none'
        ),
        (
            'source_system',
            'internal',
            false,
            'Source dataset name; does not directly store health measurements.',
            'team_only',
            'none'
        ),
        (
            'source_file_id',
            'internal',
            false,
            'Reference to raw.file_registry; does not directly store source content.',
            'team_only',
            'none'
        ),
        (
            'record_key',
            'confidential',
            false,
            'Trace key may expose source-level identifiers or file/record structure.',
            'restricted',
            'redact'
        ),
        (
            'raw_payload',
            'confidential',
            true,
            'Rejected raw payload may contain sleep or EEG-related health data.',
            'restricted',
            'redact'
        ),
        (
            'payload_bucket',
            'confidential',
            false,
            'Pointer to external quarantined payload storage.',
            'restricted',
            'redact'
        ),
        (
            'payload_object_key',
            'confidential',
            false,
            'Object key may expose source-level file or record structure.',
            'restricted',
            'redact'
        ),
        (
            'payload_size_bytes',
            'internal',
            false,
            'Operational size metadata for external quarantined payload.',
            'team_only',
            'none'
        ),
        (
            'payload_checksum_sha256',
            'internal',
            false,
            'Checksum metadata for external quarantined payload.',
            'team_only',
            'none'
        ),
        (
            'error_code',
            'internal',
            false,
            'Machine-readable quality error code.',
            'team_only',
            'none'
        ),
        (
            'error_message',
            'confidential',
            false,
            'Error message may accidentally include raw values or source fragments.',
            'restricted',
            'redact'
        ),
        (
            'severity',
            'internal',
            false,
            'Operational severity level for the quality issue.',
            'team_only',
            'none'
        ),
        (
            'detected_at',
            'internal',
            false,
            'Timestamp of quality issue detection.',
            'team_only',
            'none'
        ),
        (
            'pipeline_run_id',
            'internal',
            false,
            'Reference to ops.pipeline_run; operational metadata only.',
            'team_only',
            'none'
        ),
        (
            'status',
            'internal',
            false,
            'Review status of the quarantined record.',
            'team_only',
            'none'
        ),
        (
            'created_at',
            'internal',
            false,
            'Timestamp when the quarantine record was created.',
            'team_only',
            'none'
        )
) as columns_to_classify(
    column_name,
    classification_level,
    contains_health_data,
    sensitivity_reason,
    access_policy,
    masking_policy
)
on conflict (table_schema, table_name, column_name)
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