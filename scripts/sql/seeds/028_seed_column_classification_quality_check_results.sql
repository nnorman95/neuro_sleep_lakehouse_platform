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
    'quality',
    'quality_check_results',
    column_name,
    'quality',
    classification_level,
    false,
    contains_health_data,
    false,
    sensitivity_reason,
    access_policy,
    masking_policy
from (
    values
        (
            'quality_result_id',
            'internal',
            false,
            'Operational UUIDv7 identifier.',
            'team_only',
            'none'
        ),
        (
            'pipeline_run_id',
            'internal',
            false,
            'Reference to operational pipeline history.',
            'team_only',
            'none'
        ),
        (
            'source_system',
            'internal',
            false,
            'Source-system metadata.',
            'team_only',
            'none'
        ),
        (
            'data_layer',
            'internal',
            false,
            'Platform-layer metadata.',
            'team_only',
            'none'
        ),
        (
            'dataset_name',
            'internal',
            false,
            'Dataset identifier.',
            'team_only',
            'none'
        ),
        (
            'recording_id',
            'confidential',
            true,
            'Pseudonymous patient-level recording identifier.',
            'restricted',
            'redact'
        ),
        (
            'record_key',
            'confidential',
            true,
            'Trace key may expose patient-level object or partition structure.',
            'restricted',
            'redact'
        ),
        (
            'check_name',
            'internal',
            false,
            'Machine-readable quality-check name.',
            'team_only',
            'none'
        ),
        (
            'severity',
            'internal',
            false,
            'Operational severity metadata.',
            'team_only',
            'none'
        ),
        (
            'status',
            'internal',
            false,
            'Quality-check outcome.',
            'team_only',
            'none'
        ),
        (
            'rows_checked',
            'internal',
            false,
            'Aggregated quality metric.',
            'team_only',
            'none'
        ),
        (
            'rows_failed',
            'internal',
            false,
            'Aggregated failed-row metric.',
            'team_only',
            'none'
        ),
        (
            'error_code',
            'internal',
            false,
            'Machine-readable issue code.',
            'team_only',
            'none'
        ),
        (
            'message',
            'confidential',
            true,
            'Quality message may contain health-related values or source fragments.',
            'restricted',
            'redact'
        ),
        (
            'details',
            'confidential',
            true,
            'Structured details may contain patient-level quality metadata.',
            'restricted',
            'redact'
        ),
        (
            'checked_at',
            'internal',
            false,
            'Quality evaluation timestamp.',
            'team_only',
            'none'
        ),
        (
            'created_at',
            'internal',
            false,
            'Database creation timestamp.',
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
