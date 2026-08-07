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
    'staging',
    'silver_recordings',
    column_name,
    'staging',
    classification_level,
    contains_personal_data,
    contains_health_data,
    false,
    sensitivity_reason,
    access_policy,
    masking_policy
from (
    values
        (
            'dataset_version',
            'internal',
            false,
            false,
            'Source dataset version used for logical recording identity.',
            'team_only',
            'none'
        ),
        (
            'collection',
            'confidential',
            false,
            true,
            'Sleep-EDF study collection linked to a health-data recording.',
            'restricted',
            'none'
        ),
        (
            'recording_key',
            'confidential',
            true,
            true,
            'Pseudonymous logical recording/night identifier from a finite public health dataset.',
            'restricted',
            'none'
        )
) as logical_identity_columns(
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
