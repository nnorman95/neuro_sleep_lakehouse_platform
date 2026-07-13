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
values (
    'ops',
    'pipeline_run',
    'heartbeat_at',
    'ops',
    'internal',
    false,
    false,
    false,
    'Operational liveness timestamp for an active pipeline run.',
    'team_only',
    'none'
)
on conflict (
    table_schema,
    table_name,
    column_name
)
do update set
    data_layer = excluded.data_layer,
    classification_level =
        excluded.classification_level,
    contains_personal_data =
        excluded.contains_personal_data,
    contains_health_data =
        excluded.contains_health_data,
    contains_direct_identifier =
        excluded.contains_direct_identifier,
    sensitivity_reason =
        excluded.sensitivity_reason,
    access_policy =
        excluded.access_policy,
    masking_policy =
        excluded.masking_policy,
    updated_at = now();
