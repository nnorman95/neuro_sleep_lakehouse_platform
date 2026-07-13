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
    'ops' as table_schema,
    'pipeline_run' as table_name,
    column_name,
    'ops' as data_layer,
    'internal' as classification_level,
    false as contains_personal_data,
    false as contains_health_data,
    false as contains_direct_identifier,
    'Operational pipeline execution metadata; does not intentionally store personal identifiers or health measurements.' as sensitivity_reason,
    'team_only' as access_policy,
    'none' as masking_policy
from (
    values
        ('run_id'),
        ('pipeline_name'),
        ('task_name'),
        ('source_system'),
        ('status'),
        ('started_at'),
        ('finished_at'),
        ('rows_read'),
        ('rows_written'),
        ('files_processed'),
        ('records_quarantined'),
        ('error_message'),
        ('created_at')
) as columns_to_classify(column_name)
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
