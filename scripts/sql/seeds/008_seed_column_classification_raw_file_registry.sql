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
    'raw' as table_schema,
    'file_registry' as table_name,
    column_name,
    'raw' as data_layer,
    'internal' as classification_level,
    false as contains_personal_data,
    false as contains_health_data,
    false as contains_direct_identifier,
    'Operational file metadata; no direct personal identifiers or health measurements are stored in this table.' as sensitivity_reason,
    'team_only' as access_policy,
    'none' as masking_policy
from (
    values
        ('file_id'),
        ('source_system'),
        ('source_url'),
        ('bucket'),
        ('object_key'),
        ('file_name'),
        ('file_type'),
        ('file_size_bytes'),
        ('checksum_sha256'),
        ('ingested_at'),
        ('ingestion_run_id'),
        ('status'),
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
