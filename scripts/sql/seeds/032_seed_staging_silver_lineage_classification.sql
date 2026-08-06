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
    false,
    contains_health_data,
    false,
    sensitivity_reason,
    access_policy,
    masking_policy
from (
    values
        (
            'psg_file_id',
            'confidential',
            true,
            'Bronze lineage pointer for a health-data recording.',
            'restricted',
            'none'
        ),
        (
            'hypnogram_file_id',
            'confidential',
            true,
            'Bronze lineage pointer for sleep-stage annotations.',
            'restricted',
            'none'
        ),
        (
            'source_pair_id',
            'confidential',
            true,
            'Pseudonymous identity of one logical source recording pair.',
            'restricted',
            'none'
        ),
        (
            'input_fingerprint',
            'confidential',
            true,
            'Content identity of one health-data source pair.',
            'restricted',
            'none'
        ),
        (
            'config_id',
            'internal',
            false,
            'Transformation configuration identity.',
            'team_only',
            'none'
        ),
        (
            'schema_version',
            'internal',
            false,
            'Silver schema version.',
            'team_only',
            'none'
        ),
        (
            'transform_version',
            'internal',
            false,
            'Silver transformation version.',
            'team_only',
            'none'
        ),
        (
            'psg_checksum_sha256',
            'confidential',
            true,
            'Verified checksum of the source PSG payload.',
            'restricted',
            'none'
        ),
        (
            'hypnogram_checksum_sha256',
            'confidential',
            true,
            'Verified checksum of the source Hypnogram payload.',
            'restricted',
            'none'
        ),
        (
            'silver_bucket',
            'internal',
            false,
            'Silver object-storage bucket.',
            'team_only',
            'none'
        ),
        (
            'silver_output_prefix',
            'confidential',
            true,
            'Object prefix of a patient-level Silver representation.',
            'restricted',
            'none'
        ),
        (
            'staging_load_run_id',
            'internal',
            false,
            'Operational lineage to the staging load run.',
            'team_only',
            'none'
        ),
        (
            'loaded_at',
            'internal',
            false,
            'Staging load timestamp.',
            'team_only',
            'none'
        )
) as lineage_columns(
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
