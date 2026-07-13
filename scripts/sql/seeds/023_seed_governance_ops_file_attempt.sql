insert into governance.data_contract_registry (
    table_schema,
    table_name,
    contract_name,
    contract_version,
    contract_path,
    owner_role,
    data_layer,
    status
)
values (
    'ops',
    'file_attempt',
    'ops_file_attempt_contract',
    'v1',
    'contracts/ops_file_attempt.yml',
    'data_engineer',
    'ops',
    'active'
)
on conflict (
    table_schema,
    table_name,
    contract_version
)
do update set
    contract_name = excluded.contract_name,
    contract_path = excluded.contract_path,
    owner_role = excluded.owner_role,
    data_layer = excluded.data_layer,
    status = excluded.status,
    updated_at = now();


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
    'ops',
    'file_attempt',
    column_name,
    'ops',
    classification_level,
    false,
    false,
    false,
    sensitivity_reason,
    access_policy,
    masking_policy
from (
    values
        (
            'attempt_id',
            'internal',
            'Operational identifier for a file-processing attempt.',
            'team_only',
            'none'
        ),
        (
            'pipeline_run_id',
            'internal',
            'Reference to the parent pipeline run.',
            'team_only',
            'none'
        ),
        (
            'source_system',
            'internal',
            'External dataset or source-system name.',
            'team_only',
            'none'
        ),
        (
            'source_url',
            'confidential',
            'Source URL may expose dataset paths or source-level identifiers.',
            'restricted',
            'redact'
        ),
        (
            'bucket',
            'internal',
            'Object storage bucket name.',
            'team_only',
            'none'
        ),
        (
            'object_key',
            'confidential',
            'Object key may expose source file structure or pseudonymous identifiers.',
            'restricted',
            'redact'
        ),
        (
            'file_name',
            'confidential',
            'File name may expose source-level or pseudonymous identifiers.',
            'restricted',
            'redact'
        ),
        (
            'file_type',
            'internal',
            'Technical file format metadata.',
            'team_only',
            'none'
        ),
        (
            'status',
            'internal',
            'Operational lifecycle status of the attempt.',
            'team_only',
            'none'
        ),
        (
            'resolution',
            'internal',
            'Detailed operational outcome of the attempt.',
            'team_only',
            'none'
        ),
        (
            'file_size_bytes',
            'internal',
            'Operational file size metadata.',
            'team_only',
            'none'
        ),
        (
            'checksum_sha256',
            'internal',
            'Integrity checksum for the processed object.',
            'team_only',
            'none'
        ),
        (
            'error_type',
            'internal',
            'Machine-readable exception type.',
            'team_only',
            'none'
        ),
        (
            'error_message',
            'confidential',
            'Error text may accidentally contain source paths or source fragments.',
            'restricted',
            'redact'
        ),
        (
            'started_at',
            'internal',
            'Timestamp when file processing started.',
            'team_only',
            'none'
        ),
        (
            'finished_at',
            'internal',
            'Timestamp when file processing finished.',
            'team_only',
            'none'
        ),
        (
            'created_at',
            'internal',
            'Timestamp when the attempt record was created.',
            'team_only',
            'none'
        )
) as columns_to_classify(
    column_name,
    classification_level,
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
