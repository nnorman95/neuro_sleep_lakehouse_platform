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
values
    (
        'staging',
        'silver_subjects',
        'staging_silver_subjects_contract',
        'v1',
        'contracts/staging_silver_subjects.yml',
        'data_engineer',
        'staging',
        'active'
    ),
    (
        'staging',
        'silver_recording_contexts',
        'staging_silver_recording_contexts_contract',
        'v1',
        'contracts/staging_silver_recording_contexts.yml',
        'data_engineer',
        'staging',
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
