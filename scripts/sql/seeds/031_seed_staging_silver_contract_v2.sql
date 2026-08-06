update governance.data_contract_registry
set
    status = 'deprecated',
    updated_at = now()
where table_schema = 'staging'
  and table_name in (
      'silver_recordings',
      'silver_sleep_stage_intervals'
  )
  and contract_version = 'v1';

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
        'silver_recordings',
        'staging_silver_recordings_contract',
        'v2',
        'contracts/staging_silver_recordings_v2.yml',
        'data_engineer',
        'staging',
        'active'
    ),
    (
        'staging',
        'silver_sleep_stage_intervals',
        'staging_silver_sleep_stage_intervals_contract',
        'v2',
        'contracts/staging_silver_sleep_stage_intervals_v2.yml',
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
