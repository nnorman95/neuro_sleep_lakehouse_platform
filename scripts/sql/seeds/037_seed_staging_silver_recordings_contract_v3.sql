update governance.data_contract_registry
set
    status = 'deprecated',
    updated_at = now()
where table_schema = 'staging'
  and table_name = 'silver_recordings'
  and contract_version in (
      'v1',
      'v2'
  );

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
    'staging',
    'silver_recordings',
    'staging_silver_recordings_contract',
    'v3',
    'contracts/staging_silver_recordings_v3.yml',
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
