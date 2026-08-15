update governance.data_contract_registry
set
    status = 'deprecated',
    updated_at = now()
where table_schema = 'quality'
  and table_name = 'quarantine_records'
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
values (
    'quality',
    'quarantine_records',
    'quality_quarantine_records_contract',
    'v2',
    'contracts/quality_quarantine_records_v2.yml',
    'data_engineer',
    'quality',
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
