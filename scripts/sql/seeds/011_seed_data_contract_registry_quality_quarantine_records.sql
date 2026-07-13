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
    'v1',
    'contracts/quality_quarantine_records.yml',
    'data_engineer',
    'quality',
    'active'
)
on conflict (table_schema, table_name, contract_version)
do update set
    contract_name = excluded.contract_name,
    contract_path = excluded.contract_path,
    owner_role = excluded.owner_role,
    data_layer = excluded.data_layer,
    status = excluded.status,
    updated_at = now();
