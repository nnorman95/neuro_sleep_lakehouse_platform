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
    'governance',
    'source_system_registry',
    'governance_source_system_registry_contract',
    'v1',
    'contracts/governance_source_system_registry.yml',
    'data_engineer',
    'governance',
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
