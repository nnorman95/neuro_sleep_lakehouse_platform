create table if not exists governance.data_contract_registry (
    contract_id uuid primary key default uuidv7(),
    table_schema text not null,
    table_name text not null,
    contract_name text not null,
    contract_version text not null default 'v1',
    contract_path text not null,
    owner_role text not null,
    data_layer text not null,
    status text not null default 'active',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint data_contract_registry_unique_version
        unique (table_schema, table_name, contract_version),

    constraint data_contract_registry_layer_check
        check (data_layer in ('raw', 'staging', 'warehouse', 'mart', 'ops', 'quality', 'governance')),

    constraint data_contract_registry_status_check
        check (status in ('draft', 'active', 'deprecated', 'disabled'))
);

create index if not exists data_contract_registry_table_idx
    on governance.data_contract_registry(table_schema, table_name);

create index if not exists data_contract_registry_status_idx
    on governance.data_contract_registry(status);

create index if not exists data_contract_registry_layer_idx
    on governance.data_contract_registry(data_layer);
