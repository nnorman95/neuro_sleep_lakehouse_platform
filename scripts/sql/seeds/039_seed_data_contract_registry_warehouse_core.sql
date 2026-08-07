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
    ('warehouse', 'dim_subject', 'warehouse_dim_subject_contract', 'v1', 'contracts/warehouse_dim_subject.yml', 'data_engineer', 'warehouse', 'active'),
    ('warehouse', 'dim_recording', 'warehouse_dim_recording_contract', 'v1', 'contracts/warehouse_dim_recording.yml', 'data_engineer', 'warehouse', 'active'),
    ('warehouse', 'dim_channel', 'warehouse_dim_channel_contract', 'v1', 'contracts/warehouse_dim_channel.yml', 'data_engineer', 'warehouse', 'active'),
    ('warehouse', 'dim_sleep_stage', 'warehouse_dim_sleep_stage_contract', 'v1', 'contracts/warehouse_dim_sleep_stage.yml', 'data_engineer', 'warehouse', 'active'),
    ('warehouse', 'fact_sleep_epoch', 'warehouse_fact_sleep_epoch_contract', 'v1', 'contracts/warehouse_fact_sleep_epoch.yml', 'data_engineer', 'warehouse', 'active')
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
