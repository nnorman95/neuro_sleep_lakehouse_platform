alter table governance.data_contract_registry
    drop constraint if exists data_contract_registry_layer_check;

alter table governance.data_contract_registry
    add constraint data_contract_registry_layer_check
        check (data_layer in ('raw', 'staging', 'warehouse', 'mart', 'ops', 'quality', 'governance'));

comment on column governance.data_contract_registry.data_layer is
    'Database/data product layer. This is not the same as object storage bucket layer.';

alter table governance.column_classification
    drop constraint if exists column_classification_layer_check;

alter table governance.column_classification
    add constraint column_classification_layer_check
        check (data_layer in ('raw', 'staging', 'warehouse', 'mart', 'ops', 'quality', 'governance'));

comment on column governance.column_classification.data_layer is
    'Database/data product layer. This is not the same as object storage bucket layer.';
