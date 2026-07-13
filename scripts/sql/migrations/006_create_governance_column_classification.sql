create table if not exists governance.column_classification (
    classification_id uuid primary key default uuidv7(),
    table_schema text not null,
    table_name text not null,
    column_name text not null,
    data_layer text not null,
    classification_level text not null default 'internal',
    contains_personal_data boolean not null default false,
    contains_health_data boolean not null default false,
    contains_direct_identifier boolean not null default false,
    sensitivity_reason text,
    access_policy text not null default 'team_only',
    masking_policy text not null default 'none',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint column_classification_unique_column
        unique (table_schema, table_name, column_name),

    constraint column_classification_layer_check
        check (data_layer in ('raw', 'staging', 'warehouse', 'mart', 'ops', 'quality', 'governance')),

    constraint column_classification_level_check
        check (classification_level in ('public', 'internal', 'confidential', 'restricted')),

    constraint column_classification_access_policy_check
        check (access_policy in ('open', 'team_only', 'restricted', 'no_public_access')),

    constraint column_classification_masking_policy_check
        check (masking_policy in ('none', 'hash', 'redact', 'aggregate_only'))
);

create index if not exists column_classification_table_idx
    on governance.column_classification(table_schema, table_name);

create index if not exists column_classification_level_idx
    on governance.column_classification(classification_level);

create index if not exists column_classification_access_policy_idx
    on governance.column_classification(access_policy);

create index if not exists column_classification_health_data_idx
    on governance.column_classification(contains_health_data);
