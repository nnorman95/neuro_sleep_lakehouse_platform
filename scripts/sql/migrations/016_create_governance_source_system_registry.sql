create table if not exists governance.source_system_registry (
    source_id uuid primary key default uuidv7(),
    source_system text not null,
    dataset_name text not null,
    dataset_version text not null,
    base_url text not null,
    access_model text not null,
    credential_required boolean not null default true,
    active boolean not null default true,
    source_owner_role text not null default 'data_engineer',
    data_domain text not null,
    contains_health_data boolean not null default true,
    contains_patient_level_data boolean not null default true,
    contains_direct_identifier boolean not null default false,
    access_policy text not null default 'restricted',
    status text not null default 'planned',
    notes text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint source_system_registry_source_unique
        unique (source_system, dataset_version),

    constraint source_system_registry_access_model_check
        check (access_model in ('open', 'credentialed', 'restricted')),

    constraint source_system_registry_access_policy_check
        check (access_policy in ('open', 'team_only', 'restricted', 'no_public_access')),

    constraint source_system_registry_status_check
        check (status in ('planned', 'active', 'disabled', 'deprecated'))
);

create index if not exists source_system_registry_source_idx
    on governance.source_system_registry(source_system);

create index if not exists source_system_registry_status_idx
    on governance.source_system_registry(status);

create index if not exists source_system_registry_access_model_idx
    on governance.source_system_registry(access_model);

drop trigger if exists set_updated_at_source_system_registry
    on governance.source_system_registry;

create trigger set_updated_at_source_system_registry
before update on governance.source_system_registry
for each row
execute function governance.set_updated_at();
