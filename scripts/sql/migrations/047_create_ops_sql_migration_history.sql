create table if not exists ops.sql_migration_history (
    migration_path text primary key,
    checksum_sha256 text not null,
    applied_at timestamptz not null default now(),
    constraint sql_migration_history_path_check
        check (length(btrim(migration_path)) > 0),
    constraint sql_migration_history_checksum_check
        check (checksum_sha256 ~ '^[0-9a-f]{64}$')
);
