create table if not exists raw.file_registry (
    file_id uuid primary key default uuidv7(),
    source_system text not null,
    source_url text,
    bucket text not null,
    object_key text not null,
    file_name text not null,
    file_type text not null,
    file_size_bytes bigint,
    checksum_sha256 text,
    ingested_at timestamptz not null default now(),
    ingestion_run_id uuid references ops.pipeline_run(run_id),
    status text not null default 'registered',
    created_at timestamptz not null default now(),

    constraint file_registry_bucket_object_key_unique
        unique (bucket, object_key),

    constraint file_registry_file_size_nonnegative
        check (file_size_bytes is null or file_size_bytes >= 0),

    constraint file_registry_status_check
        check (status in ('registered', 'uploaded', 'skipped', 'duplicate', 'failed'))
);
