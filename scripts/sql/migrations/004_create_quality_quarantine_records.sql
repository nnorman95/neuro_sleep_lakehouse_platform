create table if not exists quality.quarantine_records (
    quarantine_id uuid primary key default uuidv7(),
    source_system text not null,
    source_file_id uuid references raw.file_registry(file_id),
    record_key text,
    raw_payload jsonb,
    error_code text not null,
    error_message text not null,
    severity text not null default 'error',
    detected_at timestamptz not null default now(),
    pipeline_run_id uuid references ops.pipeline_run(run_id),
    status text not null default 'open',
    created_at timestamptz not null default now(),

    constraint quarantine_records_severity_check
        check (severity in ('info', 'warning', 'error', 'critical')),

    constraint quarantine_records_status_check
        check (status in ('open', 'reviewed', 'resolved', 'ignored'))
);

create index if not exists quarantine_records_error_code_idx
    on quality.quarantine_records(error_code);

create index if not exists quarantine_records_source_file_id_idx
    on quality.quarantine_records(source_file_id);

create index if not exists quarantine_records_pipeline_run_id_idx
    on quality.quarantine_records(pipeline_run_id);
