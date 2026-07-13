create table if not exists ops.pipeline_run (
    run_id uuid primary key default uuidv7(),
    pipeline_name text not null,
    task_name text,
    source_system text,
    status text not null,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    rows_read bigint not null default 0,
    rows_written bigint not null default 0,
    files_processed integer not null default 0,
    records_quarantined integer not null default 0,
    error_message text,
    created_at timestamptz not null default now(),

    constraint pipeline_run_status_check
        check (status in ('started', 'success', 'failed', 'skipped', 'warning'))
);
