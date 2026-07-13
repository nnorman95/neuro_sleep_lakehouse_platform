create table if not exists ops.file_attempt (
    attempt_id uuid primary key default uuidv7(),

    pipeline_run_id uuid not null
        references ops.pipeline_run(run_id),

    source_system text not null,
    source_url text,

    bucket text not null,
    object_key text not null,
    file_name text not null,
    file_type text not null,

    status text not null default 'started',
    resolution text,

    file_size_bytes bigint,
    checksum_sha256 text,

    error_type text,
    error_message text,

    started_at timestamptz not null default now(),
    finished_at timestamptz,
    created_at timestamptz not null default now(),

    constraint file_attempt_run_object_unique
        unique (
            pipeline_run_id,
            bucket,
            object_key
        ),

    constraint file_attempt_status_check
        check (
            status in (
                'started',
                'uploaded',
                'skipped',
                'failed'
            )
        ),

    constraint file_attempt_resolution_check
        check (
            (
                status = 'started'
                and resolution is null
            )
            or (
                status = 'uploaded'
                and resolution =
                    'downloaded_and_uploaded'
            )
            or (
                status = 'skipped'
                and resolution in (
                    'existing_valid',
                    'recovered_existing'
                )
            )
            or (
                status = 'failed'
                and resolution is null
            )
        ),

    constraint file_attempt_size_nonnegative
        check (
            file_size_bytes is null
            or file_size_bytes >= 0
        ),

    constraint file_attempt_checksum_check
        check (
            checksum_sha256 is null
            or checksum_sha256
                ~ '^[0-9a-f]{64}$'
        ),

    constraint file_attempt_timestamps_check
        check (
            finished_at is null
            or finished_at >= started_at
        ),

    constraint file_attempt_terminal_time_check
        check (
            (
                status = 'started'
                and finished_at is null
            )
            or (
                status in (
                    'uploaded',
                    'skipped',
                    'failed'
                )
                and finished_at is not null
            )
        ),

    constraint file_attempt_failure_error_check
        check (
            (
                status = 'failed'
                and error_type is not null
                and error_message is not null
            )
            or (
                status <> 'failed'
                and error_type is null
                and error_message is null
            )
        ),

    constraint file_attempt_uploaded_metadata_check
        check (
            status <> 'uploaded'
            or (
                file_size_bytes is not null
                and checksum_sha256 is not null
            )
        )
);


create index if not exists
    idx_file_attempt_pipeline_run
on ops.file_attempt (
    pipeline_run_id
);


create index if not exists
    idx_file_attempt_object_history
on ops.file_attempt (
    bucket,
    object_key,
    started_at desc
);


create index if not exists
    idx_file_attempt_status
on ops.file_attempt (
    status
);


comment on table ops.file_attempt is
    'Immutable history of file processing outcomes for each pipeline run.';

comment on column ops.file_attempt.resolution is
    'Successful outcome detail: downloaded_and_uploaded, existing_valid, or recovered_existing.';
