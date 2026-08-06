create table if not exists quality.quality_check_results (
    quality_result_id uuid primary key default uuidv7(),
    pipeline_run_id uuid not null
        references ops.pipeline_run(run_id),
    source_system text,
    data_layer text not null,
    dataset_name text not null,
    recording_id uuid,
    record_key text,
    check_name text not null,
    severity text not null,
    status text not null,
    rows_checked bigint not null default 0,
    rows_failed bigint not null default 0,
    error_code text,
    message text,
    details jsonb not null default '{}'::jsonb,
    checked_at timestamptz not null default now(),
    created_at timestamptz not null default now(),

    constraint quality_check_results_layer_check
        check (
            data_layer in (
                'bronze',
                'silver',
                'gold',
                'raw',
                'staging',
                'warehouse',
                'mart',
                'ops',
                'quality',
                'governance'
            )
        ),

    constraint quality_check_results_severity_check
        check (
            severity in (
                'info',
                'warning',
                'error',
                'critical'
            )
        ),

    constraint quality_check_results_status_check
        check (
            status in (
                'passed',
                'warning',
                'failed',
                'skipped'
            )
        ),

    constraint quality_check_results_rows_checked_check
        check (rows_checked >= 0),

    constraint quality_check_results_rows_failed_check
        check (rows_failed >= 0)
);

create index if not exists
    quality_check_results_pipeline_run_idx
    on quality.quality_check_results(
        pipeline_run_id
    );

create index if not exists
    quality_check_results_recording_idx
    on quality.quality_check_results(
        recording_id
    );

create index if not exists
    quality_check_results_dataset_status_idx
    on quality.quality_check_results(
        data_layer,
        dataset_name,
        status
    );

create index if not exists
    quality_check_results_checked_at_idx
    on quality.quality_check_results(
        checked_at
    );
