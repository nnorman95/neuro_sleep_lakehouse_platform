alter table ops.pipeline_run
add column if not exists heartbeat_at timestamptz;

update ops.pipeline_run
set heartbeat_at = coalesce(
    finished_at,
    started_at,
    created_at
)
where heartbeat_at is null;

alter table ops.pipeline_run
alter column heartbeat_at set default now();

alter table ops.pipeline_run
alter column heartbeat_at set not null;

create index if not exists
    idx_pipeline_run_active_heartbeat
on ops.pipeline_run (
    pipeline_name,
    heartbeat_at
)
where status = 'started';

comment on column ops.pipeline_run.heartbeat_at is
    'Most recent signal that the pipeline process is still alive.';
