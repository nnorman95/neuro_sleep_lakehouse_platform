alter table staging.silver_recordings
    add column if not exists dataset_version text,
    add column if not exists collection text,
    add column if not exists recording_key text;

alter table staging.silver_recordings
    alter column dataset_version set not null,
    alter column collection set not null,
    alter column recording_key set not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname =
            'silver_recordings_dataset_version_nonempty'
          and conrelid =
            'staging.silver_recordings'::regclass
    ) then
        alter table staging.silver_recordings
            add constraint
                silver_recordings_dataset_version_nonempty
            check (
                length(trim(dataset_version)) > 0
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname =
            'silver_recordings_collection_check'
          and conrelid =
            'staging.silver_recordings'::regclass
    ) then
        alter table staging.silver_recordings
            add constraint
                silver_recordings_collection_check
            check (
                collection in (
                    'sleep-cassette',
                    'sleep-telemetry'
                )
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname =
            'silver_recordings_recording_key_nonempty'
          and conrelid =
            'staging.silver_recordings'::regclass
    ) then
        alter table staging.silver_recordings
            add constraint
                silver_recordings_recording_key_nonempty
            check (
                length(trim(recording_key)) > 0
            );
    end if;
end
$$;

create index if not exists
    idx_silver_recordings_logical_recording
    on staging.silver_recordings (
        source_system,
        dataset_version,
        collection,
        recording_key
    );

comment on column
    staging.silver_recordings.dataset_version
is
    'Source dataset version used in the logical recording business key.';

comment on column
    staging.silver_recordings.collection
is
    'Sleep-EDF collection used in the logical recording business key.';

comment on column
    staging.silver_recordings.recording_key
is
    'Logical Sleep-EDF recording/night key resolved by source classification.';
