alter table staging.silver_recordings
    add column if not exists psg_file_id uuid,
    add column if not exists hypnogram_file_id uuid,
    add column if not exists source_pair_id text,
    add column if not exists input_fingerprint text,
    add column if not exists config_id text,
    add column if not exists schema_version text,
    add column if not exists transform_version text,
    add column if not exists psg_checksum_sha256 text,
    add column if not exists hypnogram_checksum_sha256 text,
    add column if not exists silver_bucket text,
    add column if not exists silver_output_prefix text,
    add column if not exists staging_load_run_id uuid,
    add column if not exists loaded_at timestamptz
        default now();

alter table staging.silver_recordings
    alter column psg_file_id set not null,
    alter column hypnogram_file_id set not null,
    alter column source_pair_id set not null,
    alter column input_fingerprint set not null,
    alter column config_id set not null,
    alter column schema_version set not null,
    alter column transform_version set not null,
    alter column psg_checksum_sha256 set not null,
    alter column hypnogram_checksum_sha256 set not null,
    alter column silver_bucket set not null,
    alter column silver_output_prefix set not null,
    alter column staging_load_run_id set not null,
    alter column loaded_at set not null;

alter table staging.silver_recordings
    drop constraint if exists
        silver_recordings_source_objects_unique;

alter table staging.silver_sleep_stage_intervals
    drop constraint if exists
        silver_sleep_stage_intervals_onset_nonnegative;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_psg_file_fk'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint silver_recordings_psg_file_fk
            foreign key (psg_file_id)
            references raw.file_registry(file_id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_hypnogram_file_fk'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint silver_recordings_hypnogram_file_fk
            foreign key (hypnogram_file_id)
            references raw.file_registry(file_id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_staging_load_run_fk'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint silver_recordings_staging_load_run_fk
            foreign key (staging_load_run_id)
            references ops.pipeline_run(run_id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_source_pair_id_format'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint silver_recordings_source_pair_id_format
            check (
                source_pair_id ~ '^[0-9a-f]{64}$'
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_input_fingerprint_format'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint silver_recordings_input_fingerprint_format
            check (
                input_fingerprint ~ '^[0-9a-f]{64}$'
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_config_id_format'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint silver_recordings_config_id_format
            check (
                config_id ~ '^[0-9a-f]{64}$'
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_psg_checksum_format'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint silver_recordings_psg_checksum_format
            check (
                psg_checksum_sha256 ~ '^[0-9a-f]{64}$'
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_hypnogram_checksum_format'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint silver_recordings_hypnogram_checksum_format
            check (
                hypnogram_checksum_sha256
                ~ '^[0-9a-f]{64}$'
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_versioned_identity_unique'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint
                silver_recordings_versioned_identity_unique
            unique (
                source_system,
                source_pair_id,
                input_fingerprint,
                schema_version,
                transform_version,
                config_id
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'silver_recordings_output_location_unique'
          and conrelid = (
              'staging.silver_recordings'::regclass
          )
    ) then
        alter table staging.silver_recordings
            add constraint
                silver_recordings_output_location_unique
            unique (
                silver_bucket,
                silver_output_prefix
            );
    end if;
end
$$;

create index if not exists
    idx_silver_recordings_source_pair
    on staging.silver_recordings(
        source_system,
        source_pair_id
    );

create index if not exists
    idx_silver_recordings_input_fingerprint
    on staging.silver_recordings(
        input_fingerprint
    );

create index if not exists
    idx_silver_recordings_staging_load_run
    on staging.silver_recordings(
        staging_load_run_id
    );

comment on column
    staging.silver_recordings.source_pair_id
is
    'Path-based identity of one logical PSG/Hypnogram pair.';

comment on column
    staging.silver_recordings.input_fingerprint
is
    'Content identity derived from verified PSG and Hypnogram SHA-256 checksums.';

comment on column
    staging.silver_recordings.recording_id
is
    'UUIDv7 of one concrete versioned Silver representation.';
