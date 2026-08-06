create table if not exists staging.silver_subjects (
    subject_key text not null,
    source_system text not null,
    dataset_version text not null,
    collection text not null,
    source_subject_id text not null,
    source_subject_number smallint not null,
    age_years smallint not null,
    sex text not null,
    source_bucket text not null,
    source_object_key text not null,
    metadata_input_fingerprint text not null,
    schema_version text not null,
    transform_version text not null,
    silver_bucket text not null,
    silver_output_prefix text not null,
    staging_load_run_id uuid not null,
    loaded_at timestamptz not null default now(),

    constraint silver_subjects_pkey
        primary key (
            subject_key,
            metadata_input_fingerprint
        ),

    constraint silver_subjects_staging_load_run_fk
        foreign key (staging_load_run_id)
        references ops.pipeline_run(run_id),

    constraint silver_subjects_subject_key_format
        check (
            subject_key ~ '^[0-9a-f]{64}$'
        ),

    constraint silver_subjects_input_fingerprint_format
        check (
            metadata_input_fingerprint
            ~ '^[0-9a-f]{64}$'
        ),

    constraint silver_subjects_collection_check
        check (
            collection in (
                'sleep-cassette',
                'sleep-telemetry'
            )
        ),

    constraint silver_subjects_subject_number_nonnegative
        check (source_subject_number >= 0),

    constraint silver_subjects_age_range
        check (
            age_years between 1 and 120
        ),

    constraint silver_subjects_sex_check
        check (
            sex in ('F', 'M')
        ),

    constraint silver_subjects_publication_identity_unique
        unique (
            subject_key,
            metadata_input_fingerprint,
            source_system,
            dataset_version,
            collection
        ),

    constraint silver_subjects_source_identity_unique
        unique (
            source_system,
            dataset_version,
            collection,
            source_subject_id,
            metadata_input_fingerprint
        ),

    constraint silver_subjects_output_row_unique
        unique (
            silver_bucket,
            silver_output_prefix,
            subject_key
        )
);

create table if not exists
    staging.silver_recording_contexts (
    recording_key text not null,
    subject_key text not null,
    source_system text not null,
    dataset_version text not null,
    collection text not null,
    night_number smallint not null,
    lights_off_seconds integer not null,
    treatment text,
    source_bucket text not null,
    source_object_key text not null,
    metadata_input_fingerprint text not null,
    schema_version text not null,
    transform_version text not null,
    silver_bucket text not null,
    silver_output_prefix text not null,
    staging_load_run_id uuid not null,
    loaded_at timestamptz not null default now(),

    constraint silver_recording_contexts_pkey
        primary key (
            source_system,
            dataset_version,
            collection,
            recording_key,
            metadata_input_fingerprint
        ),

    constraint silver_recording_contexts_subject_fk
        foreign key (
            subject_key,
            metadata_input_fingerprint,
            source_system,
            dataset_version,
            collection
        )
        references staging.silver_subjects (
            subject_key,
            metadata_input_fingerprint,
            source_system,
            dataset_version,
            collection
        )
        on delete cascade,

    constraint silver_recording_contexts_staging_load_run_fk
        foreign key (staging_load_run_id)
        references ops.pipeline_run(run_id),

    constraint silver_recording_contexts_subject_key_format
        check (
            subject_key ~ '^[0-9a-f]{64}$'
        ),

    constraint silver_recording_contexts_input_fingerprint_format
        check (
            metadata_input_fingerprint
            ~ '^[0-9a-f]{64}$'
        ),

    constraint silver_recording_contexts_collection_check
        check (
            collection in (
                'sleep-cassette',
                'sleep-telemetry'
            )
        ),

    constraint silver_recording_contexts_night_positive
        check (night_number > 0),

    constraint silver_recording_contexts_lights_off_range
        check (
            lights_off_seconds >= 0
            and lights_off_seconds < 86400
        ),

    constraint silver_recording_contexts_treatment_check
        check (
            (
                collection = 'sleep-cassette'
                and treatment is null
            )
            or
            (
                collection = 'sleep-telemetry'
                and treatment in (
                    'placebo',
                    'temazepam'
                )
            )
        ),

    constraint silver_recording_contexts_output_row_unique
        unique (
            silver_bucket,
            silver_output_prefix,
            recording_key
        )
);

create index if not exists
    idx_silver_subjects_metadata_fingerprint
    on staging.silver_subjects (
        metadata_input_fingerprint
    );

create index if not exists
    idx_silver_subjects_staging_load_run
    on staging.silver_subjects (
        staging_load_run_id
    );

create index if not exists
    idx_silver_recording_contexts_subject_publication
    on staging.silver_recording_contexts (
        subject_key,
        metadata_input_fingerprint
    );

create index if not exists
    idx_silver_recording_contexts_logical_recording
    on staging.silver_recording_contexts (
        source_system,
        dataset_version,
        collection,
        recording_key
    );

create index if not exists
    idx_silver_recording_contexts_metadata_fingerprint
    on staging.silver_recording_contexts (
        metadata_input_fingerprint
    );

create index if not exists
    idx_silver_recording_contexts_staging_load_run
    on staging.silver_recording_contexts (
        staging_load_run_id
    );

comment on table staging.silver_subjects is
    'Version-aware landing table for normalized Silver subject metadata.';

comment on column staging.silver_subjects.subject_key is
    'Deterministic SHA-256 logical subject identity from the Silver dataset.';

comment on column
    staging.silver_subjects.metadata_input_fingerprint
is
    'SHA-256 identity of one concrete Silver subject-metadata publication.';

comment on table staging.silver_recording_contexts is
    'Version-aware landing table for Silver recording-to-subject context.';

comment on column
    staging.silver_recording_contexts.recording_key
is
    'Logical Sleep-EDF recording or recording-night identifier.';

comment on column
    staging.silver_recording_contexts.metadata_input_fingerprint
is
    'SHA-256 identity of one concrete Silver subject-metadata publication.';
