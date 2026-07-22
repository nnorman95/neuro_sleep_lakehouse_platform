create table if not exists staging.silver_recordings (
    recording_id uuid primary key,
    source_system text not null,
    psg_bucket text not null,
    psg_object_key text not null,
    hypnogram_bucket text not null,
    hypnogram_object_key text not null,
    recording_start timestamp without time zone not null,
    duration_seconds double precision not null,
    channel_count smallint not null,
    annotation_count integer not null,
    in_range_epoch_count integer not null,
    out_of_range_epoch_count integer not null,
    trailing_overhang_seconds double precision not null,

    constraint silver_recordings_duration_positive
        check (duration_seconds > 0),

    constraint silver_recordings_channel_count_nonnegative
        check (channel_count >= 0),

    constraint silver_recordings_annotation_count_nonnegative
        check (annotation_count >= 0),

    constraint silver_recordings_in_range_epoch_count_nonnegative
        check (in_range_epoch_count >= 0),

    constraint silver_recordings_out_of_range_epoch_count_nonnegative
        check (out_of_range_epoch_count >= 0),

    constraint silver_recordings_trailing_overhang_nonnegative
        check (trailing_overhang_seconds >= 0),

    constraint silver_recordings_source_objects_unique
        unique (
            psg_bucket,
            psg_object_key,
            hypnogram_bucket,
            hypnogram_object_key
        )
);


create table if not exists staging.silver_channels (
    channel_id uuid primary key,
    recording_id uuid not null,
    position smallint not null,
    source_label text not null,
    normalized_name text not null,
    sampling_frequency_hz double precision not null,
    physical_dimension text,
    physical_min double precision not null,
    physical_max double precision not null,
    digital_min integer not null,
    digital_max integer not null,
    samples_per_data_record integer not null,
    prefiltering text,

    constraint silver_channels_recording_fk
        foreign key (recording_id)
        references staging.silver_recordings (recording_id)
        on delete cascade,

    constraint silver_channels_position_positive
        check (position > 0),

    constraint silver_channels_sampling_frequency_positive
        check (sampling_frequency_hz > 0),

    constraint silver_channels_physical_range_valid
        check (physical_max > physical_min),

    constraint silver_channels_digital_range_valid
        check (digital_max > digital_min),

    constraint silver_channels_samples_per_record_positive
        check (samples_per_data_record > 0),

    constraint silver_channels_recording_position_unique
        unique (recording_id, position),

    constraint silver_channels_recording_name_unique
        unique (recording_id, normalized_name)
);


create table if not exists staging.silver_sleep_stage_intervals (
    interval_id uuid primary key,
    recording_id uuid not null,
    source_annotation_index integer not null,
    onset_seconds double precision not null,
    duration_seconds double precision not null,
    end_seconds double precision not null,
    source_label text not null,
    normalized_stage text not null,
    overlap_status text not null,

    constraint silver_sleep_stage_intervals_recording_fk
        foreign key (recording_id)
        references staging.silver_recordings (recording_id)
        on delete cascade,

    constraint silver_sleep_stage_intervals_annotation_index_nonnegative
        check (source_annotation_index >= 0),

    constraint silver_sleep_stage_intervals_onset_nonnegative
        check (onset_seconds >= 0),

    constraint silver_sleep_stage_intervals_duration_positive
        check (duration_seconds > 0),

    constraint silver_sleep_stage_intervals_end_after_onset
        check (end_seconds > onset_seconds),

    constraint silver_sleep_stage_intervals_recording_annotation_unique
        unique (recording_id, source_annotation_index)
);


create table if not exists staging.silver_sleep_stage_epochs (
    epoch_id uuid primary key,
    recording_id uuid not null,
    source_interval_id uuid not null,
    source_annotation_index integer not null,
    epoch_number integer not null,
    start_seconds double precision not null,
    duration_seconds double precision not null,
    end_seconds double precision not null,
    source_label text not null,
    normalized_stage text not null,

    constraint silver_sleep_stage_epochs_recording_fk
        foreign key (recording_id)
        references staging.silver_recordings (recording_id)
        on delete cascade,

    constraint silver_sleep_stage_epochs_interval_fk
        foreign key (source_interval_id)
        references staging.silver_sleep_stage_intervals (interval_id)
        on delete cascade,

    constraint silver_sleep_stage_epochs_annotation_index_nonnegative
        check (source_annotation_index >= 0),

    constraint silver_sleep_stage_epochs_epoch_number_nonnegative
        check (epoch_number >= 0),

    constraint silver_sleep_stage_epochs_start_nonnegative
        check (start_seconds >= 0),

    constraint silver_sleep_stage_epochs_duration_positive
        check (duration_seconds > 0),

    constraint silver_sleep_stage_epochs_end_after_start
        check (end_seconds > start_seconds),

    constraint silver_sleep_stage_epochs_recording_epoch_unique
        unique (recording_id, epoch_number)
);


create index if not exists idx_silver_channels_recording_id
    on staging.silver_channels (recording_id);

create index if not exists idx_silver_sleep_stage_intervals_recording_id
    on staging.silver_sleep_stage_intervals (recording_id);

create index if not exists idx_silver_sleep_stage_epochs_recording_id
    on staging.silver_sleep_stage_epochs (recording_id);

create index if not exists idx_silver_sleep_stage_epochs_stage
    on staging.silver_sleep_stage_epochs (normalized_stage);
