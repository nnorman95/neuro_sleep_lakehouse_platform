
{{ config(materialized='table') }}

with selected_representations as (

    select *
    from {{ ref('int_selected_recording_representation') }}

),

selected_metadata_publications as (

    select *
    from {{ ref('int_selected_metadata_publication') }}

),

selected_recordings as (

    select
        r.recording_id,
        r.source_system,
        r.dataset_version,
        r.collection,
        r.recording_key,
        r.recording_start,
        r.duration_seconds,
        r.channel_count,
        r.annotation_count,
        r.in_range_epoch_count,
        r.out_of_range_epoch_count,
        r.trailing_overhang_seconds,
        r.psg_file_id,
        r.hypnogram_file_id,
        r.source_pair_id,
        r.input_fingerprint,
        r.config_id,
        r.schema_version,
        r.transform_version,
        r.psg_checksum_sha256,
        r.hypnogram_checksum_sha256,
        r.silver_bucket,
        r.silver_output_prefix,
        r.staging_load_run_id,
        r.loaded_at
    from {{ source('staging', 'silver_recordings') }} as r
    inner join selected_representations as sr
        on sr.source_system = r.source_system
       and sr.dataset_version = r.dataset_version
       and sr.collection = r.collection
       and sr.recording_key = r.recording_key
       and sr.recording_id = r.recording_id
    where r.schema_version = '{{ var('silver_schema_version') }}'
      and r.transform_version = '{{ var('recording_transform_version') }}'

),

selected_contexts as (

    select
        c.recording_key,
        c.subject_key,
        c.source_system,
        c.dataset_version,
        c.collection,
        c.night_number,
        c.lights_off_seconds,
        c.treatment,
        c.metadata_input_fingerprint,
        c.loaded_at
    from {{ source('staging', 'silver_recording_contexts') }} as c
    inner join selected_metadata_publications as mp
        on mp.source_system = c.source_system
       and mp.dataset_version = c.dataset_version
       and mp.collection = c.collection
       and mp.metadata_input_fingerprint = c.metadata_input_fingerprint
    where c.schema_version = '{{ var('silver_schema_version') }}'
      and c.transform_version = '{{ var('subject_metadata_transform_version') }}'

),

recording_history as (

    select
        source_system,
        dataset_version,
        collection,
        recording_key,
        min(loaded_at) as first_recording_loaded_at
    from {{ source('staging', 'silver_recordings') }}
    group by
        source_system,
        dataset_version,
        collection,
        recording_key

),

context_history as (

    select
        source_system,
        dataset_version,
        collection,
        recording_key,
        min(loaded_at) as first_context_loaded_at
    from {{ source('staging', 'silver_recording_contexts') }}
    group by
        source_system,
        dataset_version,
        collection,
        recording_key

),

resolved as (

    select
        r.*,
        c.subject_key,
        c.night_number,
        c.lights_off_seconds,
        c.treatment,
        c.metadata_input_fingerprint,
        c.loaded_at as context_loaded_at,
        s.subject_sk,
        rh.first_recording_loaded_at,
        ch.first_context_loaded_at
    from selected_recordings as r
    inner join selected_contexts as c
        on c.source_system = r.source_system
       and c.dataset_version = r.dataset_version
       and c.collection = r.collection
       and c.recording_key = r.recording_key
    inner join {{ ref('dim_subject') }} as s
        on s.subject_key = c.subject_key
       and s.source_system = c.source_system
       and s.dataset_version = c.dataset_version
       and s.collection = c.collection
       and s.metadata_input_fingerprint = c.metadata_input_fingerprint
    inner join recording_history as rh
        on rh.source_system = r.source_system
       and rh.dataset_version = r.dataset_version
       and rh.collection = r.collection
       and rh.recording_key = r.recording_key
    inner join context_history as ch
        on ch.source_system = r.source_system
       and ch.dataset_version = r.dataset_version
       and ch.collection = r.collection
       and ch.recording_key = r.recording_key

),

final as (

    select
        {{ warehouse_surrogate_key([
            "'recording'",
            "source_system",
            "dataset_version",
            "collection",
            "recording_key"
        ]) }} as recording_sk,
        recording_key,
        subject_sk,
        source_system,
        dataset_version,
        collection,
        night_number,
        lights_off_seconds,
        treatment,
        recording_id as silver_recording_id,
        recording_start,
        duration_seconds,
        channel_count,
        annotation_count,
        in_range_epoch_count,
        out_of_range_epoch_count,
        trailing_overhang_seconds,
        psg_file_id,
        hypnogram_file_id,
        source_pair_id,
        input_fingerprint,
        config_id,
        schema_version,
        transform_version,
        psg_checksum_sha256,
        hypnogram_checksum_sha256,
        silver_bucket,
        silver_output_prefix,
        staging_load_run_id,
        least(
            first_recording_loaded_at,
            first_context_loaded_at
        ) as first_loaded_at,
        greatest(
            loaded_at,
            context_loaded_at
        ) as last_loaded_at
    from resolved

)

select
    recording_sk,
    recording_key,
    subject_sk,
    source_system,
    dataset_version,
    collection,
    night_number,
    lights_off_seconds,
    treatment,
    silver_recording_id,
    recording_start,
    duration_seconds,
    channel_count,
    annotation_count,
    in_range_epoch_count,
    out_of_range_epoch_count,
    trailing_overhang_seconds,
    psg_file_id,
    hypnogram_file_id,
    source_pair_id,
    input_fingerprint,
    config_id,
    schema_version,
    transform_version,
    psg_checksum_sha256,
    hypnogram_checksum_sha256,
    silver_bucket,
    silver_output_prefix,
    staging_load_run_id,
    first_loaded_at,
    last_loaded_at
from final
