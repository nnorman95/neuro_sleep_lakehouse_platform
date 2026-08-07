
{{ config(materialized='table') }}

with selected_channels as (

    select
        c.channel_id,
        c.recording_id,
        c.position,
        c.source_label,
        c.normalized_name,
        c.sampling_frequency_hz,
        c.physical_dimension,
        c.physical_min,
        c.physical_max,
        c.digital_min,
        c.digital_max,
        c.samples_per_data_record,
        c.prefiltering,
        r.recording_sk,
        sr.loaded_at as recording_loaded_at
    from {{ source('staging', 'silver_channels') }} as c
    inner join {{ ref('dim_recording') }} as r
        on r.silver_recording_id = c.recording_id
    inner join {{ source('staging', 'silver_recordings') }} as sr
        on sr.recording_id = c.recording_id

),

final as (

    select
        {{ warehouse_surrogate_key([
            "'channel'",
            "recording_sk",
            "normalized_name"
        ]) }} as channel_sk,
        recording_sk,
        channel_id as silver_channel_id,
        recording_id as silver_recording_id,
        position,
        source_label,
        normalized_name,
        sampling_frequency_hz,
        physical_dimension,
        physical_min,
        physical_max,
        digital_min,
        digital_max,
        samples_per_data_record,
        prefiltering,
        recording_loaded_at as first_loaded_at,
        recording_loaded_at as last_loaded_at
    from selected_channels

)

select
    channel_sk,
    recording_sk,
    silver_channel_id,
    silver_recording_id,
    position,
    source_label,
    normalized_name,
    sampling_frequency_hz,
    physical_dimension,
    physical_min,
    physical_max,
    digital_min,
    digital_max,
    samples_per_data_record,
    prefiltering,
    first_loaded_at,
    last_loaded_at
from final
