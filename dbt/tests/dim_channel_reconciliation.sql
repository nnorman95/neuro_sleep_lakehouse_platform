
with expected as (

    select
        r.recording_sk,
        c.channel_id as silver_channel_id,
        c.recording_id as silver_recording_id,
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
        sr.loaded_at as first_loaded_at,
        sr.loaded_at as last_loaded_at
    from {{ source('staging', 'silver_channels') }} as c
    inner join {{ ref('dim_recording') }} as r
        on r.silver_recording_id = c.recording_id
    inner join {{ source('staging', 'silver_recordings') }} as sr
        on sr.recording_id = c.recording_id

),

actual as (

    select
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
    from {{ ref('dim_channel') }}

),

missing_from_dimension as (

    select *
    from expected

    except

    select *
    from actual

),

unexpected_in_dimension as (

    select *
    from actual

    except

    select *
    from expected

)

select
    'missing_from_dimension' as failure_type,
    *
from missing_from_dimension

union all

select
    'unexpected_in_dimension' as failure_type,
    *
from unexpected_in_dimension
