
select
    channel_sk,
    recording_sk,
    position,
    sampling_frequency_hz,
    physical_min,
    physical_max,
    digital_min,
    digital_max,
    samples_per_data_record,
    first_loaded_at,
    last_loaded_at
from {{ ref('dim_channel') }}
where position <= 0
   or sampling_frequency_hz <= 0
   or physical_max <= physical_min
   or digital_max <= digital_min
   or samples_per_data_record <= 0
   or first_loaded_at > last_loaded_at
