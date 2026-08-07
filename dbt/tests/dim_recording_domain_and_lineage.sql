
select
    recording_sk,
    recording_key,
    duration_seconds,
    channel_count,
    annotation_count,
    in_range_epoch_count,
    out_of_range_epoch_count,
    trailing_overhang_seconds,
    first_loaded_at,
    last_loaded_at
from {{ ref('dim_recording') }}
where duration_seconds <= 0
   or channel_count <= 0
   or annotation_count < 0
   or in_range_epoch_count < 0
   or out_of_range_epoch_count < 0
   or trailing_overhang_seconds < 0
   or first_loaded_at > last_loaded_at
