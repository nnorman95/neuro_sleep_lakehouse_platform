
select
    sleep_epoch_sk,
    epoch_number,
    start_seconds,
    duration_seconds,
    end_seconds,
    staging_load_run_id,
    loaded_at
from {{ ref('fact_sleep_epoch') }}
where epoch_number < 0
   or start_seconds < 0
   or duration_seconds <> 30.0
   or end_seconds <= start_seconds
   or abs(end_seconds - (start_seconds + duration_seconds)) > 0.000000001
