
select
    silver_recording_id,
    epoch_number,
    count(*) as row_count
from {{ ref('fact_sleep_epoch') }}
group by
    silver_recording_id,
    epoch_number
having count(*) <> 1
