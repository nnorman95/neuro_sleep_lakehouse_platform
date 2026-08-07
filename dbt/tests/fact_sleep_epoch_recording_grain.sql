
select
    recording_sk,
    epoch_number,
    count(*) as row_count
from {{ ref('fact_sleep_epoch') }}
group by
    recording_sk,
    epoch_number
having count(*) <> 1
