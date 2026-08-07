
select
    recording_sk,
    position,
    count(*) as row_count
from {{ ref('dim_channel') }}
group by
    recording_sk,
    position
having count(*) <> 1
