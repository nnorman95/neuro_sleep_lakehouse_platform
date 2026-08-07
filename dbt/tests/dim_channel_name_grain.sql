
select
    recording_sk,
    normalized_name,
    count(*) as row_count
from {{ ref('dim_channel') }}
group by
    recording_sk,
    normalized_name
having count(*) <> 1
