select
    recording_sk,
    analytical_stage_code,
    count(*) as row_count
from {{ ref('mart_recording_stage_distribution') }}
group by
    recording_sk,
    analytical_stage_code
having count(*) <> 1
