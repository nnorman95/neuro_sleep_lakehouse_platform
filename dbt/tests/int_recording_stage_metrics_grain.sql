select
    recording_sk,
    analytical_stage_code,
    count(*) as row_count
from {{ ref('int_recording_stage_metrics') }}
group by
    recording_sk,
    analytical_stage_code
having count(*) <> 1
