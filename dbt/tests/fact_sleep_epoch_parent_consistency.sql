
select
    f.sleep_epoch_sk,
    f.subject_sk,
    f.recording_sk,
    f.silver_recording_id
from {{ ref('fact_sleep_epoch') }} as f
left join {{ ref('dim_recording') }} as r
    on r.recording_sk = f.recording_sk
   and r.silver_recording_id = f.silver_recording_id
   and r.subject_sk = f.subject_sk
where r.recording_sk is null
