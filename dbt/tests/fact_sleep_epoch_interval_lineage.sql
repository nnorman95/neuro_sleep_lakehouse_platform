
select
    f.sleep_epoch_sk,
    f.silver_recording_id,
    f.source_interval_id,
    f.source_annotation_index
from {{ ref('fact_sleep_epoch') }} as f
left join {{ source('staging', 'silver_sleep_stage_intervals') }} as i
    on i.interval_id = f.source_interval_id
   and i.recording_id = f.silver_recording_id
   and i.source_annotation_index = f.source_annotation_index
where i.interval_id is null
