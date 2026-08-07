
with selected_epoch_stages as (

    select distinct
        e.normalized_stage as silver_stage_code
    from {{ source('staging', 'silver_sleep_stage_epochs') }} as e
    inner join {{ ref('dim_recording') }} as r
        on r.silver_recording_id = e.recording_id

)

select
    s.silver_stage_code
from selected_epoch_stages as s
left join {{ ref('dim_sleep_stage') }} as d
    on d.silver_stage_code = s.silver_stage_code
where d.sleep_stage_sk is null
