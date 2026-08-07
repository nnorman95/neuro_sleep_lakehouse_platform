
select
    f.sleep_epoch_sk,
    f.sleep_stage_sk,
    f.silver_stage_code
from {{ ref('fact_sleep_epoch') }} as f
left join {{ ref('dim_sleep_stage') }} as d
    on d.sleep_stage_sk = f.sleep_stage_sk
   and d.silver_stage_code = f.silver_stage_code
where d.sleep_stage_sk is null
