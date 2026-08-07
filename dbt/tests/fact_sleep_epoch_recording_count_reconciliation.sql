
with fact_counts as (

    select
        recording_sk,
        count(*) as fact_epoch_count
    from {{ ref('fact_sleep_epoch') }}
    group by recording_sk

),

selected_epoch_counts as (

    select
        r.recording_sk,
        count(e.epoch_id) as selected_epoch_count
    from {{ ref('dim_recording') }} as r
    left join {{ source('staging', 'silver_sleep_stage_epochs') }} as e
        on e.recording_id = r.silver_recording_id
    group by r.recording_sk

)

select
    r.recording_sk,
    r.recording_key,
    r.in_range_epoch_count,
    s.selected_epoch_count,
    coalesce(f.fact_epoch_count, 0) as fact_epoch_count
from {{ ref('dim_recording') }} as r
inner join selected_epoch_counts as s
    on s.recording_sk = r.recording_sk
left join fact_counts as f
    on f.recording_sk = r.recording_sk
where r.in_range_epoch_count <> s.selected_epoch_count
   or s.selected_epoch_count <> coalesce(f.fact_epoch_count, 0)
