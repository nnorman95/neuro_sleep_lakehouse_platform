with expected_stage_count as (

    select
        count(
            distinct analytical_stage_code
        )::bigint as stage_count
    from {{ ref('dim_sleep_stage') }}

),

actual as (

    select
        recording_sk,
        count(*)::bigint as stage_count
    from {{ ref('int_recording_stage_metrics') }}
    group by recording_sk

)

select
    r.recording_sk,
    coalesce(a.stage_count, 0) as actual_stage_count,
    e.stage_count as expected_stage_count
from {{ ref('dim_recording') }} as r
cross join expected_stage_count as e
left join actual as a
    on a.recording_sk = r.recording_sk
where coalesce(a.stage_count, 0) <> e.stage_count
