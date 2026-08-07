
with actual_counts as (

    select
        recording_sk,
        count(*) as actual_channel_count
    from {{ ref('dim_channel') }}
    group by recording_sk

)

select
    r.recording_sk,
    r.recording_key,
    r.channel_count as expected_channel_count,
    coalesce(a.actual_channel_count, 0) as actual_channel_count
from {{ ref('dim_recording') }} as r
left join actual_counts as a
    on a.recording_sk = r.recording_sk
where r.channel_count <> coalesce(a.actual_channel_count, 0)
