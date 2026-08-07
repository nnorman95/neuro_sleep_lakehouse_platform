
select
    c.channel_sk,
    c.recording_sk,
    c.silver_channel_id,
    c.silver_recording_id
from {{ ref('dim_channel') }} as c
left join {{ ref('dim_recording') }} as r
    on r.recording_sk = c.recording_sk
   and r.silver_recording_id = c.silver_recording_id
where r.recording_sk is null
