
select
    channel_sk,
    recording_sk,
    normalized_name
from {{ ref('dim_channel') }}
where channel_sk <> {{ warehouse_surrogate_key([
    "'channel'",
    "recording_sk",
    "normalized_name"
]) }}
