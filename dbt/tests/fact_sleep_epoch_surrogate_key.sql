
select
    sleep_epoch_sk,
    recording_sk,
    epoch_number
from {{ ref('fact_sleep_epoch') }}
where sleep_epoch_sk <> {{ warehouse_surrogate_key([
    "'sleep_epoch'",
    "recording_sk",
    "epoch_number"
]) }}
