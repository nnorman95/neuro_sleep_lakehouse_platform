
select
    recording_sk,
    recording_key
from {{ ref('dim_recording') }}
where recording_sk <> {{ warehouse_surrogate_key([
    "'recording'",
    "source_system",
    "dataset_version",
    "collection",
    "recording_key"
]) }}
