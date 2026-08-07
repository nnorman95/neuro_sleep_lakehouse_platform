
select
    source_system,
    dataset_version,
    collection,
    recording_key,
    count(*) as row_count
from {{ ref('dim_recording') }}
group by
    source_system,
    dataset_version,
    collection,
    recording_key
having count(*) <> 1
