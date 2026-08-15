select
    source_system,
    dataset_version,
    collection,
    night_number,
    treatment,
    count(*) as row_count
from {{ ref('mart_dataset_coverage') }}
group by
    source_system,
    dataset_version,
    collection,
    night_number,
    treatment
having count(*) <> 1
