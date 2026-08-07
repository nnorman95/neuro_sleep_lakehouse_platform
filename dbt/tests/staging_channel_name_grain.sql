select recording_id, normalized_name, count(*) as row_count
from {{ source('staging', 'silver_channels') }}
group by recording_id, normalized_name
having count(*) > 1
