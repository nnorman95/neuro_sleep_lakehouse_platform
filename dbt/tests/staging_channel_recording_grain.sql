select recording_id, position, count(*) as row_count
from {{ source('staging', 'silver_channels') }}
group by recording_id, position
having count(*) > 1
