select recording_id, source_annotation_index, count(*) as row_count
from {{ source('staging', 'silver_sleep_stage_intervals') }}
group by recording_id, source_annotation_index
having count(*) > 1
