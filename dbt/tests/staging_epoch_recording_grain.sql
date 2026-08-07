select recording_id, epoch_number, count(*) as row_count
from {{ source('staging', 'silver_sleep_stage_epochs') }}
group by recording_id, epoch_number
having count(*) > 1
