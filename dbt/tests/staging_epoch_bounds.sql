select epoch_id, start_seconds, end_seconds
from {{ source('staging', 'silver_sleep_stage_epochs') }}
where start_seconds < 0
   or end_seconds <= start_seconds
