select epoch_id, duration_seconds
from {{ source('staging', 'silver_sleep_stage_epochs') }}
where duration_seconds <> 30.0
