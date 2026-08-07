
select
    count(*) as actual_row_count
from {{ ref('dim_sleep_stage') }}
having count(*) <> 8
