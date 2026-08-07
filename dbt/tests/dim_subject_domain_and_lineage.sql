select
    subject_sk,
    subject_key,
    age_years,
    source_subject_number,
    first_loaded_at,
    last_loaded_at
from {{ ref('dim_subject') }}
where age_years not between 1 and 120
   or source_subject_number < 0
   or first_loaded_at > last_loaded_at
