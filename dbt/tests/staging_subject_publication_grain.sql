select subject_key, metadata_input_fingerprint, count(*) as row_count
from {{ source('staging', 'silver_subjects') }}
group by subject_key, metadata_input_fingerprint
having count(*) > 1
