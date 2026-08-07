select source_system, dataset_version, collection, recording_key,
       metadata_input_fingerprint, count(*) as row_count
from {{ source('staging', 'silver_recording_contexts') }}
group by source_system, dataset_version, collection, recording_key,
         metadata_input_fingerprint
having count(*) > 1
