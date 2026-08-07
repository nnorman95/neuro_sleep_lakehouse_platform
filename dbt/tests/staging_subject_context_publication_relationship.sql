select c.subject_key, c.metadata_input_fingerprint, c.source_system,
       c.dataset_version, c.collection
from {{ source('staging', 'silver_recording_contexts') }} c
left join {{ source('staging', 'silver_subjects') }} s
  on s.subject_key = c.subject_key
 and s.metadata_input_fingerprint = c.metadata_input_fingerprint
 and s.source_system = c.source_system
 and s.dataset_version = c.dataset_version
 and s.collection = c.collection
where s.subject_key is null
