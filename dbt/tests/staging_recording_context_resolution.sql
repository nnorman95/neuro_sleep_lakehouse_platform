select r.recording_id, r.source_system, r.dataset_version,
       r.collection, r.recording_key
from {{ source('staging', 'silver_recordings') }} r
left join {{ source('staging', 'silver_recording_contexts') }} c
  on c.source_system = r.source_system
 and c.dataset_version = r.dataset_version
 and c.collection = r.collection
 and c.recording_key = r.recording_key
where c.recording_key is null
