
select
    d.source_system,
    d.dataset_version,
    d.collection,
    d.recording_key,
    d.silver_recording_id
from {{ ref('dim_recording') }} as d
left join {{ ref('int_selected_recording_representation') }} as sr
    on sr.source_system = d.source_system
   and sr.dataset_version = d.dataset_version
   and sr.collection = d.collection
   and sr.recording_key = d.recording_key
   and sr.recording_id = d.silver_recording_id
where sr.recording_id is null
