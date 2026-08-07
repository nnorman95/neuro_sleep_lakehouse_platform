select
    d.subject_key,
    d.source_system,
    d.dataset_version,
    d.collection,
    d.metadata_input_fingerprint
from {{ ref('dim_subject') }} as d
left join {{ ref('int_selected_metadata_publication') }} as p
    on p.source_system = d.source_system
   and p.dataset_version = d.dataset_version
   and p.collection = d.collection
   and p.metadata_input_fingerprint = d.metadata_input_fingerprint
where p.metadata_input_fingerprint is null
