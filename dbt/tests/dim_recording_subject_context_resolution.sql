
with selected_contexts as (

    select
        c.source_system,
        c.dataset_version,
        c.collection,
        c.recording_key,
        c.subject_key,
        c.metadata_input_fingerprint
    from {{ source('staging', 'silver_recording_contexts') }} as c
    inner join {{ ref('int_selected_metadata_publication') }} as mp
        on mp.source_system = c.source_system
       and mp.dataset_version = c.dataset_version
       and mp.collection = c.collection
       and mp.metadata_input_fingerprint = c.metadata_input_fingerprint
    where c.schema_version = '{{ var('silver_schema_version') }}'
      and c.transform_version = '{{ var('subject_metadata_transform_version') }}'

)

select
    d.recording_sk,
    d.recording_key,
    d.subject_sk
from {{ ref('dim_recording') }} as d
left join selected_contexts as c
    on c.source_system = d.source_system
   and c.dataset_version = d.dataset_version
   and c.collection = d.collection
   and c.recording_key = d.recording_key
left join {{ ref('dim_subject') }} as s
    on s.subject_key = c.subject_key
   and s.source_system = c.source_system
   and s.dataset_version = c.dataset_version
   and s.collection = c.collection
   and s.metadata_input_fingerprint = c.metadata_input_fingerprint
where c.recording_key is null
   or s.subject_sk is null
   or d.subject_sk <> s.subject_sk
