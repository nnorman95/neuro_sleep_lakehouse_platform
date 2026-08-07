
with expected as (

    select
        r.source_system,
        r.dataset_version,
        r.collection,
        r.recording_key,
        r.recording_id as silver_recording_id,
        s.subject_sk
    from {{ source('staging', 'silver_recordings') }} as r
    inner join {{ ref('int_selected_recording_representation') }} as sr
        on sr.source_system = r.source_system
       and sr.dataset_version = r.dataset_version
       and sr.collection = r.collection
       and sr.recording_key = r.recording_key
       and sr.recording_id = r.recording_id
    inner join {{ source('staging', 'silver_recording_contexts') }} as c
        on c.source_system = r.source_system
       and c.dataset_version = r.dataset_version
       and c.collection = r.collection
       and c.recording_key = r.recording_key
    inner join {{ ref('int_selected_metadata_publication') }} as mp
        on mp.source_system = c.source_system
       and mp.dataset_version = c.dataset_version
       and mp.collection = c.collection
       and mp.metadata_input_fingerprint = c.metadata_input_fingerprint
    inner join {{ ref('dim_subject') }} as s
        on s.subject_key = c.subject_key
       and s.source_system = c.source_system
       and s.dataset_version = c.dataset_version
       and s.collection = c.collection
       and s.metadata_input_fingerprint = c.metadata_input_fingerprint
    where r.schema_version = '{{ var('silver_schema_version') }}'
      and r.transform_version = '{{ var('recording_transform_version') }}'
      and c.schema_version = '{{ var('silver_schema_version') }}'
      and c.transform_version = '{{ var('subject_metadata_transform_version') }}'

),

actual as (

    select
        source_system,
        dataset_version,
        collection,
        recording_key,
        silver_recording_id,
        subject_sk
    from {{ ref('dim_recording') }}

),

missing_from_dimension as (

    select *
    from expected

    except

    select *
    from actual

),

unexpected_in_dimension as (

    select *
    from actual

    except

    select *
    from expected

)

select
    'missing_from_dimension' as failure_type,
    *
from missing_from_dimension

union all

select
    'unexpected_in_dimension' as failure_type,
    *
from unexpected_in_dimension
