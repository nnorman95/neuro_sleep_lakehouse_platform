{{ config(materialized='table') }}

with selected_publications as (

    select *
    from {{ ref('int_selected_metadata_publication') }}

),

selected_subjects as (

    select
        s.subject_key,
        s.source_system,
        s.dataset_version,
        s.collection,
        s.age_years,
        s.sex,
        s.source_subject_id,
        s.source_subject_number,
        s.source_bucket,
        s.source_object_key,
        s.metadata_input_fingerprint,
        s.loaded_at
    from {{ source('staging', 'silver_subjects') }} as s
    inner join selected_publications as p
        on p.source_system = s.source_system
       and p.dataset_version = s.dataset_version
       and p.collection = s.collection
       and p.metadata_input_fingerprint = s.metadata_input_fingerprint
    where s.schema_version = '{{ var('silver_schema_version') }}'
      and s.transform_version = '{{ var('subject_metadata_transform_version') }}'

),

subject_history as (

    select
        subject_key,
        min(loaded_at) as first_loaded_at
    from {{ source('staging', 'silver_subjects') }}
    group by subject_key

),

final as (

    select
        {{ warehouse_surrogate_key(["'subject'", "s.subject_key"]) }} as subject_sk,
        s.subject_key,
        s.source_system,
        s.dataset_version,
        s.collection,
        s.age_years,
        s.sex,
        s.source_subject_id,
        s.source_subject_number,
        s.source_bucket,
        s.source_object_key,
        s.metadata_input_fingerprint,
        h.first_loaded_at,
        s.loaded_at as last_loaded_at
    from selected_subjects as s
    inner join subject_history as h
        on h.subject_key = s.subject_key

)

select
    subject_sk,
    subject_key,
    source_system,
    dataset_version,
    collection,
    age_years,
    sex,
    source_subject_id,
    source_subject_number,
    source_bucket,
    source_object_key,
    metadata_input_fingerprint,
    first_loaded_at,
    last_loaded_at
from final
