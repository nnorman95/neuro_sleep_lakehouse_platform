{{ config(materialized='ephemeral') }}

with source_keys as (

    select distinct
        source_system,
        dataset_version,
        collection
    from {{ source('staging', 'silver_subjects') }}

    union

    select distinct
        source_system,
        dataset_version,
        collection
    from {{ source('staging', 'silver_recording_contexts') }}

),

eligible_subject_publications as (

    select
        source_system,
        dataset_version,
        collection,
        metadata_input_fingerprint,
        count(*) as subject_row_count
    from {{ source('staging', 'silver_subjects') }}
    where schema_version = '{{ var('silver_schema_version') }}'
      and transform_version = '{{ var('subject_metadata_transform_version') }}'
    group by
        source_system,
        dataset_version,
        collection,
        metadata_input_fingerprint

),

eligible_context_publications as (

    select
        source_system,
        dataset_version,
        collection,
        metadata_input_fingerprint,
        count(*) as recording_context_row_count
    from {{ source('staging', 'silver_recording_contexts') }}
    where schema_version = '{{ var('silver_schema_version') }}'
      and transform_version = '{{ var('subject_metadata_transform_version') }}'
    group by
        source_system,
        dataset_version,
        collection,
        metadata_input_fingerprint

),

candidate_publications as (

    select
        coalesce(
            s.source_system,
            c.source_system
        ) as source_system,
        coalesce(
            s.dataset_version,
            c.dataset_version
        ) as dataset_version,
        coalesce(
            s.collection,
            c.collection
        ) as collection,
        coalesce(
            s.metadata_input_fingerprint,
            c.metadata_input_fingerprint
        ) as metadata_input_fingerprint,
        coalesce(
            s.subject_row_count,
            0
        ) as subject_row_count,
        coalesce(
            c.recording_context_row_count,
            0
        ) as recording_context_row_count
    from eligible_subject_publications as s
    full outer join eligible_context_publications as c
        on c.source_system = s.source_system
       and c.dataset_version = s.dataset_version
       and c.collection = s.collection
       and c.metadata_input_fingerprint = s.metadata_input_fingerprint

),

candidate_rows as (

    select
        k.source_system,
        k.dataset_version,
        k.collection,
        c.metadata_input_fingerprint,
        coalesce(
            c.subject_row_count,
            0
        ) as subject_row_count,
        coalesce(
            c.recording_context_row_count,
            0
        ) as recording_context_row_count
    from source_keys as k
    left join candidate_publications as c
        on c.source_system = k.source_system
       and c.dataset_version = k.dataset_version
       and c.collection = k.collection

)

select
    source_system,
    dataset_version,
    collection,
    metadata_input_fingerprint,
    subject_row_count,
    recording_context_row_count,
    count(
        metadata_input_fingerprint
    ) over (
        partition by
            source_system,
            dataset_version,
            collection
    ) as eligible_publication_count
from candidate_rows
