{{ config(materialized='ephemeral') }}

with source_keys as (

    select distinct
        source_system,
        dataset_version,
        collection,
        recording_key
    from {{ source('staging', 'silver_recordings') }}

),

eligible_representations as (

    select
        source_system,
        dataset_version,
        collection,
        recording_key,
        recording_id
    from {{ source('staging', 'silver_recordings') }}
    where schema_version = '{{ var('silver_schema_version') }}'
      and transform_version = '{{ var('recording_transform_version') }}'

),

candidate_rows as (

    select
        k.source_system,
        k.dataset_version,
        k.collection,
        k.recording_key,
        r.recording_id
    from source_keys as k
    left join eligible_representations as r
        on r.source_system = k.source_system
       and r.dataset_version = k.dataset_version
       and r.collection = k.collection
       and r.recording_key = k.recording_key

)

select
    source_system,
    dataset_version,
    collection,
    recording_key,
    recording_id,
    count(
        recording_id
    ) over (
        partition by
            source_system,
            dataset_version,
            collection,
            recording_key
    ) as eligible_representation_count
from candidate_rows
