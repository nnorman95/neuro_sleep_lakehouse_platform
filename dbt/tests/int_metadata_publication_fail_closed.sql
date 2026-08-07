with candidates as (

    select *
    from {{ ref('int_metadata_publication_candidates') }}

),

invalid_candidates as (

    select
        source_system,
        dataset_version,
        collection,
        metadata_input_fingerprint,
        subject_row_count,
        recording_context_row_count,
        eligible_publication_count
    from candidates
    where eligible_publication_count <> 1
       or metadata_input_fingerprint is null
       or subject_row_count <= 0
       or recording_context_row_count <= 0

),

empty_guard as (

    select
        cast(null as text) as source_system,
        cast(null as text) as dataset_version,
        cast(null as text) as collection,
        cast(null as text) as metadata_input_fingerprint,
        cast(0 as bigint) as subject_row_count,
        cast(0 as bigint) as recording_context_row_count,
        cast(0 as bigint) as eligible_publication_count
    where not exists (
        select 1
        from candidates
    )

)

select *
from invalid_candidates

union all

select *
from empty_guard
