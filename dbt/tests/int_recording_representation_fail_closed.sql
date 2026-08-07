with candidates as (

    select *
    from {{ ref('int_recording_representation_candidates') }}

),

invalid_candidates as (

    select
        source_system,
        dataset_version,
        collection,
        recording_key,
        recording_id,
        eligible_representation_count
    from candidates
    where eligible_representation_count <> 1
       or recording_id is null

),

empty_guard as (

    select
        cast(null as text) as source_system,
        cast(null as text) as dataset_version,
        cast(null as text) as collection,
        cast(null as text) as recording_key,
        cast(null as uuid) as recording_id,
        cast(0 as bigint) as eligible_representation_count
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
