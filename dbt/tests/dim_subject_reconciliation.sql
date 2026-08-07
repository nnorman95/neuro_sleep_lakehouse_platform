with expected as (

    select
        s.subject_key
    from {{ source('staging', 'silver_subjects') }} as s
    inner join {{ ref('int_selected_metadata_publication') }} as p
        on p.source_system = s.source_system
       and p.dataset_version = s.dataset_version
       and p.collection = s.collection
       and p.metadata_input_fingerprint = s.metadata_input_fingerprint
    where s.schema_version = '{{ var('silver_schema_version') }}'
      and s.transform_version = '{{ var('subject_metadata_transform_version') }}'

),

actual as (

    select subject_key
    from {{ ref('dim_subject') }}

),

missing_from_dimension as (

    select subject_key
    from expected

    except

    select subject_key
    from actual

),

unexpected_in_dimension as (

    select subject_key
    from actual

    except

    select subject_key
    from expected

)

select
    'missing_from_dimension' as failure_type,
    subject_key
from missing_from_dimension

union all

select
    'unexpected_in_dimension' as failure_type,
    subject_key
from unexpected_in_dimension
