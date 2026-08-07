
{{ config(materialized='ephemeral') }}

select
    source_system,
    dataset_version,
    collection,
    recording_key,
    recording_id
from {{ ref('int_recording_representation_candidates') }}
where eligible_representation_count = 1
  and recording_id is not null
