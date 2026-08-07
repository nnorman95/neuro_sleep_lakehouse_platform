{{ config(materialized='ephemeral') }}

select
    source_system,
    dataset_version,
    collection,
    metadata_input_fingerprint,
    subject_row_count,
    recording_context_row_count
from {{ ref('int_metadata_publication_candidates') }}
where eligible_publication_count = 1
  and metadata_input_fingerprint is not null
  and subject_row_count > 0
  and recording_context_row_count > 0
