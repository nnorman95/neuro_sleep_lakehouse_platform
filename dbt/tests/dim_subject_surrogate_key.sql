select
    subject_sk,
    subject_key
from {{ ref('dim_subject') }}
where subject_sk <> {{ warehouse_surrogate_key(["'subject'", "subject_key"]) }}
