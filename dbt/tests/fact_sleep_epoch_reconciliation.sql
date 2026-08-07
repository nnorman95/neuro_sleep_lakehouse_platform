
with expected as (

    select
        {{ warehouse_surrogate_key([
            "'sleep_epoch'",
            "r.recording_sk",
            "e.epoch_number"
        ]) }} as sleep_epoch_sk,
        r.subject_sk,
        r.recording_sk,
        d.sleep_stage_sk,
        e.epoch_id as silver_epoch_id,
        e.recording_id as silver_recording_id,
        e.source_interval_id,
        e.source_annotation_index,
        e.epoch_number,
        e.start_seconds,
        e.duration_seconds,
        e.end_seconds,
        e.source_label,
        e.normalized_stage as silver_stage_code,
        sr.staging_load_run_id,
        sr.loaded_at
    from {{ source('staging', 'silver_sleep_stage_epochs') }} as e
    inner join {{ ref('dim_recording') }} as r
        on r.silver_recording_id = e.recording_id
    inner join {{ ref('dim_sleep_stage') }} as d
        on d.silver_stage_code = e.normalized_stage
    inner join {{ source('staging', 'silver_recordings') }} as sr
        on sr.recording_id = e.recording_id

),

actual as (

    select
        sleep_epoch_sk,
        subject_sk,
        recording_sk,
        sleep_stage_sk,
        silver_epoch_id,
        silver_recording_id,
        source_interval_id,
        source_annotation_index,
        epoch_number,
        start_seconds,
        duration_seconds,
        end_seconds,
        source_label,
        silver_stage_code,
        staging_load_run_id,
        loaded_at
    from {{ ref('fact_sleep_epoch') }}

),

missing_from_fact as (

    select *
    from expected

    except

    select *
    from actual

),

unexpected_in_fact as (

    select *
    from actual

    except

    select *
    from expected

)

select
    'missing_from_fact' as failure_type,
    *
from missing_from_fact

union all

select
    'unexpected_in_fact' as failure_type,
    *
from unexpected_in_fact
