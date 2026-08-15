{{ config(
    materialized='table',
    schema='mart',
    contract={'enforced': true}
) }}

select
    m.recording_sk,
    m.subject_sk,
    m.recording_key,
    m.source_system,
    m.dataset_version,
    m.collection,
    m.night_number,
    m.treatment,
    s.age_years,
    s.sex,
    m.psg_duration_minutes,
    m.annotated_minutes,
    m.scored_minutes,
    m.sleep_minutes,
    m.wake_minutes,
    m.n1_minutes,
    m.n2_minutes,
    m.n3_minutes,
    m.rem_minutes,
    m.unknown_minutes,
    m.movement_minutes,
    m.annotation_coverage_pct,
    m.sleep_pct_of_scored_time,
    m.first_annotated_epoch,
    m.last_annotated_epoch,
    m.first_sleep_epoch,
    m.last_sleep_epoch,
    m.analytical_stage_transition_count,
    (
        m.unannotated_head_seconds
        / 60.0
    )::double precision as unannotated_head_minutes,
    (
        m.unannotated_tail_seconds
        / 60.0
    )::double precision as unannotated_tail_minutes,
    (
        m.unannotated_total_seconds
        / 60.0
    )::double precision as unannotated_total_minutes,
    m.has_unannotated_head,
    m.has_unannotated_tail
from {{ ref('int_recording_sleep_metrics') }} as m
inner join {{ ref('dim_subject') }} as s
    on s.subject_sk = m.subject_sk
