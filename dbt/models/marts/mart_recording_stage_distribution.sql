{{ config(
    materialized='table',
    schema='mart',
    contract={'enforced': true}
) }}

select
    m.recording_sk,
    m.subject_sk,
    r.recording_key,
    r.source_system,
    r.dataset_version,
    r.collection,
    r.night_number,
    r.treatment,
    s.age_years,
    s.sex,
    m.analytical_stage_code,
    m.epoch_count,
    m.duration_minutes,
    case
        when sm.annotated_seconds > 0
        then (
            m.duration_seconds
            / sm.annotated_seconds
            * 100.0
        )::double precision
        else null
    end as pct_of_annotated_time,
    case
        when m.analytical_stage_code in (
            'N1',
            'N2',
            'N3',
            'REM'
        )
         and sm.sleep_seconds > 0
        then (
            m.duration_seconds
            / sm.sleep_seconds
            * 100.0
        )::double precision
        else null
    end as pct_of_sleep_time
from {{ ref('int_recording_stage_metrics') }} as m
inner join {{ ref('int_recording_sleep_metrics') }} as sm
    on sm.recording_sk = m.recording_sk
inner join {{ ref('dim_recording') }} as r
    on r.recording_sk = m.recording_sk
inner join {{ ref('dim_subject') }} as s
    on s.subject_sk = m.subject_sk
