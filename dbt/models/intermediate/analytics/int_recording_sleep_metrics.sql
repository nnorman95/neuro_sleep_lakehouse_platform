{{ config(materialized='ephemeral') }}

with stage_rollup as (

    select
        recording_sk,
        subject_sk,
        sum(epoch_count)::bigint as annotated_epoch_count,
        sum(duration_seconds)::double precision as annotated_seconds,
        sum(
            case
                when analytical_stage_code in (
                    'W',
                    'N1',
                    'N2',
                    'N3',
                    'REM'
                )
                then duration_seconds
                else 0.0
            end
        )::double precision as scored_seconds,
        sum(
            case
                when analytical_stage_code in (
                    'N1',
                    'N2',
                    'N3',
                    'REM'
                )
                then duration_seconds
                else 0.0
            end
        )::double precision as sleep_seconds,
        sum(
            case
                when analytical_stage_code = 'W'
                then duration_seconds
                else 0.0
            end
        )::double precision as wake_seconds,
        sum(
            case
                when analytical_stage_code = 'N1'
                then duration_seconds
                else 0.0
            end
        )::double precision as n1_seconds,
        sum(
            case
                when analytical_stage_code = 'N2'
                then duration_seconds
                else 0.0
            end
        )::double precision as n2_seconds,
        sum(
            case
                when analytical_stage_code = 'N3'
                then duration_seconds
                else 0.0
            end
        )::double precision as n3_seconds,
        sum(
            case
                when analytical_stage_code = 'REM'
                then duration_seconds
                else 0.0
            end
        )::double precision as rem_seconds,
        sum(
            case
                when analytical_stage_code = 'UNKNOWN'
                then duration_seconds
                else 0.0
            end
        )::double precision as unknown_seconds,
        sum(
            case
                when analytical_stage_code = 'MOVEMENT'
                then duration_seconds
                else 0.0
            end
        )::double precision as movement_seconds
    from {{ ref('int_recording_stage_metrics') }}
    group by
        recording_sk,
        subject_sk

),

epoch_sequence as (

    select
        f.recording_sk,
        f.epoch_number,
        f.start_seconds,
        f.end_seconds,
        d.analytical_stage_code,
        lag(
            d.analytical_stage_code
        ) over (
            partition by f.recording_sk
            order by f.epoch_number
        ) as previous_analytical_stage_code
    from {{ ref('fact_sleep_epoch') }} as f
    inner join {{ ref('dim_sleep_stage') }} as d
        on d.sleep_stage_sk = f.sleep_stage_sk

),

timeline_rollup as (

    select
        recording_sk,
        min(epoch_number)::integer as first_annotated_epoch,
        max(epoch_number)::integer as last_annotated_epoch,
        min(start_seconds)::double precision as first_annotated_start_seconds,
        max(end_seconds)::double precision as last_annotated_end_seconds,
        min(epoch_number) filter (
            where analytical_stage_code in (
                'N1',
                'N2',
                'N3',
                'REM'
            )
        )::integer as first_sleep_epoch,
        max(epoch_number) filter (
            where analytical_stage_code in (
                'N1',
                'N2',
                'N3',
                'REM'
            )
        )::integer as last_sleep_epoch,
        count(*) filter (
            where previous_analytical_stage_code is not null
              and analytical_stage_code
                  <> previous_analytical_stage_code
        )::bigint as analytical_stage_transition_count
    from epoch_sequence
    group by recording_sk

)

select
    r.recording_sk,
    r.subject_sk,
    r.recording_key,
    r.source_system,
    r.dataset_version,
    r.collection,
    r.night_number,
    r.treatment,
    r.duration_seconds::double precision as psg_duration_seconds,
    s.annotated_epoch_count,
    s.annotated_seconds,
    s.scored_seconds,
    s.sleep_seconds,
    s.wake_seconds,
    s.n1_seconds,
    s.n2_seconds,
    s.n3_seconds,
    s.rem_seconds,
    s.unknown_seconds,
    s.movement_seconds,
    (
        r.duration_seconds / 60.0
    )::double precision as psg_duration_minutes,
    (
        s.annotated_seconds / 60.0
    )::double precision as annotated_minutes,
    (
        s.scored_seconds / 60.0
    )::double precision as scored_minutes,
    (
        s.sleep_seconds / 60.0
    )::double precision as sleep_minutes,
    (
        s.wake_seconds / 60.0
    )::double precision as wake_minutes,
    (
        s.n1_seconds / 60.0
    )::double precision as n1_minutes,
    (
        s.n2_seconds / 60.0
    )::double precision as n2_minutes,
    (
        s.n3_seconds / 60.0
    )::double precision as n3_minutes,
    (
        s.rem_seconds / 60.0
    )::double precision as rem_minutes,
    (
        s.unknown_seconds / 60.0
    )::double precision as unknown_minutes,
    (
        s.movement_seconds / 60.0
    )::double precision as movement_minutes,
    case
        when r.duration_seconds > 0
        then (
            s.annotated_seconds
            / r.duration_seconds
            * 100.0
        )::double precision
        else null
    end as annotation_coverage_pct,
    case
        when s.scored_seconds > 0
        then (
            s.sleep_seconds
            / s.scored_seconds
            * 100.0
        )::double precision
        else null
    end as sleep_pct_of_scored_time,
    t.first_annotated_epoch,
    t.last_annotated_epoch,
    t.first_annotated_start_seconds,
    t.last_annotated_end_seconds,
    t.first_sleep_epoch,
    t.last_sleep_epoch,
    t.analytical_stage_transition_count,
    greatest(
        t.first_annotated_start_seconds,
        0.0
    )::double precision as unannotated_head_seconds,
    greatest(
        r.duration_seconds
        - t.last_annotated_end_seconds,
        0.0
    )::double precision as unannotated_tail_seconds,
    greatest(
        r.duration_seconds
        - s.annotated_seconds,
        0.0
    )::double precision as unannotated_total_seconds,
    (
        t.first_annotated_start_seconds > 0
    ) as has_unannotated_head,
    (
        t.last_annotated_end_seconds
        < r.duration_seconds
    ) as has_unannotated_tail
from {{ ref('dim_recording') }} as r
inner join stage_rollup as s
    on s.recording_sk = r.recording_sk
   and s.subject_sk = r.subject_sk
inner join timeline_rollup as t
    on t.recording_sk = r.recording_sk
