{{ config(materialized='ephemeral') }}

with analytical_stages as (

    select distinct
        analytical_stage_code
    from {{ ref('dim_sleep_stage') }}

),

stage_aggregates as (

    select
        f.recording_sk,
        d.analytical_stage_code,
        count(*)::bigint as epoch_count,
        sum(f.duration_seconds)::double precision as duration_seconds
    from {{ ref('fact_sleep_epoch') }} as f
    inner join {{ ref('dim_sleep_stage') }} as d
        on d.sleep_stage_sk = f.sleep_stage_sk
    group by
        f.recording_sk,
        d.analytical_stage_code

),

recording_stage_grid as (

    select
        r.recording_sk,
        r.subject_sk,
        s.analytical_stage_code
    from {{ ref('dim_recording') }} as r
    cross join analytical_stages as s

)

select
    g.recording_sk,
    g.subject_sk,
    g.analytical_stage_code,
    coalesce(
        a.epoch_count,
        0::bigint
    )::bigint as epoch_count,
    coalesce(
        a.duration_seconds,
        0.0::double precision
    )::double precision as duration_seconds,
    (
        coalesce(
            a.duration_seconds,
            0.0::double precision
        )
        / 60.0
    )::double precision as duration_minutes
from recording_stage_grid as g
left join stage_aggregates as a
    on a.recording_sk = g.recording_sk
   and a.analytical_stage_code = g.analytical_stage_code
