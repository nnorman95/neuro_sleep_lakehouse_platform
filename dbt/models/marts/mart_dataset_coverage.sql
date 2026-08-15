{{ config(
    materialized='table',
    schema='mart',
    contract={'enforced': true}
) }}

select
    source_system,
    dataset_version,
    collection,
    night_number,
    treatment,
    count(distinct subject_sk)::bigint as subject_count,
    count(*)::bigint as recording_count,
    (
        sum(psg_duration_seconds)
        / 3600.0
    )::double precision as psg_hours,
    (
        sum(annotated_seconds)
        / 3600.0
    )::double precision as annotated_hours,
    (
        sum(scored_seconds)
        / 3600.0
    )::double precision as scored_hours,
    (
        sum(sleep_seconds)
        / 3600.0
    )::double precision as sleep_hours,
    (
        sum(wake_seconds)
        / 3600.0
    )::double precision as wake_hours,
    (
        sum(unknown_seconds)
        / 3600.0
    )::double precision as unknown_hours,
    (
        sum(movement_seconds)
        / 3600.0
    )::double precision as movement_hours,
    case
        when sum(psg_duration_seconds) > 0
        then (
            sum(annotated_seconds)
            / sum(psg_duration_seconds)
            * 100.0
        )::double precision
        else null
    end as annotation_coverage_pct,
    count(*) filter (
        where has_unannotated_head
    )::bigint as recordings_with_unannotated_head,
    count(*) filter (
        where has_unannotated_tail
    )::bigint as recordings_with_unannotated_tail
from {{ ref('int_recording_sleep_metrics') }}
group by
    source_system,
    dataset_version,
    collection,
    night_number,
    treatment
