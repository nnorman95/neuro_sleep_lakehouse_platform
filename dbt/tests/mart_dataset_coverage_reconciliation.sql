with mart_totals as (

    select
        sum(recording_count)::bigint as recording_count,
        sum(annotated_hours)::double precision as annotated_hours,
        sum(sleep_hours)::double precision as sleep_hours
    from {{ ref('mart_dataset_coverage') }}

),

warehouse_totals as (

    select
        count(*)::bigint as recording_count
    from {{ ref('dim_recording') }}

),

metric_totals as (

    select
        (
            sum(annotated_seconds)
            / 3600.0
        )::double precision as annotated_hours,
        (
            sum(sleep_seconds)
            / 3600.0
        )::double precision as sleep_hours
    from {{ ref('int_recording_sleep_metrics') }}

)

select
    m.recording_count as mart_recording_count,
    w.recording_count as warehouse_recording_count,
    m.annotated_hours as mart_annotated_hours,
    x.annotated_hours as metric_annotated_hours,
    m.sleep_hours as mart_sleep_hours,
    x.sleep_hours as metric_sleep_hours
from mart_totals as m
cross join warehouse_totals as w
cross join metric_totals as x
where m.recording_count <> w.recording_count
   or abs(
        m.annotated_hours
        - x.annotated_hours
    ) > 0.000001
   or abs(
        m.sleep_hours
        - x.sleep_hours
    ) > 0.000001
