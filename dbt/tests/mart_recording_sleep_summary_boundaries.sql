select
    recording_sk,
    psg_duration_minutes,
    annotated_minutes,
    annotation_coverage_pct,
    sleep_pct_of_scored_time,
    unannotated_head_minutes,
    unannotated_tail_minutes,
    unannotated_total_minutes
from {{ ref('mart_recording_sleep_summary') }}
where psg_duration_minutes <= 0
   or annotated_minutes < 0
   or annotated_minutes > psg_duration_minutes + 0.000001
   or annotation_coverage_pct < 0
   or annotation_coverage_pct > 100.000001
   or (
        sleep_pct_of_scored_time is not null
        and (
            sleep_pct_of_scored_time < 0
            or sleep_pct_of_scored_time > 100.000001
        )
   )
   or unannotated_head_minutes < 0
   or unannotated_tail_minutes < 0
   or unannotated_total_minutes < 0
   or abs(
        unannotated_total_minutes
        - (
            psg_duration_minutes
            - annotated_minutes
        )
    ) > 0.000001
