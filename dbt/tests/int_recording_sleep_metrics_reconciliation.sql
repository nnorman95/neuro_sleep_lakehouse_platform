select
    recording_sk,
    annotated_seconds,
    scored_seconds,
    sleep_seconds,
    wake_seconds,
    unknown_seconds,
    movement_seconds,
    annotation_coverage_pct,
    sleep_pct_of_scored_time
from {{ ref('int_recording_sleep_metrics') }}
where abs(
        annotated_seconds
        - (
            scored_seconds
            + unknown_seconds
            + movement_seconds
        )
    ) > 0.000001
   or abs(
        scored_seconds
        - (
            sleep_seconds
            + wake_seconds
        )
    ) > 0.000001
   or annotation_coverage_pct < 0
   or annotation_coverage_pct > 100.000001
   or (
        sleep_pct_of_scored_time is not null
        and (
            sleep_pct_of_scored_time < 0
            or sleep_pct_of_scored_time > 100.000001
        )
   )
   or unannotated_head_seconds < 0
   or unannotated_tail_seconds < 0
   or unannotated_total_seconds < 0
