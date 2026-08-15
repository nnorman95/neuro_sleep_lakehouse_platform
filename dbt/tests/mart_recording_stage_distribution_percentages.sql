with per_recording as (

    select
        recording_sk,
        sum(pct_of_annotated_time)::double precision
            as annotated_pct_sum,
        sum(pct_of_sleep_time) filter (
            where analytical_stage_code in (
                'N1',
                'N2',
                'N3',
                'REM'
            )
        )::double precision as sleep_pct_sum,
        count(*) filter (
            where analytical_stage_code not in (
                'N1',
                'N2',
                'N3',
                'REM'
            )
              and pct_of_sleep_time is not null
        )::bigint as invalid_non_sleep_pct_rows
    from {{ ref('mart_recording_stage_distribution') }}
    group by recording_sk

)

select
    recording_sk,
    annotated_pct_sum,
    sleep_pct_sum,
    invalid_non_sleep_pct_rows
from per_recording
where abs(annotated_pct_sum - 100.0) > 0.000001
   or abs(sleep_pct_sum - 100.0) > 0.000001
   or invalid_non_sleep_pct_rows <> 0
