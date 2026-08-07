
{{ config(materialized='table') }}

select
    sleep_stage_sk,
    silver_stage_code,
    analytical_stage_code
from (
    values
        (1::smallint, 'W'::text,        'W'::text),
        (2::smallint, 'N1'::text,       'N1'::text),
        (3::smallint, 'N2'::text,       'N2'::text),
        (4::smallint, 'N3'::text,       'N3'::text),
        (5::smallint, 'N4'::text,       'N3'::text),
        (6::smallint, 'REM'::text,      'REM'::text),
        (7::smallint, 'UNKNOWN'::text,  'UNKNOWN'::text),
        (8::smallint, 'MOVEMENT'::text, 'MOVEMENT'::text)
) as controlled_stage_mapping(
    sleep_stage_sk,
    silver_stage_code,
    analytical_stage_code
)
