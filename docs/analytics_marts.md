# Analytics Marts

Phase 7 turns the trusted Warehouse Core into a small set of tables that can be
queried directly from SQL, pandas, R, or a BI tool. The marts are intentionally
descriptive: they summarize what is present in the data without adding research
claims or arbitrary quality cutoffs.

## 1. Current analytical cohort

```text
Collections:              sleep-cassette, sleep-telemetry
Recordings:               18
Represented subjects:      9
Channels:                 110
Annotation intervals:   3,263
Sleep-stage epochs:     35,710
Analytical stages:           7
```

The full subject metadata publication still contains 100 subjects and 197
recording contexts. Only 9 subjects are represented by the current 18-recording
analytical cohort.

The cohort is an engineering/research working set. It is not presented as a
statistically representative sample of Sleep-EDF.

## 2. Requirements

Phase 7 was built around seven concrete analytical requirements.

| ID | Requirement | Implemented by |
|---|---|---|
| DA-01 | Show what research material is currently available: subjects, recordings, hours, collections, nights, and treatments. | `mart_dataset_coverage`, `mart_recording_sleep_summary` |
| DA-02 | Provide one correct descriptive sleep-architecture summary per PSG recording. | `mart_recording_sleep_summary` |
| DA-03 | Provide sleep-stage distribution in a long format that works well in SQL, pandas, R, and BI tools. | `mart_recording_stage_distribution` |
| DA-04 | Make structural coverage visible instead of hiding gaps at the start/end of annotation. | `mart_recording_sleep_summary` |
| DA-05 | Keep the research context needed to interpret a row: subject, collection, night, treatment, age, and sex. | recording summary and stage distribution marts |
| DA-06 | Do not introduce research conclusions or arbitrary scientific exclusion thresholds in core models. | all Phase 7 models |
| DA-07 | Let a larger recording cohort flow through without redesigning the schema. | Warehouse-driven marts; current expansion from 5 to 18 recordings |

## 3. Model graph

```text
warehouse.fact_sleep_epoch
        |
        v
int_recording_stage_metrics
        |
        +------------------------------+
        |                              |
        v                              v
mart_recording_stage_distribution   int_recording_sleep_metrics
                                       |
                                       +--> mart_recording_sleep_summary
                                       |
                                       +--> mart_dataset_coverage
```

The two `int_` models are ephemeral dbt models. They keep repeated calculations
in one place without creating extra physical PostgreSQL tables.

## 4. Analytical stage mapping

Silver/Warehouse preserve eight normalized source codes:

```text
W
N1
N2
N3
N4
REM
UNKNOWN
MOVEMENT
```

Phase 7 works with seven analytical stages:

```text
W
N1
N2
N3
REM
UNKNOWN
MOVEMENT
```

The controlled mapping is:

```text
N3 -> analytical N3
N4 -> analytical N3
```

No source information is lost: the original Silver stage is still available in
the Warehouse fact/dimension. `UNKNOWN` and `MOVEMENT` are not silently folded
into normal sleep.

## 5. Shared metric definitions

The marts use these definitions consistently:

```text
annotated time
  all emitted annotation-derived epochs

scored time
  W + N1 + N2 + N3 + REM

sleep time
  N1 + N2 + N3 + REM

wake time
  W
```

### Annotation coverage

```text
annotation_coverage_pct
= annotated_seconds / psg_duration_seconds * 100
```

This is a structural coverage metric. It tells the consumer how much of the PSG
timeline is represented by emitted annotation-derived epochs.

### Sleep percentage of scored time

```text
sleep_pct_of_scored_time
= sleep_seconds / scored_seconds * 100
```

This metric is deliberately **not** named `sleep_efficiency`. Clinical sleep
efficiency normally depends on a defined time-in-bed interval. The current
pipeline does not claim that stronger interpretation.

### Unannotated head/tail

The recording summary also exposes:

```text
first_annotated_epoch
last_annotated_epoch
unannotated_head_minutes
unannotated_tail_minutes
unannotated_total_minutes
has_unannotated_head
has_unannotated_tail
```

For example, the current data preserves:

```text
ST7091J first annotated epoch = 1
ST7161J first annotated epoch = 14
```

These values remain visible instead of being renumbered to start at zero.

### Stage transitions

`analytical_stage_transition_count` counts changes between adjacent analytical
stages. A source transition between `N3` and `N4` does not count as an analytical
transition because both map to analytical `N3`.

## 6. `int_recording_stage_metrics`

**Grain:** one recording + one analytical stage.

The model aggregates `warehouse.fact_sleep_epoch` and emits a complete stage
grid for every current recording. If a stage does not occur, the row still
exists with zero epochs and zero duration.

Current expected size:

```text
18 recordings x 7 analytical stages = 126 rows
```

Main fields:

```text
recording_sk
subject_sk
analytical_stage_code
epoch_count
duration_seconds
duration_minutes
```

## 7. `int_recording_sleep_metrics`

**Grain:** one recording.

This model centralizes recording-level calculations used by more than one mart.
It derives:

- annotated, scored, sleep, wake, N1/N2/N3/REM, unknown, and movement duration;
- PSG duration and annotation coverage;
- sleep percentage of scored time;
- first/last annotated epoch;
- first/last sleep epoch;
- analytical stage-transition count;
- unannotated head/tail/total duration.

No `usable`, `good_recording`, or threshold-based research flag is produced.

## 8. `mart.mart_recording_stage_distribution`

**Grain:** one logical recording + one analytical stage.

Current rows: **126**.

The mart is long-format and carries enough context to filter or group without
rebuilding common joins every time:

```text
recording_sk
subject_sk
recording_key
source_system
dataset_version
collection
night_number
treatment
age_years
sex
analytical_stage_code
epoch_count
duration_minutes
pct_of_annotated_time
pct_of_sleep_time
```

`pct_of_annotated_time` sums to 100% across all seven analytical-stage rows for a
recording.

`pct_of_sleep_time` is populated only for `N1`, `N2`, `N3`, and `REM`. It is
`NULL` for `W`, `UNKNOWN`, and `MOVEMENT` because those stages are outside the
sleep-time denominator.

## 9. `mart.mart_recording_sleep_summary`

**Grain:** one logical recording.

Current rows: **18**.

This is the main descriptive mart for recording-level analysis. It includes:

```text
recording + subject keys
source / dataset / collection
night number / treatment
age / sex
PSG duration
annotated / scored / sleep / wake minutes
N1 / N2 / N3 / REM minutes
unknown / movement minutes
annotation coverage
sleep percentage of scored time
first/last annotated epoch
first/last sleep epoch
analytical stage-transition count
unannotated head/tail/total minutes
```

The model keeps recording context intact. It does not average multiple nights or
placebo/temazepam conditions into one subject-level row.

## 10. `mart.mart_dataset_coverage`

**Grain:**

```text
source_system
+ dataset_version
+ collection
+ night_number
+ treatment
```

Current rows: **6**.

Main metrics:

```text
subject_count
recording_count
psg_hours
annotated_hours
scored_hours
sleep_hours
wake_hours
unknown_hours
movement_hours
annotation_coverage_pct
recordings_with_unannotated_head
recordings_with_unannotated_tail
```

This table answers “what material do we have?” It does not claim that a group is
large enough for inference or that the current cohort represents the source
population.

## 11. Why there is no subject sleep-summary mart yet

A subject-level sleep summary would be easy to create, but it would also be easy
to misuse. Sleep Telemetry subjects can have multiple nights and treatment
conditions. Averaging them immediately would hide context that is still important
at this stage.

For now, recording-level marts keep that context explicit. A subject-level mart
should be added only when a concrete analysis defines how nights and treatments
are meant to be combined.

## 12. Validation

The current dbt project contains:

```text
14 models
249 data tests
257 executed model/test nodes in a full build
```

Current full build result:

```text
PASS=257
WARN=0
ERROR=0
SKIP=0
```

Phase 7 tests cover:

- one row per recording-stage grain;
- complete seven-stage grid per recording;
- recording metric reconciliation;
- stage-percentage reconciliation;
- recording-summary coverage boundaries;
- dataset-coverage grain;
- dataset-level reconciliation back to Warehouse metrics.

Additional validation confirmed:

```text
mart_recording_sleep_summary       = 18 rows
mart_recording_stage_distribution = 126 rows
mart_dataset_coverage               = 6 rows
```

Two consecutive full dbt rebuilds produced the same
`mart_recording_sleep_summary` content checksum during the Phase 7 validation
run.

## 13. Access and interpretation boundary

The recording-level marts contain exact age, sex, night, and treatment context.
They are controlled analytical models, not public anonymous extracts.

`mart_dataset_coverage` is aggregated, but some groups can still contain a small
number of subjects, so broad publication still requires review.

The marts are descriptive engineering outputs. They should not be presented as:

- clinical diagnoses;
- treatment-effect estimates;
- statistically representative population results;
- scientific recording-quality classifications.

## 14. What comes next

Phase 7 intentionally stops before high-volume signal feature engineering. The
next analytical step can read signal Parquet from MinIO, compute compact
window/recording features with a high-volume processing engine, and publish only
versioned features that have a defined grain and lineage.
