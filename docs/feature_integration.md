# Feature Integration

## 1. Scope

Phase 9 connects the compact Phase 8 Gold signal features to relational
Warehouse context without recomputing sample-level signal data.

```text
Gold signal_features
        +
warehouse.dim_subject
warehouse.dim_recording
warehouse.dim_channel
warehouse.fact_sleep_epoch
warehouse.dim_sleep_stage
        |
        v
Spark feature integration
        |
        v
Gold integrated_signal_features
```

The Phase 8 `signal_features` dataset remains reusable and label-independent.
Phase 9 publishes a separate enriched representation.

## 2. Grain

The integrated dataset keeps the Phase 8 feature grain:

```text
recording_id + channel_id + epoch_number
```

One source Gold feature row must produce exactly one integrated row.

Current verified output:

```text
83,909 integrated rows
```

## 3. Join semantics

Recording and channel context is resolved with the concrete Silver identities
already carried by Gold:

```text
Gold recording_id = Warehouse silver_recording_id
Gold channel_id   = Warehouse silver_channel_id
```

Sleep-stage context uses:

```text
Gold recording_id = fact_sleep_epoch.silver_recording_id
Gold epoch_number = fact_sleep_epoch.epoch_number
```

Recording/channel Warehouse context is required. Sleep-stage context is a left
join because real signal windows can exist without a source hypnogram label.

Current verified reconciliation:

```text
SC4001E   18,550 rows   18,550 labeled      0 unlabeled
SC4002E   19,810 rows   19,810 labeled      0 unlabeled
SC4011E   19,614 rows   19,614 labeled      0 unlabeled
SC4012E   19,950 rows   19,950 labeled      0 unlabeled
ST7011J    5,985 rows    5,460 labeled    525 unlabeled

total      83,909 rows   83,384 labeled    525 unlabeled
```

For `ST7011J`, epochs `1092..1196` have signal windows but no emitted Warehouse
sleep-stage row. Five channels across 105 epochs produce the 525 preserved
unlabeled rows.

No synthetic sleep stage is assigned.

## 4. Integrated context

The enriched rows retain all Phase 8 feature fields and add:

```text
subject_sk
subject_key
age_years
sex

recording_sk
channel_sk
night_number
treatment
lights_off_seconds

sleep_epoch_sk
sleep_stage_sk
silver_epoch_id
sleep_stage_source_label
silver_stage_code
analytical_stage_code
labeled_epoch_start_seconds
labeled_epoch_end_seconds

has_warehouse_context
has_sleep_stage_label
integration_version
```

Sleep-stage fields are nullable only when `has_sleep_stage_label=false`.

## 5. Publication layout

The integrated dataset is a separate immutable Gold representation:

```text
gold/
  physionet/sleep-edfx/<dataset-version>/
    integrated_signal_features/
      <collection>/
        <recording-key>/
          schema_version=1.0.0/
            feature_version=1.0.0/
              integration_version=1.0.0/
                input_recording_id=<silver-recording-id>/
                  warehouse_context_sha256=<sha256>/
                    data/
                      part-*.snappy.parquet
                    _SUCCESS.json
```

`input_recording_id` identifies the selected Silver representation used by the
source Gold feature set.

`warehouse_context_sha256` is a deterministic fingerprint of the Warehouse
recording/channel and sleep-epoch context used for the join. A changed Warehouse
context therefore creates a new immutable integrated representation instead of
overwriting an earlier valid result.

## 6. Lineage

Before Phase 9 publishes an integrated representation, it validates the
corresponding Phase 8 Gold publication.

The integrated success manifest records:

```text
source Gold output prefix
source Gold success object + ETag
source Gold data object + ETag
source Gold row count
source Gold partial-window count

Warehouse context SHA-256
recording/channel context row count
sleep-epoch context row count

Spark version
integrated data object path + size + ETag
integrated row counts and label coverage
```

This keeps the enriched dataset traceable to both sides of the join.

## 7. Validation

Phase 9 validates:

```text
Gold row preservation
unique recording_id + channel_id + epoch_number grain
100% recording/channel Warehouse context resolution
expected labeled row count
expected unlabeled row count
sleep-window timing alignment
nullable sleep-stage semantics
feature version
integration version
physical Parquet read-back
manifest/object inventory
```

Verified totals:

```text
recordings:              5
integrated rows:    83,909
labeled rows:       83,384
unlabeled rows:        525
data files:              5
```

## 8. Idempotency and recovery

Publication behavior is conservative:

```text
empty exact prefix
    -> write

completed valid prefix
    -> skip

incomplete prefix without _SUCCESS.json
    -> remove that exact incomplete prefix
    -> rebuild

prefix with _SUCCESS.json but invalid state
    -> fail closed
    -> do not auto-delete
```

The first complete Phase 9 run wrote all five recordings. The immediate rerun
reported:

```text
written=0
skipped=5
recovered_objects=0
rows=83,909
```

Synthetic reliability smoke tests also verify partial-prefix recovery and
completed-prefix protection.

## 9. Process optimization

Phase 9 does not return to the 116,242,840 Silver sample rows.

```text
Phase 8 reusable Gold:
5 Parquet files
83,909 feature rows

Phase 9:
5 compact Gold feature files
+ Warehouse context
-> 5 integrated Parquet files
-> 83,909 integrated rows
```

The expensive signal aggregation is reused. Signal feature computation and
relational context integration can evolve independently.

## 10. Commands

Validate the join:

```bash
make feature-integration-check
```

Build missing integrated publications or skip completed ones:

```bash
make integrated-signal-features
```

Validate physical integrated publications:

```bash
make integrated-signal-features-check
```

Exercise recovery and fail-closed behavior:

```bash
make integrated-gold-reliability-smoke
```

Run the complete Phase 9 regression:

```bash
make phase9-check
```

## 11. Phase boundary

Phase 9 ends with a validated, versioned analytical feature dataset combining
signal statistics with subject, recording, channel, and optional sleep-stage
context.

Scheduling and dependency orchestration are intentionally left to the next
phase.
