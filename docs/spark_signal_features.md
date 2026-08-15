# Spark Signal Features

## 1. Scope

Phase 8 adds the high-volume signal-processing path to NeuroSleep.

The relational path remains in PostgreSQL and dbt. Sample-level signal data stays
in MinIO and is processed with Spark through S3A.

Current verified signal scope:

```text
selected recordings:       5
Silver Parquet files:   1,416
Silver signal rows: 116,242,840
selected Silver size:   ~0.698 GiB
```

The Spark input is not a wildcard over all physical Silver signal objects. It is
derived from the current Warehouse selection and the exact object inventory in
each selected Silver `_SUCCESS.json` manifest.

This excludes historical or otherwise unselected physical representations.

## 2. Runtime

The current verified local runtime is:

```text
Python:   3.13.5
PySpark:  4.2.0
Spark:    4.2.0
Java:     21.0.12
Hadoop:   3.5.0
S3A:      hadoop-aws 3.5.0
```

Spark runs locally with `local[*]`. The signal subset is large enough to make
sample-level aggregation and small-file handling meaningful, but not large
enough to justify a multi-node Spark cluster in this project.

## 3. Input selection

The input path is:

```text
warehouse.dim_recording current selection
        |
        v
selected Silver recording_id + silver_output_prefix
        |
        v
exact signals/*.parquet objects from Silver _SUCCESS.json
        |
        v
Spark S3A read from MinIO
```

Current selected recordings:

```text
SC4001E
SC4002E
SC4011E
SC4012E
ST7011J
```

A historical SC4001E representation remains physically present in Silver but is
not part of the current Warehouse selection. It is therefore excluded from Spark
processing without a hard-coded recording allowlist.

## 4. Feature grain

The Gold feature grain is:

```text
one selected recording
+ one selected channel
+ one 30-second signal window
= one feature row
```

The grouping key is:

```text
recording_id
channel_id
epoch_number
```

Signal windows are independent of sleep-stage labels in Phase 8. Sleep-stage and
other Warehouse context are joined later in Phase 9.

This keeps the feature dataset reusable and avoids coupling signal processing to
one label set.

## 5. Window behavior

Normal windows are 30 seconds.

A final partial window is preserved rather than silently discarded. Its expected
sample count is derived from the remaining recording duration and channel sampling
frequency.

Current verified output:

```text
SC4001E: 18,550 rows / 0 partial rows
SC4002E: 19,810 rows / 0 partial rows
SC4011E: 19,614 rows / 0 partial rows
SC4012E: 19,950 rows / 0 partial rows
ST7011J:  5,985 rows / 5 partial rows

total:   83,909 rows / 5 partial rows
```

The five partial rows in ST7011J represent the same final partial time window
across its five channels.

## 6. Feature schema

Identity and lineage:

```text
source_system
dataset_version
collection
recording_key
recording_id
channel_id
channel_position
source_label
normalized_name
sampling_frequency_hz
feature_version
```

Window and validation fields:

```text
epoch_number
window_start_seconds
window_end_seconds
window_duration_seconds
is_partial_window
sample_count
expected_sample_count
sample_coverage_pct
first_sample_index
last_sample_index
first_sample_elapsed_seconds
last_sample_elapsed_seconds
invalid_signal_sample_count
samples_per_full_window
```

Statistical features:

```text
mean
stddev_pop
min
max
peak_to_peak
rms
```

`stddev_pop` is used because each emitted row describes the full set of samples
available for that signal window, not an estimate from a sample of that window.

Phase 8 intentionally stops at descriptive statistical features. It does not add
arbitrary neuroscience thresholds, frequency-band interpretations, or ML labels.

## 7. Gold publication

Gold uses immutable, versioned publication prefixes:

```text
gold/
  physionet/sleep-edfx/<dataset-version>/
    signal_features/
      <collection>/
        <recording-key>/
          schema_version=1.0.0/
            feature_version=1.0.0/
              input_recording_id=<silver-recording-id>/
                data/
                  part-*.snappy.parquet
                _SUCCESS.json
```

The Silver `recording_id` is part of the Gold path. A later selected Silver
representation therefore produces a different Gold prefix instead of overwriting
historical output.

The Gold `_SUCCESS.json` manifest is written last. It records the Gold versions,
row count, partial-window count, Spark version, Silver lineage, source signal
counts, data object path, file size, and ETag.

## 8. Idempotency and recovery

Publication behavior is fail-closed:

```text
empty exact Gold prefix
    -> write

completed valid prefix
    -> skip

incomplete prefix without _SUCCESS.json
    -> delete exact partial prefix
    -> rebuild

prefix with _SUCCESS.json but invalid contract/lineage/object state
    -> fail
    -> do not auto-delete
```

A completed full rerun of the current five recordings produced:

```text
written=0
skipped=5
recovered_objects=0
rows=83,909
```

The recovery smoke also verifies that incomplete synthetic output is removed,
while a completed or invalid completed prefix is protected from automatic
deletion.

## 9. Physical result

Current Gold state:

```text
Parquet data files:       5
_SUCCESS.json manifests:  5
other objects:            0
Gold Parquet size:        4.328 MiB
Gold feature rows:       83,909
```

The five Gold Parquet files replace the 1,416 fragmented Silver signal files as
the compact downstream analytical representation. Silver remains the trusted
sample-level source and is not deleted.

## 10. Commands

Runtime and selected-input reconciliation:

```bash
make spark-smoke
```

Feature math and full selected-signal validation:

```bash
make spark-feature-check
```

Build or safely skip current Gold publications:

```bash
make gold-signal-features
```

Validate completed Gold publications:

```bash
make gold-signal-features-check
```

Exercise Gold recovery and fail-closed behavior on synthetic prefixes:

```bash
make gold-reliability-smoke
```

Run the full Phase 8 regression path:

```bash
make phase8-check
```

For a controlled single-recording run:

```bash
SPARK_SIGNAL_RECORDING_KEYS=SC4001E make gold-signal-features
```

The environment allowlist is an operational testing control. Production logic
does not contain a hard-coded five-recording allowlist.

## 11. Phase boundary

Phase 8 produces validated, versioned, reusable signal features.

It does not yet join those rows to sleep-stage labels or relational analytical
context. That belongs to Phase 9, where the Gold feature identity can be combined
with Warehouse recording/channel/epoch context without recomputing sample-level
features.
