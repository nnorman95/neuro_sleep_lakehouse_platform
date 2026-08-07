# ADR 002: Warehouse Grain and Silver Version Selection

## Status

Accepted; Phase 6 physical build semantics refined by ADR 003.

ADR 003 supersedes the implementation-specific parts of this ADR where they conflict, especially implicit version replacement, cross-model transaction guarantees, deterministic Warehouse keys, and fail-closed current-version selection. The logical grains and identity distinctions established here remain accepted.

## Context

The Silver layer publishes immutable, versioned recording outputs in MinIO.

A logical Sleep-EDF recording is identified by:

```text
source_system
dataset_version
collection
recording_key
```

Examples:

```text
SC4001E
SC4002E
ST7011J
```

A concrete Silver recording representation is identified by `recording_id`.

The same logical recording may be transformed again when the source bytes,
schema version, transform version, or transform configuration changes.
Therefore, one logical recording may have multiple Silver `recording_id`
values over time.

The versioned Silver identity is defined by:

```text
source_system
source_pair_id
input_fingerprint
schema_version
transform_version
config_id
```

The Warehouse Core must support stable analytical joins while retaining the
exact Silver lineage that produced each warehouse row.

The current Phase 6 scope does not require a full historical warehouse with
slowly changing dimensions or simultaneous analytical access to every Silver
version.

## Decision

### Logical subject grain

`warehouse.dim_subject` will contain one row per logical subject.

The logical subject identity is the deterministic Silver `subject_key`,
derived from:

```text
source_system
dataset_version
collection
source_subject_id
```

The warehouse will use a surrogate key for joins while retaining
`subject_key` for lineage and idempotent matching.

Restricted source identifiers will not be exposed broadly through analytical
marts.

### Logical recording grain

`warehouse.dim_recording` will contain one row per logical recording or
recording night.

The logical recording identity is:

```text
source_system
dataset_version
collection
recording_key
```

The table will use a warehouse surrogate key while retaining `recording_key`.

`recording_key` and `recording_id` are not interchangeable:

- `recording_key` identifies the logical recording;
- `recording_id` identifies one concrete versioned Silver representation.

### Current Silver version

For each logical recording, the Warehouse Core will expose one selected
current Silver representation.

The selected version must be:

- complete;
- backed by a valid `_SUCCESS.json` manifest;
- reconciled successfully;
- accepted by the Silver quality gate;
- connected to registered Bronze source files;
- connected to exactly one Silver recording context.

The selected Silver `recording_id` will be retained in the warehouse for
lineage.

### Version replacement

When a newer approved Silver version is selected for an existing logical
recording, the Warehouse load will update the recording and replace dependent
current-state channel and epoch rows in one database transaction.

The replacement must be idempotent:

- loading the same selected Silver version again produces no duplicates;
- a failed replacement leaves the previous complete warehouse state intact;
- dependent rows are not left partially updated.

### Warehouse Core grain

The Phase 6 Warehouse Core will use these grains:

```text
warehouse.dim_subject
one row per logical subject

warehouse.dim_recording
one row per logical recording or recording night

warehouse.dim_channel
one row per channel in the currently selected recording version

warehouse.dim_sleep_stage
one row per normalized sleep-stage code

warehouse.fact_sleep_epoch
one row per 30-second epoch in the selected Silver recording version
```

`warehouse.fact_sleep_epoch` will retain both:

```text
recording_sk
silver_recording_id
```

The expected source-level uniqueness is:

```text
silver_recording_id
+ epoch_number
```

### Required staging inputs

The Warehouse Core requires these Silver metadata landing tables:

```text
staging.silver_subjects
staging.silver_recording_contexts
staging.silver_recordings
staging.silver_channels
staging.silver_sleep_stage_epochs
```

`staging.silver_sleep_stage_intervals` remains available for lineage,
validation, and future analytical requirements, but it is not a separate
Warehouse Core fact.

A recording must match exactly one `recording_context` before it can enter the
subject-aware warehouse.

Missing or ambiguous recording-context matches will block the affected
warehouse load instead of creating orphan analytical rows.

### High-volume signals

Raw signal samples remain in MinIO as Parquet.

The Warehouse Core will store:

- analytical metadata;
- dimensions;
- sleep-epoch facts;
- summaries;
- quality and lineage references;
- MinIO object pointers where needed.

It will not load every signal sample into PostgreSQL.

### Deferred history model

Phase 6 will not create a separate historical table for every Silver recording
version.

Full version history remains available through:

- immutable Silver prefixes in MinIO;
- `_SUCCESS.json` manifests;
- source fingerprints;
- pipeline-run history;
- quality-check history;
- Bronze file lineage.

A dedicated warehouse version-history model may be added later if an
analytical requirement justifies it.

## Consequences

### Positive

- Warehouse joins use stable logical entities.
- Exact Silver lineage remains available.
- Reprocessing does not create duplicate logical recordings.
- Current analytical tables remain simple.
- Version replacement can be tested transactionally.
- High-volume signal data does not overload PostgreSQL.

### Trade-offs

- The Warehouse Core exposes only the selected current version.
- Historical comparisons between Silver versions require MinIO and operational
  lineage until a dedicated history model is added.
- Channel and epoch rows must be replaced when the selected Silver version
  changes.
- Warehouse loading cannot proceed until subject metadata and recording
  contexts are landed in staging.

## Non-goals

This decision does not introduce:

- slowly changing dimensions;
- a warehouse fact for every raw signal sample;
- `warehouse.fact_signal_quality`;
- `warehouse.fact_device_event`;
- Gold or mart models;
- a generic multi-source identity framework beyond the current Sleep-EDF
  requirements.
