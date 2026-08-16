# Data Flow

This document follows data from PhysioNet through the relational analytics
path, the Phase 8 Spark/Gold signal-feature path, the Phase 9 feature integration
path, the Phase 10 Airflow control flow, and the Phase 11 Kafka device-event
stream. It focuses on what is implemented today and where the project
deliberately stops.

## 1. Extract and Bronze

```text
Sleep-EDF / PhysioNet
  -> RECORDS + SHA256SUMS.txt
  -> source selection
  -> streaming HTTP download
  -> official SHA-256 verification
  -> MinIO Bronze
  -> raw.file_registry
  -> ops.pipeline_run
  -> ops.file_attempt
```

Existing verified objects are recovered or skipped rather than downloaded again.
On failure or user interruption, the run is finalized, the heartbeat and advisory
lock are released, resources are closed, and unfinished `.part` files are removed.

## 2. Bronze to Silver recordings

```text
Bronze PSG EDF + Hypnogram EDF
  -> complete-pair discovery by recording_key
  -> source-lineage resolution
  -> edfio parsing
  -> recording + channel metadata
  -> source annotation intervals
  -> 30-second epoch expansion
  -> optional chunked signal extraction
  -> Silver quality gate
  -> explicit PyArrow tables
  -> Parquet + ZSTD
  -> verified upload
  -> _SUCCESS.json
  -> reconciliation
  -> durable quality history
```

A recording publication is versioned by:

```text
schema_version
transform_version
source_pair_id
input_fingerprint
config_id
```

A matching completed output is skipped on rerun. An incomplete output under the
same versioned prefix is removed and rebuilt.

### Quality failure path

```text
Silver quality error
  -> quality.quality_check_results
  -> one active quality.quarantine_records incident
  -> Silver publication blocked
```

Repeated failures of the same concrete representation refresh the active
incident. A later successful written or skipped run resolves it. Runtime/network/
database/storage failures remain operational failures and do not create a fake
data-quality incident.

## 3. Bronze to Silver subject metadata

```text
SC-subjects.xls + ST-subjects.xls
  -> Bronze registry lookup
  -> object download + checksum verification
  -> collection-specific parsing
  -> demographic normalization
  -> deterministic subject_key
  -> recording_key / subject_key reconciliation
  -> subjects.parquet
  -> recording_contexts.parquet
  -> _SUCCESS.json
```

Current publication:

```text
100 subjects
197 recording contexts
```

An unchanged publication is skipped on rerun.

## 4. Sleep-stage semantics

Silver preserves source Stage 3 and Stage 4 separately:

```text
Sleep stage 3 -> N3
Sleep stage 4 -> N4
```

The Warehouse reference dimension later maps both to analytical `N3` while still
keeping the source-preserving code available for lineage.

`UNKNOWN` and `MOVEMENT` stay explicit throughout the relational path.

## 5. Timeline and coverage behavior

### Sleep Cassette

Source annotation intervals can extend beyond the PSG end. Silver keeps the
original interval and overhang metrics, but only real 30-second epochs inside the
PSG timeline are emitted.

### Sleep Telemetry

Telemetry can start or end with PSG time that has no source annotation. This is
a warning, not a reason to fabricate epochs.

Examples preserved through Warehouse and marts:

```text
ST7091J first annotated epoch = 1  (30 seconds)
ST7161J first annotated epoch = 14 (420 seconds)
```

An internal gap in the emitted epoch sequence remains an error.

## 6. Analytical cohort expansion

The first full-signal subset contains five recordings:

```text
SC4001E
SC4002E
SC4011E
SC4012E
ST7011J
```

For Phase 7, 13 more recordings were added to the relational analytical cohort.
They were processed with signal extraction disabled because the marts use
metadata and sleep-stage epochs, not raw samples.

The resulting analytical cohort is:

```text
18 recordings
9 represented subjects
110 channels
3,263 annotation intervals
35,710 emitted epochs
```

The original five-recording full-signal subset still contains 116,242,840 signal
rows in MinIO.

## 7. Silver to PostgreSQL staging

Subject metadata path:

```text
subjects.parquet + recording_contexts.parquet + _SUCCESS.json
  -> publication/object/checksum/schema validation
  -> PostgreSQL transaction
  -> staging.silver_subjects: 100
  -> staging.silver_recording_contexts: 197
```

Recording path:

```text
18 current compatible Silver recording publications
  -> reject legacy/incompatible versions
  -> validate _SUCCESS.json + logical identity
  -> verify object sizes + SHA-256
  -> verify exact Parquet schemas + row counts
  -> PostgreSQL transaction
  -> staging.silver_recordings: 18
  -> staging.silver_channels: 110
  -> staging.silver_sleep_stage_intervals: 3,263
  -> staging.silver_sleep_stage_epochs: 35,710
```

Signal Parquet is never downloaded by the staging loader.

Idempotency was checked directly during the expansion:

```text
first run:  13 publications written / 5 skipped / 26,005 rows written
rerun:       0 publications written / 18 skipped / 0 rows written
```

## 8. Staging to Warehouse

```text
PostgreSQL staging
  -> dbt source tests
  -> fail-closed metadata publication selection
  -> fail-closed recording representation selection
  -> deterministic Warehouse surrogate keys
  -> dimensions + sleep-epoch fact
  -> grain / relationship / lineage / reconciliation tests
```

Current Warehouse:

```text
warehouse.dim_subject          100
warehouse.dim_recording         18
warehouse.dim_channel          110
warehouse.dim_sleep_stage        8
warehouse.fact_sleep_epoch  35,710
```

The fact grain is one emitted 30-second epoch per selected recording. Epochs are
not multiplied by channel.

## 9. Warehouse to Phase 7 marts

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

Current physical row counts:

```text
mart.mart_recording_sleep_summary       18
mart.mart_recording_stage_distribution 126
mart.mart_dataset_coverage                6
```

The stage mart emits seven analytical stages for every recording, including
zero-duration stages. The summary mart reports sleep architecture and structural
coverage. The coverage mart reports what material exists by source, collection,
night, and treatment context.

The formulas and grains are documented in
[`analytics_marts.md`](analytics_marts.md).

## 10. Silver signals to Gold features

The signal path starts from the same trusted logical selection used by the
Warehouse rather than from a wildcard over the Silver bucket.

```text
warehouse.dim_recording current selection
  -> silver_recording_id + silver_output_prefix
  -> exact signal objects from Silver _SUCCESS.json
  -> Spark S3A
  -> recording/channel/30-second aggregation
  -> sample coverage + descriptive statistics
  -> pre-publication validation
  -> Gold Parquet
  -> Spark read-back validation
  -> Gold _SUCCESS.json
```

Current input:

```text
5 selected recordings
1,416 Silver Parquet files
116,242,840 signal rows
~0.698 GiB selected Silver signal data
```

Current Gold output:

```text
5 Parquet data files
5 success manifests
83,909 feature rows
5 partial-window rows
4.328 MiB Parquet
```

The output grain is one recording + channel + 30-second signal window. Sleep-stage
labels are deliberately not joined during Phase 8.

Completed exact Gold outputs are skipped before signal recomputation. An
incomplete exact prefix without a success manifest can be recovered automatically.
A completed but invalid prefix fails closed.

## 11. Validation flow

The relational dbt project contains 14 models and 249 data tests. The Phase 7
full build baseline passes all 257 executed model/test nodes.

Phase 8 adds separate high-volume checks:

- Spark runtime and Hadoop/S3A compatibility;
- exact current Silver signal selection;
- complete Spark S3A row reconciliation;
- synthetic full/partial feature math;
- sample count, sample index, timing, coverage, and finite-feature checks;
- Gold manifest and object validation;
- Gold Spark read-back validation;
- idempotent full rerun;
- partial-output recovery and completed-prefix fail-closed smoke tests.

Canonical milestone regressions are:

```bash
make phase8-check
make phase9-check
make phase10-check
```

`phase10-check` runs the Phase 9 regression first, then validates the dependency
contract, Airflow execution image, Compose runtime connectivity, Airflow
foundation behavior, and the eight-task pipeline DAG contract.

## 12. Scale boundary

Low-volume metadata, epochs, dimensions, and relational marts belong in
PostgreSQL.

The 116,242,840 selected signal rows remain Silver Parquet in MinIO. Spark
converts them to 83,909 reusable window-level Gold feature rows without loading
sample-level signals into PostgreSQL.

Spark currently runs with `local[*]`. A multi-node cluster or custom partition
tuning is deferred until scale or measurements justify the additional operational
complexity.

## 13. Lineage boundary

Warehouse surrogate keys are analytical join keys, not replacements for lineage.
The relational path remains traceable through subject, recording, source,
version, and Silver publication identity.

The Gold signal-feature path additionally records:

```text
source_system
dataset_version
collection
recording_key
recording_id
channel_id
feature_version
Gold schema_version
Silver bucket + output_prefix
Silver signal file/row/size counts
Gold data object path + size + ETag
Spark version
```

Phase 9 joins compact signal features to Warehouse labels and context without
losing the identity of the concrete Silver representation that produced them.

## 14. Gold features to integrated Gold

Phase 9 starts from the compact Phase 8 output:

```text
Gold signal_features
  5 Parquet files
  83,909 rows
        |
        +--> Warehouse recording/channel/subject context
        |
        +--> Warehouse sleep-epoch + sleep-stage context
        |
        v
Spark feature integration
        |
        v
Gold integrated_signal_features
  5 Parquet files
  83,909 rows
```

Recording/channel context is required for every feature row. Sleep-stage context
is optional because real signal windows can exist outside source annotation
coverage.

Current reconciliation:

```text
all integrated rows:       83,909
Warehouse context rows:    83,909
sleep-stage labeled rows:  83,384
unlabeled rows:                525
```

The 525 unlabeled rows are `ST7011J` epochs `1092..1196` across five channels.

The integrated publication records validated source-Gold lineage and a
deterministic Warehouse-context SHA-256. This separates reusable signal
computation from relational context integration and avoids re-reading
sample-level Silver data during Phase 9.

## 15. Phase 10 orchestration control flow

Phase 10 does not introduce a second implementation of the pipeline. Airflow
calls the same commands used by the manual workflow and only owns dependency
ordering, retries, bounded parallelism, and run/task state.

```text
extract_bronze
      |
      +----------------------+
      v                      v
build_subject_metadata_    build_recording_silver
silver                      |
      |                     +--------------------+
      v                     v                    v
load_subject_metadata_   load_recording_      build_gold_signal_
staging                  staging              features
      |                     |                    |
      +----------+----------+                    |
                 v                               |
       build_warehouse_and_marts                 |
                 |                               |
                 +---------------+---------------+
                                 v
                 build_integrated_signal_features
```

The branch after Bronze is intentional: subject metadata and recording Silver can
proceed independently. After recording Silver, relational staging and Gold signal
features can also proceed independently. Integrated Gold waits for both the dbt
Warehouse/marts path and the Gold feature path.

The DAG policy is manual execution (`schedule=None`), `catchup=False`,
`max_active_runs=1`, and one retry per task. The local Airflow runtime uses
`LocalExecutor` with parallelism two.

Two consecutive full DAG runs completed with all eight tasks successful. The
second run exercised the existing idempotent/skip behavior, and the full Phase 9
regression passed afterward, confirming that orchestration did not change data
grain or duplicate the existing publications.

## 16. Phase 11 Kafka device-event flow

The streaming source is intentionally separate from the Sleep-EDF batch path:

```text
simulated_bci_device
  -> DeviceEvent contract v1.0.0
  -> Kafka producer
  -> neurosleep.simulated-bci.device-events.v1
  -> consumer validation
```

Valid path:

```text
valid event
  -> ops.kafka_device_event_inbox
  -> event_id deduplication
  -> ingestion-delay classification
  -> late / out-of-order flags
  -> synchronous Kafka offset commit
  -> dbt warehouse.fact_device_event
```

Invalid path:

```text
contract-invalid Kafka message
  -> quality.quarantine_records
  -> raw transport metadata/payload retained
  -> synchronous Kafka offset commit only after quarantine persistence
```

If durable inbox or quarantine persistence fails, the Kafka offset does not
advance. A later replay is therefore expected. Identical valid replays are
idempotent at the inbox through `event_id`.

Current late-event threshold is 60 seconds. A valid forward sequence gap is not
automatically treated as out of order; a later backward sequence and/or event
time is classified explicitly.

See [`kafka_device_events.md`](kafka_device_events.md).

See [`airflow_orchestration.md`](airflow_orchestration.md).
