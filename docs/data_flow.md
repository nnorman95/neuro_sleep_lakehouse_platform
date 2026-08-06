# Data Flow

## Extract and Bronze

```text
Sleep-EDF / PhysioNet
  -> RECORDS + SHA256SUMS.txt
  -> manifest parsing and sample/full selection
  -> streaming HTTP download
  -> official SHA-256 verification
  -> MinIO Bronze
  -> raw.file_registry + ops.pipeline_run + ops.file_attempt
```

Existing verified objects are skipped or recovered instead of blindly
downloaded again. Bronze reconciliation compares MinIO and PostgreSQL registry
state.

## Bronze to Silver

```text
PSG EDF + Hypnogram EDF
  -> edfio parsing
  -> recording/channel/interval/epoch/signal models
  -> Silver quality gate
  -> PyArrow tables
  -> Parquet + ZSTD
  -> MinIO Silver
  -> _SUCCESS.json + reconciliation
```

Silver preserves source Stage 3 and Stage 4 separately as `N3` and `N4`.

## PostgreSQL analytical path

```text
MinIO Silver metadata
  -> staging.silver_* DDL exists
  -> production loader pending identity/lineage stabilization
  -> dbt warehouse/marts later
```

High-volume signal samples remain in MinIO rather than being loaded row-by-row
into PostgreSQL.
