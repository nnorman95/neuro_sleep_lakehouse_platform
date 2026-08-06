# Storage Layout

## 1. MinIO Buckets

```text
bronze
silver
gold
quarantine
logs
```

## 2. Bronze Layout

Bronze preserves source-relative paths:

```text
bronze/
  physionet/
    sleep-edfx/
      1.0.0/
        RECORDS
        SHA256SUMS.txt
        SC-subjects.xls
        ST-subjects.xls
        sleep-cassette/
          *-PSG.edf
          *-Hypnogram.edf
        sleep-telemetry/
          *-PSG.edf
          *-Hypnogram.edf
```

Bronze is source-preserving. Source files are not overwritten to apply cleaning
or normalization.

## 3. Silver Recording Layout

A logical recording root is derived from the PSG object key without the
`-PSG.edf` suffix.

```text
silver/
  physionet/sleep-edfx/1.0.0/<collection>/<recording-root>/
    schema_version=<schema-version>/
      transform_version=<transform-version>/
        source_pair_id=<sha256>/
          input_fingerprint=<sha256>/
            config_id=<sha256>/
              recordings/part-00000.parquet
              channels/part-00000.parquet
              sleep_stage_intervals/part-00000.parquet
              sleep_stage_epochs/part-00000.parquet
              signals/channel=<normalized-name>/part-*.parquet
              _SUCCESS.json
```

The current recording transform version is `1.1.0`.

The versioned path allows multiple valid Silver representations of one logical
source pair to coexist.

## 4. Silver Subject-Metadata Layout

```text
silver/
  physionet/sleep-edfx/1.0.0/metadata/
    schema_version=1.0.0/
      transform_version=1.0.0/
        input_fingerprint=<sha256>/
          subjects.parquet
          recording_contexts.parquet
          _SUCCESS.json
```

The metadata fingerprint includes the verified source workbooks and transform
versions.

## 5. Success Manifests

`_SUCCESS.json` is part of the published dataset contract. It records enough
information to validate completed output, including expected data objects,
counts, checksums, versions, identity values, and source lineage.

A prefix without a valid success manifest is not treated as complete. The
pipeline may delete incomplete objects under that exact versioned prefix and
rebuild them. A completed valid prefix is preserved and skipped on rerun.

## 6. Gold Layout

`gold` is reserved for future curated high-volume analytical and ML-ready
Parquet outputs.

Gold is not the same thing as PostgreSQL `mart`:

```text
Gold  object-storage analytical datasets
mart  PostgreSQL consumption models
```

Neither should be built before Warehouse Core semantics are stable.

## 7. Quarantine Layout

Large rejected or diagnostic payloads are stored in `quarantine`. PostgreSQL
stores their object pointers, sizes, checksums, reason, severity, and run
lineage.

## 8. Logs Layout

The `logs` bucket is reserved for persisted structured-log artifacts when
needed. Console and structured-log timestamps use UTC.

## 9. Git Boundary

The repository may contain only code, configuration examples, contracts, and
documentation.

The following remain local and ignored:

- EDF and XLS source data;
- generated Parquet;
- `_SUCCESS.json` runtime objects;
- quarantine payloads;
- runtime logs;
- `.env` and secrets;
- temporary `.part` files.
