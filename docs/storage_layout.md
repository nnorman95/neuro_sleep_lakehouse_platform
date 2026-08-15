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

Phase 8 implements reusable 30-second signal features:

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

Phase 9 adds a separate integrated representation:

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

Gold and PostgreSQL `mart` remain different layers:

```text
Gold  object-storage analytical feature datasets
mart  PostgreSQL relational consumption models
```

`input_recording_id` binds both Gold representations to one concrete selected
Silver representation. The integrated path additionally binds to the Warehouse
integration context through `warehouse_context_sha256`.

Success manifests are written only after Parquet read-back validation. A valid
completed prefix is skipped on rerun. An incomplete exact prefix without
`_SUCCESS.json` can be removed and rebuilt. A prefix with a success manifest is
never auto-deleted by recovery logic.

Verified Phase 8 signal-feature state:

```text
5 Parquet data files
5 _SUCCESS.json manifests
0 other objects
83,909 feature rows
4.328 MiB Parquet
```

Verified Phase 9 integrated state:

```text
5 Parquet data files
5 _SUCCESS.json manifests
83,909 integrated rows
83,384 labeled rows
525 unlabeled rows
```
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
