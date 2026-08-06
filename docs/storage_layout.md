# Storage Layout

## Buckets

```text
bronze
silver
gold
quarantine
logs
```

Bronze preserves source-relative paths.

Silver uses versioned recording prefixes:

```text
silver/physionet/sleep-edfx/1.0.0/<collection>/<recording>/
  schema_version=<version>/
    transform_version=<version>/
      source_pair_id=<sha256>/
        config_id=<sha256>/
          recordings/part-00000.parquet
          channels/part-00000.parquet
          sleep_stage_intervals/part-00000.parquet
          sleep_stage_epochs/part-00000.parquet
          signals/channel=<normalized_name>/part-*.parquet
          _SUCCESS.json
```

`gold` is reserved for future curated high-volume analytical and ML-ready
Parquet outputs. It is not the same thing as PostgreSQL `mart`.

Large quarantine payloads live in `quarantine`; PostgreSQL stores pointers.
Real EDF and generated Parquet files are excluded from Git.
