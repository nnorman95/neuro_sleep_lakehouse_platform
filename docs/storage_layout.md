# Storage Layout

## MinIO buckets

```text
bronze
silver
gold
quarantine
logs
```

## Sleep-EDF Bronze layout

```text
bronze/
  physionet/
    sleep-edfx/
      1.0.0/
        sleep-cassette/
          SC...-PSG.edf
          SC...-Hypnogram.edf
        sleep-telemetry/
          ST...-PSG.edf
          ST...-Hypnogram.edf
        RECORDS
        SC-subjects.xls
        ST-subjects.xls
```

Object keys preserve the source-relative path.

Example:

```text
physionet/sleep-edfx/1.0.0/sleep-cassette/SC4001E0-PSG.edf
```

## Quarantine layout

```text
quarantine/
  source_system/
  pipeline_run_id/
  payload
```

PostgreSQL stores quarantine metadata and object pointers.

## Repository policy

Real source files and generated Parquet files are excluded from
Git through `.gitignore`.
