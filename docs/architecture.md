# Architecture

## Active source

```text
Sleep-EDF Database Expanded v1.0.0
source_system = physionet_sleep_edf
external access model = open
internal patient-level access policy = restricted
```

## Implemented flow

```text
PhysioNet
  -> remote manifest and checksum inventory
  -> streaming Python Extract
  -> SHA-256 verification
  -> MinIO Bronze
  -> Python / edfio / NumPy / PyArrow
  -> Silver quality gate
  -> MinIO Silver Parquet
  -> _SUCCESS.json + reconciliation
```

Operational metadata is stored in `raw`, `ops`, `quality`, and `governance`
PostgreSQL schemas.

## Current Silver datasets

```text
recordings/part-00000.parquet
channels/part-00000.parquet
sleep_stage_intervals/part-00000.parquet
sleep_stage_epochs/part-00000.parquet
signals/channel=<normalized_name>/part-*.parquet
_SUCCESS.json
```

A full persistent Silver run has been completed for `SC4001E0`. Four cassette
PSG/Hypnogram pairs have been inspected at the Bronze/schema level.

## PostgreSQL analytical path

Initial `staging.silver_*` landing tables exist. The production loader is
intentionally deferred until Silver identity/version lineage is finalized.
Warehouse and mart transformations are planned through dbt after staging is
stable.

## Object storage

```text
bronze      immutable source files
silver      cleaned/versioned Parquet datasets
gold        future curated analytical/ML outputs
quarantine  rejected or diagnostic payloads
logs        persisted log artifacts when needed
```
