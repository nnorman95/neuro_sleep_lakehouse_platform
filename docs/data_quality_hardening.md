# Data Quality Hardening

Phase 12 strengthens existing data-quality boundaries with intentionally broken,
controlled fixtures. It does not introduce a new data layer, a second validation
framework, or a second quarantine path.

The objective is to prove that trusted Silver and staging boundaries fail closed
when files or related publications are structurally or logically inconsistent.

## Scope

Phase 12 implements four focused fixture groups:

```text
Silver schema drift
Silver manifest integrity
Silver publication consistency
Subject-metadata identity
```

The fixtures use temporary local data and call the same validation functions used
by the real Silver and staging paths.

## Silver schema drift

A valid `channels` Parquet baseline is accepted. The same boundary rejects:

```text
missing column
unexpected column
wrong column type
```

The fixture payloads keep their own file size and SHA-256 metadata consistent, so
these cases specifically exercise exact Parquet schema validation.

Expected markers:

```text
silver_schema_drift_valid_baseline=true
silver_schema_drift_missing_column_blocked=true
silver_schema_drift_extra_column_blocked=true
silver_schema_drift_wrong_type_blocked=true
phase12_schema_drift_smoke_status=success
```

## Silver manifest integrity

A valid Parquet payload is paired with deliberately incorrect manifest metadata.
The staging boundary rejects:

```text
file_size_bytes mismatch
checksum_sha256 mismatch
row_count mismatch
```

Expected markers:

```text
silver_manifest_integrity_valid_baseline=true
silver_manifest_integrity_file_size_mismatch_blocked=true
silver_manifest_integrity_checksum_mismatch_blocked=true
silver_manifest_integrity_row_count_mismatch_blocked=true
phase12_manifest_integrity_smoke_status=success
```

## Silver publication consistency

Individually valid tables can still be invalid as one recording publication.
The existing publication validator rejects:

```text
foreign recording_id
duplicate channel_id
epoch referencing an absent interval
declared channel_count inconsistent with actual channel rows
```

Expected markers:

```text
silver_publication_consistency_valid_baseline=true
silver_publication_consistency_foreign_recording_id_blocked=true
silver_publication_consistency_duplicate_channel_id_blocked=true
silver_publication_consistency_orphan_interval_reference_blocked=true
silver_publication_consistency_declared_channel_count_blocked=true
phase12_publication_consistency_smoke_status=success
```

## Subject-metadata identity

The subject-metadata staging boundary rejects:

```text
duplicate subject_key
duplicate logical recording identity
recording context referencing an absent subject
source_system mismatch against the manifest
dataset_version mismatch against the manifest
```

Expected markers:

```text
subject_metadata_identity_valid_baseline=true
subject_metadata_identity_duplicate_subject_key_blocked=true
subject_metadata_identity_duplicate_recording_identity_blocked=true
subject_metadata_identity_missing_subject_reference_blocked=true
subject_metadata_identity_source_system_mismatch_blocked=true
subject_metadata_identity_dataset_version_mismatch_blocked=true
phase12_subject_metadata_identity_smoke_status=success
```

## Canonical commands

Run only the controlled broken-data fixtures:

```bash
make phase12-quality-smoke
```

Run the complete Phase 12 audit:

```bash
make phase12-check
```

The complete audit performs:

```text
1. Python source compilation
2. all four Phase 12 broken-data fixture groups
3. the existing 26-test Silver regression
4. repository diff hygiene
```

A successful audit ends with:

```text
phase12_source_compilation=success
phase12_broken_data_fixtures=success
phase12_silver_quality_regression=success
phase12_diff_hygiene=success
phase12_validation_status=success
```

## Design boundary

Phase 12 deliberately reuses production validation functions rather than copying
business rules into test-only implementations. The fixtures prove existing
behavior; they do not replace the Silver quality gate, staging loaders,
reconciliation, quarantine routing, or dbt tests.

This keeps one source of truth for validation behavior and reduces maintenance
and operational complexity.

## Out of scope

Phase 12 does not add:

- a new data-quality framework;
- scientific signal-quality thresholds;
- new Gold feature-quality semantics;
- a second quarantine store;
- distributed exactly-once processing;
- new Airflow or Kafka business logic.
