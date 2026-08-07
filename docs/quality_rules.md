# Quality Rules

This document separates implemented quality behavior from future analytical
quality work.

## 1. Principles

- Keep Bronze source objects unchanged.
- Validate before promoting data to a trusted output.
- Never silently drop rejected or suspicious data.
- Preserve source file, object, recording, and pipeline-run lineage.
- Distinguish errors, warnings, informational results, and critical failures.
- Make idempotency and reconciliation testable.
- Keep source-preserving values when analytical mappings are added.
- Do not document future checks as already implemented.

## 2. Severity and Status

Allowed durable quality severities:

```text
info
warning
error
critical
```

Allowed durable quality statuses:

```text
passed
warning
failed
skipped
```

General behavior:

| Severity | Pipeline behavior |
|---|---|
| `info` | record observation and continue |
| `warning` | preserve visibility and continue when explicitly allowed |
| `error` | block the affected trusted publication |
| `critical` | block publication and require operational attention |

A rule may differ between collections only when the source semantics and reason
are documented.

## 3. Implemented Quality Storage

### `quality.quarantine_records`

Stores rejected-record metadata and either:

- a small `raw_payload`; or
- a pointer to a large object in MinIO `quarantine`.

### `quality.quality_check_results`

Stores durable check history with:

```text
quality_result_id
pipeline_run_id
source_system
data_layer
dataset_name
recording_id
record_key
check_name
severity
status
rows_checked
rows_failed
error_code
message
details
checked_at
created_at
```

The table is implemented by migration `026_create_quality_check_results.sql`.
It is not a future placeholder.

`data_layer` accepts both lakehouse and PostgreSQL scopes because checks can
apply to either system.

## 4. Bronze Integrity Rules

Implemented Bronze checks include:

- safe source-relative paths;
- complete PSG/Hypnogram pairing;
- official checksum-manifest membership;
- HTTP success and non-empty payloads;
- exact file size when known;
- official SHA-256 match;
- valid object-storage metadata;
- `raw.file_registry` consistency;
- immutable terminal file-attempt status;
- one active pipeline lock;
- heartbeat liveness;
- cleanup after failure and interruption;
- MinIO/PostgreSQL reconciliation.

Reconciliation statuses:

```text
healthy
missing_in_storage
missing_in_registry
metadata_mismatch
```

No mismatch is silently accepted as healthy.

## 5. Silver Structural Rules

A Silver recording publication requires:

- one valid recording row;
- channel count matching parsed PSG metadata;
- valid channel positions and sampling frequencies;
- valid source annotation intervals;
- valid 30-second emitted epochs;
- consistent `recording_id` across related datasets;
- finite signal values;
- valid sample indexes and chunk boundaries;
- expected Arrow schemas;
- verified Parquet round trips;
- payload size and checksum validation;
- a complete `_SUCCESS.json` manifest;
- successful reconciliation.

Quality errors block `_SUCCESS.json` publication.

## 6. Sleep-Stage Rules

Implemented source-preserving Silver values:

```text
W
N1
N2
N3
N4
REM
UNKNOWN
MOVEMENT
```

Source Stage 3 and Stage 4 remain separate in Silver. An analytical Warehouse
mapping may combine both into analytical `N3`, but the source-normalized value
must remain traceable.

Unsupported empty or unexpected labels are errors unless a documented rule maps
them explicitly to `UNKNOWN`.

`UNKNOWN` and `MOVEMENT` must not silently become ordinary sleep stages.

## 7. Time and Coverage Rules

General rules:

- recording duration must be positive;
- channel sampling frequency must be positive;
- annotation duration must be positive;
- emitted epoch duration must equal 30 seconds;
- emitted epoch number must be unique within one concrete Silver recording;
- source intervals may begin before PSG time zero;
- emitted epoch start positions remain non-negative;
- signal chunks must stay within the requested extraction range.

### Cassette overhang

For the inspected Cassette data, annotation coverage can extend beyond PSG end.

Implemented behavior:

- preserve the original interval;
- calculate trailing overhang;
- count out-of-range epochs;
- emit only epochs inside PSG coverage;
- do not attach out-of-range annotation epochs to signal samples.

### Telemetry undercoverage

Telemetry can have a non-30-second-aligned recording duration and an unannotated
PSG tail.

Implemented behavior:

```text
non-aligned recording duration -> warning
unannotated PSG tail           -> warning
real emitted epoch past PSG    -> error
```

The complete PSG signal is retained. Only real annotation-derived epochs are
emitted.

## 8. Channel Rules

- channel names are normalized but source labels remain available;
- channel position must be positive and unique within a recording;
- sampling frequency must be positive;
- physical unit may be null because the source can omit it;
- physical and digital ranges must remain structurally valid;
- channel metadata is recording-specific and must not be assumed globally
  constant.

Missing optional units can produce a warning without invalidating the recording.

## 9. Subject-Metadata Rules

Implemented rules include:

- collection must be recognized;
- source subject identifier must be present;
- source subject number must be non-negative;
- age must be present and valid for the source workbook;
- sex code must be valid for the collection-specific mapping;
- the same logical subject cannot have conflicting demographics;
- `recording_key` must be present and unique in the normalized publication;
- night number must be positive;
- lights-off seconds must be within one day;
- Telemetry treatment must match the source night context;
- each recording context must resolve to an emitted `subject_key`;
- source workbook lineage must be present.

`subject_key` is deterministic from:

```text
source_system
dataset_version
collection
source_subject_id
```

It is pseudonymous, not guaranteed anonymous.

## 10. Identity and Idempotency Rules

A concrete recording publication is uniquely described by:

```text
source_system
source_pair_id
input_fingerprint
schema_version
transform_version
config_id
```

The output location is unique by:

```text
silver_bucket
silver_output_prefix
```

A matching completed output is skipped. An incomplete output under the same
versioned prefix is removed and rebuilt. A changed fingerprint or transform
identity produces a new representation instead of overwriting a valid old one.

Subject metadata uses its own input fingerprint derived from both source
workbooks and version information.

## 11. Duplicate Rules

Implemented or required keys:

| Dataset | Duplicate identity |
|---|---|
| Bronze object | `bucket + object_key` |
| Source file content check | verified SHA-256 |
| Silver recording version | version-aware identity tuple |
| Silver output location | `silver_bucket + silver_output_prefix` |
| Recording channel | concrete `recording_id + channel position` |
| Sleep epoch | concrete `recording_id + epoch_number` |
| Staged subject publication row | `subject_key + metadata_input_fingerprint` |
| Staged recording context publication row | `source_system + dataset_version + collection + recording_key + metadata_input_fingerprint` |

Warehouse duplicate rules will be finalized with Warehouse DDL.

## 12. Warning and Error Examples

Warnings currently supported include:

- missing optional channel units;
- special source stage labels;
- Cassette trailing annotation overhang;
- Telemetry non-aligned duration;
- Telemetry unannotated PSG tail.

Errors include:

- checksum mismatch;
- incomplete source pair;
- unsafe path;
- invalid channel count;
- unsupported stage label;
- duplicate emitted epoch number;
- emitted epoch beyond PSG coverage;
- malformed Arrow/Parquet schema;
- missing expected Silver object;
- manifest identity mismatch;
- payload checksum mismatch;
- conflicting subject demographics;
- duplicate recording context.

## 13. Phase 6 Staging and Warehouse Quality

Implemented subject-metadata staging checks:

- `_SUCCESS.json` identity and object inventory match;
- file sizes and SHA-256 checksums match the manifest;
- exact Parquet schemas and row counts match;
- source, publication, and Silver lineage are populated;
- every recording context resolves to a staged subject;
- both tables are written in one transaction;
- rerunning the same publication creates no duplicates;
- written and skipped loads finalize `ops.pipeline_run` correctly.

Implemented recording-dataset staging checks:

- only current compatible Silver publications are selected;
- legacy/incompatible publication versions are excluded;
- recording publication manifest and object inventory match;
- object sizes and SHA-256 checksums match the manifest;
- exact Parquet schemas and row counts match;
- logical recording identity comes from canonical Sleep-EDF source classification;
- recording, channel, interval, and epoch relationships remain consistent;
- every staged recording resolves to recording context;
- child rows have no orphan recording or interval relationships;
- signal objects are not loaded into PostgreSQL;
- rerunning the loader creates no duplicates;
- written and skipped loads finalize `ops.pipeline_run` correctly.

Required Warehouse checks:

- one row per documented dimension grain;
- valid surrogate keys;
- no orphan facts;
- accepted analytical sleep-stage values;
- source Stage 3/4 mapping remains traceable;
- epoch duration equals 30 seconds;
- uniqueness of `silver_recording_id + epoch_number`;
- subject and recording relationships are complete for loaded scope;
- idempotent rebuild/load behavior.

## 14. Future Quality Scope

Not implemented yet:

- window-level signal-quality metrics such as missing ratio, noise score, or
  artifact score;
- device-event quality rules;
- Kafka ordering and duplicate-event checks;
- Great Expectations integration;
- intentionally broken-data fixture suite;
- Gold feature-quality rules;
- mart-level aggregate checks.

These remain future scope and must not be represented as current datasets.

## 15. Validation Commands

```bash
make smoke
make reliability-smoke
make silver-smoke
make test
```

Current regression status:

```text
Core:        15/15
Reliability: 17/17
Silver:      24/24
Total:       56/56
```

## 16. What Must Not Happen

- modifying Bronze objects to hide source defects;
- dropping bad rows without traceable handling;
- treating warnings as invisible success;
- publishing `_SUCCESS.json` after a quality error;
- overwriting a valid versioned Silver prefix;
- loading every signal sample into PostgreSQL;
- claiming future signal-quality or device-event datasets are implemented;
- exposing restricted subject identifiers in broad marts by default;
- documenting a constraint that the physical database does not enforce.

## 17. Current Status

Implemented:

```text
Bronze integrity and reconciliation
Quarantine metadata and payload pointers
Silver structural quality gate
Silver warning and error semantics
Durable quality.quality_check_results history
Silver publication and reconciliation checks
Subject metadata validation
Subject metadata staging schema and loader validation
Recording metadata staging schema and loader validation
Interruption and failure cleanup tests
```

Next:

```text
Warehouse Core schema and dimensional-grain checks
Warehouse transformation relationship tests
dbt/SQL tests where dbt adds real value
```
