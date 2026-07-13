# Quality Rules

This document describes the first planned data quality rules for the NeuroSleep Lakehouse Platform.

The goal is simple: bad data should never disappear silently. Every rejected, suspicious, duplicated, late, or contract-breaking record should have a visible reason and a defined handling strategy.

## 1. Quality Principles

The platform should follow these rules:

- Keep raw data unchanged.
- Validate data before promoting it to trusted layers.
- Never silently drop bad records.
- Store rejected records in quarantine with a clear reason.
- Track which source file created the bad record.
- Track which pipeline run detected the issue.
- Separate errors, warnings, and informational checks.
- Prefer explicit rules over hidden assumptions.

## 2. Quality Flow

```text
raw or parsed record
-> validation rule
-> valid record goes forward
-> invalid record goes to quarantine
-> quality result is logged
```

Bad data should be handled like this:

```text
bad record
-> quality.quarantine_records
-> quarantine object storage
-> quality report
```

## 3. Severity Levels

| Severity | Meaning | Example |
|----------|---------|---------|
| error | Record cannot be trusted and should not continue | Missing subject id |
| warning | Record can continue, but the issue should be visible | Optional metadata missing |
| info | Useful observation, not a failure | New source file type detected |

Severity should be consistent. A rule should not be an error in one job and a warning in another unless the reason is documented.

## 4. Handling Actions

| Action | Meaning |
|--------|---------|
| pass | Record is valid and continues |
| warn | Record continues, but warning is logged |
| quarantine | Record is rejected and stored with reason |
| deduplicate | Duplicate is removed or marked using explicit rules |
| backfill | Late data updates previously processed results |
| contract_failure | Schema or column expectation failed |
| map_to_unknown | Unsupported value is mapped to a controlled unknown value |

## 5. Core Error Codes

| Error Code | Severity | Default Action | Meaning |
|------------|----------|----------------|---------|
| missing_subject_id | error | quarantine | Subject id is missing |
| missing_recording_id | error | quarantine | Recording id is missing |
| missing_source_file_id | error | quarantine | Record cannot be linked to a source file |
| unknown_sleep_stage | error | quarantine or map_to_unknown | Sleep stage label is not supported |
| negative_duration | error | quarantine | Duration is below zero |
| zero_duration | error | quarantine | Duration is zero where positive duration is required |
| invalid_timestamp | error | quarantine | Timestamp cannot be parsed |
| end_before_start | error | quarantine | End time is earlier than start time |
| duplicate_file | warning | deduplicate | File checksum already exists |
| duplicate_recording | warning | deduplicate | Same recording appears more than once |
| duplicate_epoch | warning | deduplicate | Same epoch appears more than once |
| missing_channel | warning | warn or quarantine | Expected signal channel is missing |
| schema_drift_extra_column | warning | contract_failure | Unexpected extra column appears |
| schema_drift_missing_column | error | contract_failure | Required column is missing |
| corrupted_metadata | error | quarantine | Metadata file cannot be parsed |
| late_arriving_annotation | warning | backfill | Annotation arrives after recording was processed |
| out_of_order_event | warning | warn or reorder | Device event arrives out of timestamp order |
| duplicate_device_event | warning | deduplicate | Device event id already exists |

## 6. Required Field Rules

Important fields must not be null.

| Table or Layer | Field | Rule |
|----------------|-------|------|
| raw.file_registry | file_id | Not null and unique |
| raw.file_registry | source_system | Not null |
| raw.file_registry | object_key | Not null |
| raw.file_registry | checksum_sha256 | Not null |
| silver_subjects | subject_id | Not null and unique |
| silver_recordings | recording_id | Not null and unique |
| silver_recordings | subject_id | Not null |
| silver_sleep_epochs | epoch_id | Not null and unique |
| silver_sleep_epochs | recording_id | Not null |
| silver_sleep_epochs | sleep_stage | Not null and accepted value |
| quality.quarantine_records | error_code | Not null |
| quality.quarantine_records | raw_payload | Not null when available |

## 7. Referential Integrity Rules

Records should reference existing parent records.

| Child | Parent | Rule |
|-------|--------|------|
| recording.subject_id | subject.subject_id | Recording must belong to an existing subject |
| channel.recording_id | recording.recording_id | Channel must belong to an existing recording |
| sleep_epoch.recording_id | recording.recording_id | Epoch must belong to an existing recording |
| signal_quality.recording_id | recording.recording_id | Quality record must belong to an existing recording |
| quarantine.source_file_id | raw.file_registry.file_id | Quarantine record should link to source file when possible |
| quarantine.pipeline_run_id | ops.pipeline_run.run_id | Quarantine record should link to pipeline run when possible |

If a required parent record is missing, the child record should not be promoted silently.

## 8. Sleep Stage Rules

Allowed standardized sleep stages:

```text
W
N1
N2
N3
REM
UNKNOWN
```

Possible mapping examples:

| Source Value | Standard Value |
|--------------|----------------|
| Wake | W |
| Sleep stage W | W |
| W | W |
| NREM 1 | N1 |
| Sleep stage 1 | N1 |
| N1 | N1 |
| NREM 2 | N2 |
| Sleep stage 2 | N2 |
| N2 | N2 |
| NREM 3 | N3 |
| Sleep stage 3 | N3 |
| N3 | N3 |
| REM sleep | REM |
| Sleep stage R | REM |
| REM | REM |
| Unknown | UNKNOWN |

Unsupported values such as `N5`, `BAD_STAGE`, or empty labels should be handled explicitly.

Default handling:

```text
unknown_sleep_stage -> quarantine
```

Alternative handling, if documented:

```text
unknown_sleep_stage -> map_to_unknown
```

## 9. Time And Duration Rules

Time fields must be valid and logically consistent.

| Rule | Expected Behavior |
|------|-------------------|
| Timestamp must be parseable | Invalid timestamp goes to quarantine |
| End time must be after start time | Invalid row goes to quarantine |
| Duration must be positive | Negative or zero duration goes to quarantine |
| Sleep epoch duration should usually be 30 seconds | Invalid epoch duration is quarantined or flagged |
| Event timestamp must not be null | Invalid device event goes to quarantine |

Example:

```text
epoch_end_time <= epoch_start_time
-> error_code=end_before_start
-> quarantine
```

## 10. Duplicate Rules

Duplicates should be handled by explicit keys.

| Duplicate Type | Detection Key | Default Action |
|----------------|---------------|----------------|
| Source file | checksum_sha256 | deduplicate |
| Recording | source_system + source_recording_id | deduplicate |
| Subject | source_system + source_subject_id | deduplicate |
| Sleep epoch | recording_id + epoch_start_time | deduplicate |
| Device event | event_id | deduplicate |

Duplicate handling should be idempotent. Running the same ingestion twice should not create duplicate trusted records.

## 11. Signal Quality Rules

Signal quality metrics should be bounded and interpretable.

| Field | Rule |
|-------|------|
| missing_ratio | Between 0 and 1 |
| noise_score | Between 0 and 1 when calculated |
| artifact_score | Between 0 and 1 when calculated |
| signal_quality_score | Between 0 and 1 |
| is_usable | Boolean |

Example:

```text
signal_quality_score < 0 or signal_quality_score > 1
-> quarantine or contract failure
```

## 12. Device Event Rules

Device events are planned for the Kafka extension.

Expected fields:

```text
event_id
device_id
subject_id
recording_id
event_type
battery_level
signal_quality
event_timestamp
```

Rules:

| Field | Rule |
|-------|------|
| event_id | Not null and unique |
| device_id | Not null |
| event_timestamp | Not null and parseable |
| battery_level | Between 0 and 100 |
| signal_quality | Between 0 and 1 |
| event_type | Accepted value |

Duplicate event:

```text
duplicate_device_event -> deduplicate
```

Out-of-order event:

```text
out_of_order_event -> warn or reorder
```

## 13. Schema Drift Rules

Schema drift means the incoming data structure changed.

Examples:

```text
new unexpected column
missing required column
changed data type
renamed field
```

Handling:

| Drift Type | Default Action |
|------------|----------------|
| Extra optional column | warning or contract failure |
| Missing required column | contract_failure |
| Changed required type | contract_failure |
| Renamed required field | contract_failure |

Schema drift should be recorded in quality results.

## 14. Late-Arriving Data Rules

Late-arriving data means related data arrives after the first processing pass.

Example:

```text
recording arrives today
annotation arrives tomorrow
```

Default handling:

```text
late_arriving_annotation -> backfill
```

Backfill rules:

- Reprocess only affected recording when possible.
- Avoid duplicating existing epochs.
- Log the backfill in `ops.pipeline_run`.
- Keep lineage showing which run updated the data.

## 15. Quarantine Record Structure

Quarantine table:

```text
quality.quarantine_records
```

Planned columns:

```text
quarantine_id
source_system
source_file_id
record_key
raw_payload
error_code
error_message
severity
detected_at
pipeline_run_id
status
```

Possible statuses:

```text
open
reviewed
fixed
ignored
reprocessed
```

## 16. Quality Check Results Structure

Quality result table:

```text
quality.quality_check_results
```

Planned columns:

```text
check_id
check_name
table_name
severity
status
rows_checked
rows_failed
error_code
pipeline_run_id
checked_at
details
```

Possible statuses:

```text
passed
failed
warning
skipped
```

## 17. Quality Metrics

Useful quality metrics:

```text
files_processed
records_read
records_written
records_quarantined
duplicate_files_detected
duplicate_records_detected
quality_checks_passed
quality_checks_failed
quarantine_rate
```

Example:

```text
quarantine_rate = records_quarantined / records_read
```

These metrics should eventually appear in dashboards or quality reports.

## 18. First Bad Sample Files

The project should include intentionally broken sample files later.

Planned bad samples:

```text
missing_subject_id.csv
duplicate_recording.csv
unknown_sleep_stage.csv
negative_duration.csv
wrong_timestamp.csv
missing_channel.json
corrupted_metadata.json
schema_drift_extra_column.csv
late_arriving_annotation.csv
duplicate_device_event.json
out_of_order_device_events.json
```

Each bad sample should have an expected handling behavior.

## 19. First Quality Checks To Implement

Start simple.

First checks:

```text
file_id is not null
checksum_sha256 is not null
subject_id is not null
recording_id is not null
sleep_stage is accepted value
epoch_duration_sec equals 30
recording duration is positive
recording_end_time is after recording_start_time
signal_quality_score is between 0 and 1
```

These checks are enough for the first quality layer.

## 20. What Should Not Happen

Avoid:

- Dropping bad records without logging.
- Overwriting raw files to fix data.
- Hiding duplicate records.
- Building marts from unvalidated raw data.
- Mixing warnings and errors without rules.
- Letting unknown sleep stages silently enter trusted tables.
- Ignoring schema drift.
- Creating quality reports that are not connected to pipeline runs.

## 21. Current Status

Current status:

```text
Initial quality rules design.
```

This document should be refined after the exact dataset sample is selected and the first raw files are inspected.
