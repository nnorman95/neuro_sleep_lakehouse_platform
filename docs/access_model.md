# Access Model

This document separates upstream source accessibility from internal data access.

## 1. External Source Access

Sleep-EDF Database Expanded is an open-access PhysioNet dataset.

```text
source_system = physionet_sleep_edf
access_model = open
credential_required = false
```

These fields describe how the upstream data is obtained. They do not determine
who should access subject-level data inside the platform.

## 2. Internal Platform Policy

```text
patient-level Bronze data       restricted
patient-level Silver data       restricted
subject-aware staging data      restricted
subject-aware Warehouse data    restricted
operational metadata            team_only
aggregated non-identifying marts review_before_broad_access
public repository               code, config examples, and documentation only
```

Real EDF, XLS, Parquet, quarantine payloads, credentials, and runtime logs are
not committed to Git.

## 3. Why Open Data Is Still Restricted Internally

The source is public, but combinations such as the following remain
quasi-identifying:

```text
source_subject_id
source_subject_number
age_years
sex
collection
night_number
treatment
recording timing
```

The project therefore uses:

```text
access_model = open
access_policy = restricted
```

The fields intentionally describe different concepts.

## 4. Identifier Policy

### Default analytical identifiers

Controlled analytical models should prefer:

```text
subject_sk
recording_sk
recording_key
```

`subject_key` remains available for controlled stable joins but is not exposed in
the current Phase 7 marts.

### Restricted lineage identifiers

These should not be exposed in broad marts unless a concrete requirement exists:

```text
source_subject_id
source_subject_number
source object keys
source file UUIDs
verified source checksums
```

`subject_key` is deterministic and pseudonymous. It is not described as
irreversible anonymization because its inputs come from a finite, known public
dataset.

## 5. Warehouse and Mart Rules

`warehouse.dim_subject` may retain restricted lineage fields for controlled
engineering and audit use, but downstream marts should omit source subject IDs,
source object keys, file UUIDs, and source checksums unless a concrete requirement
needs them.

The current Phase 7 recording marts expose exact age, sex, night, and treatment
context because those fields are needed for controlled analysis. They must
therefore be treated as restricted analytical models, not as anonymous public
outputs.

`mart.mart_dataset_coverage` is aggregated, but some groups can contain a small
number of subjects. It still requires review before broad publication.

When data is prepared for wider access, prefer less identifying groupings (for
example age bands) when the analytical question permits it.

## Device-Event Access Boundary

Phase 11 simulated BCI events are generated data, but the Warehouse contract is
modeled as health-device telemetry so that the access model remains realistic.

Controlled identifiers include:

```text
event_id
device_id
session_id
```

They are pseudonymous rather than direct identifiers. Event payloads and
event-time activity are treated as restricted health telemetry in
`warehouse.fact_device_event`.

Kafka topic/partition/offset fields are operational lineage and use internal
team-only classification where appropriate.

## 6. Secrets and Credentials

Secrets belong in the local `.env` file and must never be committed.

Examples:

```text
POSTGRES_PASSWORD
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
```

`.env.example` documents variable names only and must not contain production
credentials.

## 7. Governance Metadata

`governance.source_system_registry` stores upstream access and internal policy.
`governance.column_classification` stores column-level flags such as:

```text
classification_level
contains_personal_data
contains_health_data
contains_direct_identifier
access_policy
masking_policy
```

Warehouse tables have registry-backed column classification for all 108
physical columns: 81 original Warehouse Core columns plus 27 columns in
`warehouse.fact_device_event`. Phase 7 marts currently use enforced dbt contracts and inherit
the restricted analytical policy from their Warehouse inputs. Dedicated mart
registry classifications are intentionally deferred until the access/BI phase,
before any broader publication is enabled.

## 8. Current Status

Implemented:

- open-source access metadata;
- restricted internal policy;
- governance registries and column classification;
- restricted handling guidance for patient-level Bronze and Silver data;
- subject/context staging column classifications;
- Warehouse classification for all 108 physical columns, including the Phase 11 device-event fact;
- restricted/redacted handling for source subject identifiers;
- aggregate-only policy for exact demographic/treatment fields where configured;
- Phase 7 marts that omit direct subject IDs and source-object lineage and remain restricted pending broader-access review.

Phase 7 marts are implemented and keep direct subject IDs and source-object lineage out. Their
remaining access-governance work is explicit mart-level registry classification
before BI or broader publication is enabled.
