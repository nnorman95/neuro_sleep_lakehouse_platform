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

Broad analytical models should use:

```text
subject_sk
subject_key
recording_sk
recording_key
```

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
engineering and audit use, but downstream marts should omit them by default.

Age and sex can be used in controlled analytical models. Broad outputs should
prefer derived groups when the analytical question permits it, for example age
bands instead of exact age.

Aggregates should be reviewed before broad publication when a grouping could
isolate a very small number of subjects.

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

New subject-aware relational structures must be accompanied by matching
classification seeds before those structures are considered complete. The
current Warehouse Core satisfies this requirement for all 81 physical columns.

## 8. Current Status

Implemented:

- open-source access metadata;
- restricted internal policy;
- governance registries and column classification;
- restricted handling guidance for patient-level Bronze and Silver data;
- subject/context staging column classifications;
- Warehouse Core classification for all 81 physical columns;
- restricted/redacted handling for source subject identifiers;
- aggregate-only policy for exact demographic/treatment fields where configured.

The remaining downstream rule is to keep source identifiers out of broad marts
by default.
