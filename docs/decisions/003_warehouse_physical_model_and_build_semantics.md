# ADR 003: Warehouse Physical Model and Build Semantics

## Status

Accepted.

## Context

ADR 002 established the logical Warehouse Core grains and the rule that the
Warehouse exposes one approved current Silver representation for each logical
recording.

The dbt source layer is now implemented against PostgreSQL staging. Before
Warehouse SQL is written, the physical dimensional model, key strategy,
current-version selection, materialization semantics, and integrity rules must
be explicit.

This ADR also refines ADR 002 where dbt implementation details require more
precise behavior. If this ADR conflicts with ADR 002 on Phase 6 Warehouse build
semantics, this ADR takes precedence.

The design must avoid these failure modes:

- unstable warehouse keys after a full dbt rebuild;
- treating version-specific Silver IDs as logical Warehouse identity;
- silently choosing a "latest" row by load timestamp;
- silently choosing a "latest" Silver recording by UUID ordering;
- mixing subject rows from different metadata publications;
- inventing a global channel identity from a repeated channel label;
- normalizing an analytical star schema as if it were an OLTP schema;
- multiplying recording-level epochs by channel;
- writing rebuild-time timestamps that make unchanged data look changed;
- claiming one cross-model PostgreSQL transaction that dbt does not provide;
- tightly coupling independently replaced dbt tables with physical foreign keys;
- introducing SCD or incremental complexity without a requirement.

Immutable history remains in Silver, staging, manifests, and operational
lineage. Phase 6 Warehouse Core is a current-state analytical model.

## Decision

### 1. Public Warehouse Core

The first public Warehouse Core contains:

```text
warehouse.dim_subject
warehouse.dim_recording
warehouse.dim_channel
warehouse.dim_sleep_stage
warehouse.fact_sleep_epoch
```

Version-selection and reconciliation logic may use internal dbt models, but
those models are implementation details rather than public analytical
relations.

### 2. Current-state semantics

Phase 6 uses Type 1 / current-state semantics.

The Warehouse exposes:

- one explicitly eligible subject-metadata publication per source collection;
- one explicitly eligible Silver representation per logical recording;
- channels from that selected recording representation;
- epochs from that selected recording representation.

Historical Warehouse rows are not maintained as SCD history in Phase 6.
Historical source states remain recoverable from immutable Silver and staging
lineage.

### 3. Fail-closed version selection

Warehouse selection must never infer business approval from ingestion order.

The following are forbidden as implicit current-version rules:

```text
max(loaded_at)
max(staging_load_run_id)
max(recording_id)
row_number() over (... order by loaded_at desc)
UUIDv7 ordering as "latest wins"
```

Those fields are lineage or ordering metadata, not approval semantics.

#### Metadata publication

For each:

```text
source_system
+ dataset_version
+ collection
```

Phase 6 requires exactly one eligible complete metadata publication identified
by one `metadata_input_fingerprint`.

The selected publication must be shared by both:

```text
staging.silver_subjects
staging.silver_recording_contexts
```

If zero eligible publications exist, the Warehouse build fails.

If more than one eligible publication exists, the Warehouse build fails rather
than choosing one implicitly.

A future explicit metadata-publication registry may replace this fail-closed
rule.

#### Recording representation

For each logical recording business key:

```text
source_system
+ dataset_version
+ collection
+ recording_key
```

Phase 6 requires exactly one eligible compatible staged Silver representation.

If zero representations exist, the recording is absent from the current
Warehouse input.

If more than one eligible representation exists for the same logical
recording, the Warehouse build fails rather than silently selecting the newest
materialization.

A future approved-version registry may explicitly select one
`silver_recording_id`. Until that registry exists, "latest wins" behavior is
not allowed.

### 4. Deterministic Warehouse keys

dbt performs deterministic full table rebuilds in Phase 6. Warehouse keys must
therefore remain stable across rebuilds and must not depend on sequence state,
insert order, or `row_number()`.

Phase 6 uses deterministic hashed Warehouse surrogate keys for entity and fact
identity.

The implementation will use one shared dbt macro based on PostgreSQL's built-in
`md5()` over an unambiguous JSON array serialization:

```sql
md5(jsonb_build_array(...key parts...)::text)
```

MD5 is used only as a deterministic non-security identifier. It is not used for
passwords, privacy, anonymization, or cryptographic trust.

Canonical key definitions:

```text
subject_sk
= hash(["subject", subject_key])

recording_sk
= hash([
    "recording",
    source_system,
    dataset_version,
    collection,
    recording_key
  ])

channel_sk
= hash([
    "channel",
    recording_sk,
    normalized_name
  ])

sleep_epoch_sk
= hash([
    "sleep_epoch",
    recording_sk,
    epoch_number
  ])
```

Every input to these keys is required to be non-null.

`dim_sleep_stage` is a tiny controlled reference dimension and uses fixed
explicit integer keys rather than hashes.

### 5. `warehouse.dim_subject`

Grain:

```text
one row per logical subject
```

Primary Warehouse key:

```text
subject_sk
```

Business key:

```text
subject_key
```

The dimension includes all validated subjects in the selected current metadata
publication, including subjects that do not yet have a loaded Warehouse
recording.

Candidate attributes include:

```text
subject_sk
subject_key
source_system
dataset_version
collection
age_years
sex
source_subject_id
source_subject_number
source_bucket
source_object_key
metadata_input_fingerprint
first_loaded_at
last_loaded_at
```

`metadata_input_fingerprint` is publication lineage. It does not define subject
identity.

Source identifiers remain governed/restricted attributes and must not be
mistaken for anonymized values.

Required uniqueness:

```text
subject_sk
subject_key
```

### 6. `warehouse.dim_recording`

Grain:

```text
one row per logical recording / study night
```

Primary Warehouse key:

```text
recording_sk
```

Logical business key:

```text
source_system
+ dataset_version
+ collection
+ recording_key
```

The dimension resolves the selected recording representation to exactly one
recording context and therefore exactly one subject.

Candidate attributes include:

```text
recording_sk
recording_key
subject_sk
source_system
dataset_version
collection
night_number
lights_off_seconds
treatment

silver_recording_id
recording_start
duration_seconds
channel_count
annotation_count
in_range_epoch_count
out_of_range_epoch_count
trailing_overhang_seconds

psg_file_id
hypnogram_file_id
source_pair_id
input_fingerprint
config_id
schema_version
transform_version
psg_checksum_sha256
hypnogram_checksum_sha256
silver_bucket
silver_output_prefix
staging_load_run_id
first_loaded_at
last_loaded_at
```

`silver_recording_id` identifies the currently selected concrete Silver
materialization. It is lineage, not logical Warehouse identity.

Count attributes such as `channel_count` and `annotation_count` are retained as
recording metadata/reconciliation attributes. They are not a substitute for a
recording-grain fact table if additive recording metrics are required later.

Required uniqueness:

```text
recording_sk

source_system
+ dataset_version
+ collection
+ recording_key

silver_recording_id
```

### 7. `warehouse.dim_channel`

Grain:

```text
one logical channel within one logical recording,
described by the currently selected Silver representation
```

Phase 6 does not treat a repeated normalized label such as `EEG Fpz-Cz` as one
global channel entity across the dataset.

Primary Warehouse key:

```text
channel_sk
```

Stable logical identity within the current model:

```text
recording_sk
+ normalized_name
```

`position` is a separately validated attribute and must also be unique within
one recording.

Candidate attributes include:

```text
channel_sk
recording_sk
silver_channel_id
silver_recording_id
position
source_label
normalized_name
sampling_frequency_hz
physical_dimension
physical_min
physical_max
digital_min
digital_max
samples_per_data_record
prefiltering
first_loaded_at
last_loaded_at
```

A globally conformed channel dimension or recording-channel bridge is deferred
until a channel-grain analytical fact creates a real need.

### 8. `warehouse.dim_sleep_stage`

Grain:

```text
one source-preserving normalized Silver stage code
```

Controlled reference data:

| `sleep_stage_sk` | `silver_stage_code` | `analytical_stage_code` |
|---:|---|---|
| 1 | `W` | `W` |
| 2 | `N1` | `N1` |
| 3 | `N2` | `N2` |
| 4 | `N3` | `N3` |
| 5 | `N4` | `N3` |
| 6 | `REM` | `REM` |
| 7 | `UNKNOWN` | `UNKNOWN` |
| 8 | `MOVEMENT` | `MOVEMENT` |

Source Stage 3 and Stage 4 remain distinguishable while analytical grouping may
map both to N3.

`UNKNOWN` and `MOVEMENT` remain explicit values and are never silently mapped
to ordinary scored sleep.

### 9. `warehouse.fact_sleep_epoch`

Grain:

```text
one emitted 30-second epoch
for one selected logical recording representation
```

Primary Warehouse key:

```text
sleep_epoch_sk
```

Stable analytical identity:

```text
recording_sk
+ epoch_number
```

The fact directly references the analytical dimensions that describe an epoch:

```text
subject_sk
recording_sk
sleep_stage_sk
```

`subject_sk` is intentionally present even though the subject can also be
reached through `dim_recording`.

This is deliberate star-schema denormalization, not accidental duplication.
Epoch analysis commonly groups directly by subject attributes, and a fact table
should not require a snowflake traversal through `dim_recording` for every
subject-level query.

Consistency is enforced by a test requiring:

```text
fact_sleep_epoch.subject_sk
=
dim_recording.subject_sk
for the same recording_sk
```

Candidate fact columns include:

```text
sleep_epoch_sk
subject_sk
recording_sk
sleep_stage_sk

silver_epoch_id
silver_recording_id
source_interval_id
source_annotation_index

epoch_number
start_seconds
duration_seconds
end_seconds
source_label
silver_stage_code

staging_load_run_id
loaded_at
```

`silver_epoch_id` is exact Silver lineage and may change after a future
reprocessing. It is therefore not the logical Warehouse fact identity.

Required uniqueness:

```text
sleep_epoch_sk

recording_sk
+ epoch_number

silver_recording_id
+ epoch_number
```

Epochs are recording-level facts and are never multiplied by channel.

### 10. Warehouse lineage timestamps

A dbt rebuild must not make unchanged source data appear newly changed.

Warehouse models therefore must not stamp rows with `current_timestamp` merely
because dbt rebuilt a table.

Where retained:

```text
first_loaded_at
```

is derived from the earliest accepted staging occurrence of the logical entity.

```text
last_loaded_at
```

is derived from the latest accepted staging occurrence relevant to the current
entity state.

Silver run IDs, object pointers, fingerprints, checksums, schema versions, and
transform versions remain lineage attributes where useful.

### 11. dbt materialization

The initial Warehouse Core uses:

```text
materialized = table
```

for public dimensions and the fact.

The current data volume does not justify incremental complexity.

Full deterministic rebuilds provide:

- simple recovery;
- reproducible current-state transformations;
- straightforward reconciliation;
- no sequence-dependent key assignment;
- easier verification of Warehouse grain.

Incremental loading is deferred until runtime or data volume creates a measured
need.

Internal selection models should be ephemeral where practical. A persisted
intermediate relation must have a specific operational or observability reason.

### 12. Contracts, constraints, and relationship integrity

Public Warehouse models will use explicit dbt model contracts for column names
and PostgreSQL data types.

Physical constraints should be added only where the PostgreSQL adapter and dbt
table replacement semantics support them reliably.

At minimum, every public model will have data tests for:

- primary-key uniqueness;
- non-null key columns;
- business-key uniqueness;
- accepted categorical values;
- range/domain rules;
- cross-model relationships;
- reconciliation to the selected staging input.

Cross-model PostgreSQL foreign-key constraints are not required in the initial
dbt-managed Warehouse Core.

Reason: dbt table materialization replaces relations independently. Hard
foreign-key coupling between replaceable analytical tables can make rebuilds
and relation swaps unnecessarily fragile.

Referential integrity is still mandatory; it is enforced by dbt relationship
and reconciliation tests.

### 13. Build consistency

Phase 6 does not claim one PostgreSQL transaction across the entire dbt DAG.

The guarantee is:

- each dbt table model uses the database transaction semantics of its own
  materialization;
- relevant staging input must not be mutated concurrently with a Warehouse
  publication run;
- all models consume the same fail-closed current-selection logic;
- dependency ordering is controlled through `ref()`;
- `dbt build` runs tests in the dependency graph;
- a failed build is not considered a successfully published Warehouse state.

A future orchestration layer may provide stronger publication semantics by
building into an isolated schema/version and promoting it only after full
validation.

### 14. Expected current baseline cardinality

For the current staged production baseline, the first Warehouse Core build is
expected to produce:

```text
dim_subject        100 rows
dim_recording        5 rows
dim_channel         33 rows
dim_sleep_stage      8 rows
fact_sleep_epoch 12,224 rows
```

These are reconciliation expectations for the current dataset, not permanent
hard-coded business rules.

### 15. Required Warehouse validation

#### `dim_subject`

- `subject_sk` non-null and unique;
- `subject_key` non-null and unique;
- exactly one eligible metadata publication per source collection;
- accepted subject-domain values;
- required source lineage present.

#### `dim_recording`

- `recording_sk` non-null and unique;
- logical business key unique;
- selected `silver_recording_id` non-null and unique;
- exactly one resolved subject/context;
- positive duration;
- non-negative reconciliation counts;
- required selected-Silver lineage present.

#### `dim_channel`

- `channel_sk` non-null and unique;
- `recording_sk + normalized_name` unique;
- `recording_sk + position` unique;
- `silver_recording_id` matches the selected parent recording;
- positive sampling frequency;
- valid physical and digital ranges.

#### `dim_sleep_stage`

- exactly eight controlled rows;
- `sleep_stage_sk` unique;
- `silver_stage_code` unique;
- analytical mapping exactly matches the controlled mapping.

#### `fact_sleep_epoch`

- `sleep_epoch_sk` non-null and unique;
- `recording_sk + epoch_number` unique;
- `silver_recording_id + epoch_number` unique;
- every `subject_sk`, `recording_sk`, and `sleep_stage_sk` resolves;
- fact `subject_sk` agrees with the parent recording's `subject_sk`;
- `duration_seconds = 30`;
- `start_seconds >= 0`;
- `end_seconds > start_seconds`;
- no multiplication by channel;
- row count reconciles exactly to epochs from selected Silver representations.

### 16. Explicit non-goals

Phase 6 does not introduce:

- SCD Type 2 history;
- an all-version Warehouse Core;
- silent "latest wins" version selection;
- an incremental fact-loading framework;
- a global conformed channel dimension;
- a recording-channel bridge;
- channel-level signal facts;
- PostgreSQL storage of raw signal samples;
- `fact_signal_quality`;
- `fact_device_event`;
- `fact_recording_summary`;
- signal-feature facts;
- Gold or mart models.

## Consequences

### Positive

- Warehouse keys remain stable across full rebuilds.
- Silver materialization IDs remain separate from logical analytical identity.
- Metadata publications cannot be silently mixed.
- Multiple compatible recording versions cannot silently overwrite each other.
- The fact remains a real star-schema analytical surface for subject queries.
- Channel identity is not falsely globalized.
- Rebuild-time timestamps do not create artificial change.
- dbt integrity tests and physical constraints have clearly separated roles.
- Build guarantees match what dbt actually provides.

### Trade-offs

- A second eligible metadata publication or recording representation blocks the
  Warehouse until explicit selection logic exists.
- Current-state history must be reconstructed from Silver/staging lineage.
- Full rebuilds will eventually become inefficient if Warehouse volume grows
  substantially.
- Cross-model referential integrity is tested by dbt rather than enforced with
  PostgreSQL foreign keys.
- Deterministic hashed keys are wider than integer identity keys.
