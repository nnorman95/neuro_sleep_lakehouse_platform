# ADR 001: Silver Identity and Lineage

## Status

Accepted.

## Context

A source PSG/Hypnogram pair can be processed more than once when its
content, Silver schema, transform implementation, or transform
configuration changes.

Path identity alone is insufficient because the same object-storage
paths can later contain different bytes.

## Decision

The platform uses separate identifiers for separate concepts.

### `source_pair_id`

A SHA-256 identifier derived from the logical PSG and Hypnogram object
locations.

It answers:

> Which logical source pair is this?

### `input_fingerprint`

A SHA-256 identifier derived from the verified SHA-256 checksums of the
PSG and Hypnogram payloads.

It answers:

> Which exact source bytes were processed?

### `config_id`

A SHA-256 identifier derived from the canonical Silver transform
configuration, including schema and transform versions.

It answers:

> Which transformation configuration was used?

### `recording_id`

A UUIDv7 assigned to one concrete Silver representation.

It answers:

> Which materialized Silver recording is this?

A new content fingerprint, schema version, transform version, or
configuration produces a new Silver representation and therefore a new
`recording_id`.

## Version-aware Silver grain

One `staging.silver_recordings` row represents:

```text
source_system
+ source_pair_id
+ input_fingerprint
+ schema_version
+ transform_version
+ config_id
```

The Silver object location is also unique:

```text
silver_bucket
+ silver_output_prefix
```

## Lineage

The staging recording row links to:

- the PSG row in `raw.file_registry`;
- the Hypnogram row in `raw.file_registry`;
- the verified source checksums;
- the Silver bucket and output prefix;
- the pipeline run that loaded the staging row.

## Annotation timing

Source annotations may start before the PSG recording boundary.
Therefore `staging.silver_sleep_stage_intervals.onset_seconds` may be
negative.

Epoch rows remain limited to the emitted in-range PSG timeline and keep
their non-negative `start_seconds` rule.

## Consequences

- Replacing bytes under the same paths no longer represents the same
  input.
- Multiple valid Silver versions can coexist for one logical source
  pair.
- Staging preserves enough lineage to trace a row back to Bronze and
  forward to its Silver objects.
- Existing source-path-only uniqueness is removed.
