# Sleep-EDF Sample Inspection

## Scope

This inspection covers the four PSG/Hypnogram pairs currently stored in the Bronze layer:

- `SC4001E0-PSG.edf` + `SC4001EC-Hypnogram.edf`
- `SC4002E0-PSG.edf` + `SC4002EC-Hypnogram.edf`
- `SC4011E0-PSG.edf` + `SC4011EH-Hypnogram.edf`
- `SC4012E0-PSG.edf` + `SC4012EC-Hypnogram.edf`

The files were read from MinIO with `edfio==0.4.13`.

## Main findings

### PSG structure

Each PSG file contains seven channels:

| Position | Channel | Sampling frequency | Unit |
|---:|---|---:|---|
| 1 | EEG Fpz-Cz | 100 Hz | uV |
| 2 | EEG Pz-Oz | 100 Hz | uV |
| 3 | EOG horizontal | 100 Hz | uV |
| 4 | Resp oro-nasal | 1 Hz | empty |
| 5 | EMG submental | 1 Hz | uV |
| 6 | Temp rectal | 1 Hz | usually DegC |
| 7 | Event marker | 1 Hz | empty |

The PSG files do not all have an identical header schema.

`SC4012E0-PSG.edf` has an empty physical unit for `Temp rectal`, while the other inspected PSG files use `DegC`.

Silver processing must therefore store channel metadata per recording and must not assume that units are globally fixed.

### Hypnogram structure

Each Hypnogram is an annotation-only EDF file:

- ordinary signal count: `0`
- annotations contain onset, duration, and stage label
- annotation durations are multiples of 30 seconds
- total annotated duration is 86,400 seconds
- total expanded epoch count is 2,880 per Hypnogram

Adjacent equal 30-second sleep-stage epochs are compacted into longer annotation intervals.

Example:

```text
onset=30630
duration=120
text=Sleep stage 1
```

represents four consecutive 30-second epochs with the same stage label.

The original interval must be preserved, while a derived 30-second epoch table can be created for analysis.

## Pair-level results

| PSG file | PSG duration (s) | Epochs inside PSG | Epochs outside PSG | Trailing annotation overhang (s) |
|---|---:|---:|---:|---:|
| SC4001E0-PSG.edf | 79,500 | 2,650 | 230 | 6,900 |
| SC4002E0-PSG.edf | 84,900 | 2,830 | 50 | 1,500 |
| SC4011E0-PSG.edf | 84,060 | 2,802 | 78 | 2,340 |
| SC4012E0-PSG.edf | 85,500 | 2,850 | 30 | 900 |

For all four pairs:

- PSG and Hypnogram start date/time match
- Hypnogram annotations overlap the PSG
- annotation durations are multiples of 30 seconds
- Hypnogram coverage extends beyond the end of the PSG
- channel order and sampling frequencies are stable
- one unit-level schema difference exists

Silver processing must only attach derived epochs that overlap the PSG duration.

Annotations outside the PSG range must not be joined to signal samples, but the overhang must remain observable as a data-quality metric.

## Observed stage labels

The sample contains:

- `Sleep stage W`
- `Sleep stage 1`
- `Sleep stage 2`
- `Sleep stage 3`
- `Sleep stage 4`
- `Sleep stage R`
- `Sleep stage ?`
- `Movement time`

Silver must preserve the original source label.

A normalized stage field can be added separately:

| Source label | Normalized stage |
|---|---|
| Sleep stage W | W |
| Sleep stage 1 | N1 |
| Sleep stage 2 | N2 |
| Sleep stage 3 | N3 |
| Sleep stage 4 | N4 |
| Sleep stage R | REM |
| Sleep stage ? | UNKNOWN |
| Movement time | MOVEMENT |

Stage 3 and Stage 4 must remain distinguishable in the source-preserving Silver representation.

A later analytical layer may derive an AASM-style stage where source Stage 3 and Stage 4 are combined into N3.

## Silver design implications

### Recording metadata

Store one row per PSG/Hypnogram pair with:

- recording identifier
- PSG object key
- Hypnogram object key
- start date/time
- PSG duration
- channel count
- annotation count
- in-range epoch count
- out-of-range epoch count
- trailing overhang seconds

### Channel metadata

Store one row per recording and channel with:

- recording identifier
- channel position
- source label
- sampling frequency
- physical unit
- physical range
- digital range
- samples per data record
- prefiltering text

The physical unit must be nullable.

### Source annotation intervals

Preserve each original annotation interval with:

- recording identifier
- source onset seconds
- source duration seconds
- source label
- interval end seconds
- overlap status with PSG

### Derived sleep-stage epochs

Create 30-second rows only for epochs that overlap the PSG:

- recording identifier
- epoch number
- epoch start seconds
- epoch end seconds
- source stage label
- normalized stage
- source annotation index

Do not duplicate signal values in this table.

### Signal data

Signal samples must be stored by recording and channel because channels have different sampling frequencies.

A suitable Parquet design is:

```text
silver/sleep_edf/signals/
  recording_id=<recording_id>/
    channel=<normalized_channel_name>/
      part-*.parquet
```

Each signal dataset should include:

- recording identifier
- channel identifier
- sample index
- elapsed seconds
- signal value
- sampling frequency
- physical unit

## Quality rules derived from inspection

1. PSG and Hypnogram start date/time must match.
2. Each Hypnogram must contain annotations.
3. Annotation duration must be positive and divisible by 30 seconds.
4. Annotation intervals must overlap the PSG.
5. Out-of-range annotation epochs must be counted and excluded from signal joins.
6. Channel metadata must be stored per recording.
7. Missing physical units must be accepted but surfaced as a quality warning.
8. Original stage labels must be preserved.
9. Unknown and movement labels must not be silently converted to ordinary sleep stages.
10. The number of in-range 30-second epochs should equal `PSG duration / 30` when the PSG duration is divisible by 30.

## Inspection status

```text
pair_count=4
matching_channel_schema_count=3
differing_channel_schema_count=1
temporary_file_cleanup=automatic
edf_pair_schema_audit_status=success
```
