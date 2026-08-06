# Sleep-EDF Inspection

## 1. Scope

The current production baseline includes four Sleep Cassette pairs and one
Sleep Telemetry pair.

Cassette:

- `SC4001E0-PSG.edf` + `SC4001EC-Hypnogram.edf`
- `SC4002E0-PSG.edf` + `SC4002EC-Hypnogram.edf`
- `SC4011E0-PSG.edf` + `SC4011EH-Hypnogram.edf`
- `SC4012E0-PSG.edf` + `SC4012EC-Hypnogram.edf`

Telemetry:

- `ST7011J0-PSG.edf` + `ST7011JP-Hypnogram.edf`

Files are read from MinIO Bronze with `edfio==0.4.13`.

## 2. Cassette PSG Findings

Each inspected Cassette PSG contains seven channels:

| Position | Channel | Sampling frequency | Unit |
|---:|---|---:|---|
| 1 | EEG Fpz-Cz | 100 Hz | uV |
| 2 | EEG Pz-Oz | 100 Hz | uV |
| 3 | EOG horizontal | 100 Hz | uV |
| 4 | Resp oro-nasal | 1 Hz | nullable |
| 5 | EMG submental | 1 Hz | uV |
| 6 | Temp rectal | 1 Hz | usually DegC, but nullable |
| 7 | Event marker | 1 Hz | nullable |

The PSG headers are not globally identical. `SC4012E0-PSG.edf` has an empty
physical unit for `Temp rectal`, while the other inspected Cassette files use
`DegC`.

Therefore channel metadata is stored per recording and physical unit is
nullable.

## 3. Cassette Hypnogram Findings

Each inspected Cassette Hypnogram is annotation-only:

- ordinary signal count: `0`;
- annotations contain onset, duration, and source stage label;
- annotation durations are multiples of 30 seconds;
- total annotated duration: 86,400 seconds;
- total source epoch count after expansion: 2,880.

Adjacent equal 30-second stages are compacted into longer source intervals.

Example:

```text
onset=30630
duration=120
text=Sleep stage 1
```

This represents four consecutive 30-second epochs. Silver preserves the source
interval and separately emits derived 30-second epoch rows.

## 4. Cassette Pair Results

| PSG file | PSG duration (s) | Epochs inside PSG | Epochs outside PSG | Trailing overhang (s) |
|---|---:|---:|---:|---:|
| `SC4001E0-PSG.edf` | 79,500 | 2,650 | 230 | 6,900 |
| `SC4002E0-PSG.edf` | 84,900 | 2,830 | 50 | 1,500 |
| `SC4011E0-PSG.edf` | 84,060 | 2,802 | 78 | 2,340 |
| `SC4012E0-PSG.edf` | 85,500 | 2,850 | 30 | 900 |

For all four pairs:

- PSG and Hypnogram start date/time match;
- annotations overlap the PSG;
- source durations are multiples of 30 seconds;
- Hypnogram coverage extends beyond PSG end;
- channel order and sampling frequencies are stable;
- at least one unit-level difference exists.

The out-of-range annotation tail remains observable, but derived epochs outside
PSG coverage are not joined to signal samples.

## 5. Telemetry Findings

The production Telemetry recording is `ST7011J`.

Telemetry differs from the inspected Cassette pattern:

- recording duration may not align exactly to 30 seconds;
- the PSG may have a tail without annotation coverage;
- the complete PSG signal must still be retained;
- only real annotation-derived epochs are emitted.

Quality semantics:

```text
non-30-second-aligned duration -> warning
unannotated PSG tail           -> warning
real epoch extending past PSG  -> error
```

The production output contains:

```text
14,720,329 signal rows
104 data objects
```

## 6. Observed Sleep-Stage Labels

| Source label | Source-preserving normalized value |
|---|---|
| `Sleep stage W` | `W` |
| `Sleep stage 1` | `N1` |
| `Sleep stage 2` | `N2` |
| `Sleep stage 3` | `N3` |
| `Sleep stage 4` | `N4` |
| `Sleep stage R` | `REM` |
| `Sleep stage ?` | `UNKNOWN` |
| `Movement time` | `MOVEMENT` |

Stage 3 and Stage 4 remain distinguishable in Silver. A later analytical layer
may derive an AASM-style mapping where both contribute to analytical `N3`.

## 7. Subject Workbook Findings

`SC-subjects.xls`:

```text
78 unique subjects
153 recording contexts
sex code 1 = F
sex code 2 = M
```

`ST-subjects.xls`:

```text
22 subjects
44 recording contexts
sex code 1 = M
sex code 2 = F
placebo and temazepam contexts per source row
```

Combined Silver metadata:

```text
100 subjects
197 recording contexts
```

Night, treatment, and lights-off values belong to recording context, not the
subject row.

## 8. Silver Design Consequences

### Recording metadata

Store one row per concrete Silver representation with:

- `recording_id`;
- source PSG/Hypnogram locations;
- start and duration;
- channel and annotation counts;
- in-range and out-of-range epoch counts;
- overhang/coverage metrics;
- version and lineage identity.

### Channel metadata

Store one row per concrete recording and channel. Do not assume global channel
units.

### Source intervals

Preserve source onset, duration, label, normalized stage, and PSG overlap
classification. Source onset can be negative.

### Derived epochs

Emit one 30-second row per real annotation-derived epoch inside PSG coverage.

### Signals

Store high-volume samples as chunked Parquet in MinIO. Keep sample-to-channel
and sample-to-epoch relationships without loading every sample into PostgreSQL.

### Subject context

Use `subject_key` for logical participants and `recording_key` for logical
recording/night identity. Keep these separate from concrete Silver
`recording_id`.

## 9. Production Totals

Across four Cassette and one Telemetry recording:

```text
116,255,936 Silver signal rows
```

The current inspection supports Warehouse modeling but does not imply that the
entire Sleep-EDF source has been processed.
