# Data Sources

## 1. Active Source

The active source is **Sleep-EDF Database Expanded v1.0.0**, published on
PhysioNet.

```text
source_system: physionet_sleep_edf
access_model: open
credential_required: false
base_url: https://physionet.org/files/sleep-edfx/1.0.0/
```

The internal platform access policy for subject-level data is restricted.

## 2. Source Collections

```text
sleep-cassette/
sleep-telemetry/
```

Both collections contain PSG recordings and matching sleep-stage Hypnograms.
The two collections have different subject-metadata workbook semantics and can
also differ in recording-coverage behavior.

## 3. Supported File Roles

| Role | Typical filename | Format | Use |
|---|---|---|---|
| PSG | `*-PSG.edf` | EDF | signal samples and recording metadata |
| Hypnogram | `*-Hypnogram.edf` | EDF+ | source sleep-stage annotations |
| Cassette subjects | `SC-subjects.xls` | XLS | Cassette subjects and recording contexts |
| Telemetry subjects | `ST-subjects.xls` | XLS | Telemetry subjects and treatment contexts |
| Records inventory | `RECORDS` | text | source path inventory |
| Checksum inventory | `SHA256SUMS.txt` | text | official SHA-256 values |
| Other metadata | XML and source control files | mixed | retained as Bronze metadata |

## 4. Official Manifests

The extractor uses:

```text
RECORDS
SHA256SUMS.txt
```

`SHA256SUMS.txt` is the main machine-readable verification inventory because it
contains both relative source paths and official SHA-256 checksums.

Unsafe paths, incomplete PSG/Hypnogram pairs, unsupported entries, and checksum
mismatches are rejected explicitly.

## 5. Recording Identity

Sleep-EDF filenames are normalized into logical recording keys.

Examples:

```text
SC4001E0-PSG.edf          -> SC4001E
SC4001EC-Hypnogram.edf    -> SC4001E
ST7011J0-PSG.edf          -> ST7011J
ST7011JP-Hypnogram.edf    -> ST7011J
```

`recording_key` identifies the logical subject/night recording. It is distinct
from the UUIDv7 `recording_id` assigned to a concrete Silver representation.

## 6. Subject Workbook Semantics

### `SC-subjects.xls`

```text
unique subjects: 78
recording contexts: 153
sex code 1: F
sex code 2: M
```

Each recording context contains a subject, night number, and lights-off value.
Treatment is null for Cassette.

### `ST-subjects.xls`

```text
subjects: 22
recording contexts: 44
sex code 1: M
sex code 2: F
```

Each source row contains both placebo-night and temazepam-night context. The
normalizer emits separate recording-context rows with the correct treatment,
night number, and lights-off value.

### Combined normalized output

```text
subjects: 100
recording contexts: 197
```

## 7. Current Production Sample

The current Bronze/Silver production sample contains:

```text
SC4001E
SC4002E
SC4011E
SC4012E
ST7011J
```

The four Cassette recordings and one Telemetry recording produce
116,242,840 Silver signal rows in total. Together with 13,096 recording-metadata
rows (5 recordings, 33 channels, 834 intervals, and 12,224 epochs), the current
Silver recording outputs contain 116,255,936 rows across those datasets.

The subject workbooks are processed as complete metadata sources, so the
subject publication covers all 100 source subjects and all 197 contexts rather
than only the five signal recordings currently materialized in Silver.

## 8. User-Controlled Source Selection

Sample mode:

```env
DATA_PROFILE=sample
SLEEP_EDF_MAX_RECORDINGS=4
SLEEP_EDF_INCLUDE_CASSETTE=true
SLEEP_EDF_INCLUDE_TELEMETRY=true
SLEEP_EDF_INCLUDE_METADATA=true
```

`SLEEP_EDF_MAX_RECORDINGS=0` removes the recording limit.

Full mode:

```env
DATA_PROFILE=full
```

Full mode selects all discovered checksum-manifest entries and enables both
collections and metadata.

## 9. Bronze Object Layout

Source-relative paths are preserved under:

```text
bronze/physionet/sleep-edfx/1.0.0/
  RECORDS
  SHA256SUMS.txt
  SC-subjects.xls
  ST-subjects.xls
  sleep-cassette/
  sleep-telemetry/
```

Real source files are excluded from Git.

## 10. Source Limitations

- Sleep-EDF uses historical sleep-stage labels, including separate Stage 3 and
  Stage 4.
- Physical channel units can be missing or differ between recordings.
- Annotation coverage can exceed PSG duration for Cassette recordings.
- Telemetry recordings can have non-30-second-aligned duration and an
  unannotated PSG tail.
- Source subject identifiers are coded, but combinations of subject metadata
  remain quasi-identifying.

These characteristics are modeled explicitly rather than hidden through silent
cleaning.
