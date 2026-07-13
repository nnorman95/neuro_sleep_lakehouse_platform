# Data Sources

## Active source

The active source is Sleep-EDF Database Expanded v1.0.0,
published on PhysioNet.

```text
source_system: physionet_sleep_edf
access_model: open
credential_required: false
```

Base source URL:

```text
https://physionet.org/files/sleep-edfx/1.0.0/
```

## Source collections

```text
sleep-cassette/
sleep-telemetry/
```

Each collection contains PSG recordings and matching sleep-stage
hypnograms.

Supported data roles:

| Role | Typical filename | Format |
|---|---|---|
| PSG | `*-PSG.edf` | EDF |
| Hypnogram | `*-Hypnogram.edf` | EDF+ |
| Metadata | `RECORDS`, XLS, XML | mixed |

## Official manifests

The extractor uses:

```text
RECORDS
SHA256SUMS.txt
```

`SHA256SUMS.txt` is the main machine-readable inventory because it
provides both source paths and expected SHA-256 checksums.

## User-controlled limits

Sample mode:

```env
DATA_PROFILE=sample
SLEEP_EDF_MAX_RECORDINGS=4
```

`SLEEP_EDF_MAX_RECORDINGS=0` means no recording limit.


Full mode:

```env
DATA_PROFILE=full
```

Full mode selects every discovered checksum-manifest entry and
enables both source collections and metadata.

## Local storage

Source files are written to:

```text
bronze/
  physionet/
    sleep-edfx/
      1.0.0/
        sleep-cassette/
        sleep-telemetry/
```

Real EDF files must not be committed to Git.
