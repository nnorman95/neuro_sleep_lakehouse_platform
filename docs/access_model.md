# Access Model

## Sleep-EDF source access

Sleep-EDF Database Expanded is configured as an open-access source.

```text
source_system = physionet_sleep_edf
access_model = open
credential_required = false
access_policy = open
```

The extractor does not require a PhysioNet username or password.

Open access does not remove the requirement to follow the dataset
license and citation conditions.

## Repository access

The public repository contains:

- source code;
- SQL migrations and seeds;
- data contracts;
- configuration examples;
- synthetic smoke-test payloads;
- documentation.

The repository does not contain:

- real EDF recordings;
- generated local data;
- database passwords;
- MinIO secrets;
- local `.env`;
- local virtual environments.
