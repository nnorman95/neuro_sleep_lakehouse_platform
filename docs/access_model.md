# Access Model

## External source access

Sleep-EDF Database Expanded is an open-access PhysioNet source.

```text
source_system = physionet_sleep_edf
access_model = open
credential_required = false
```

`access_model` describes how the upstream dataset is obtained.

## Internal platform access

Open source distribution does not mean patient-level sleep data should be
unrestricted inside the platform.

```text
patient-level Bronze/Silver data = restricted
operational metadata = team_only
public repository = code/config/docs only
```

The source registry therefore uses both:

```text
access_model = open
access_policy = restricted
```

These fields intentionally describe different concepts.
