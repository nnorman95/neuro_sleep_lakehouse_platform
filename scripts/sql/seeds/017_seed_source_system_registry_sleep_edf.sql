INSERT INTO governance.source_system_registry (
    source_system,
    dataset_name,
    dataset_version,
    base_url,
    access_model,
    credential_required,
    active,
    source_owner_role,
    data_domain,
    contains_health_data,
    contains_patient_level_data,
    contains_direct_identifier,
    access_policy,
    status,
    notes
)
VALUES (
    'physionet_sleep_edf',
    'Sleep-EDF Database Expanded',
    '1.0.0',
    'https://physionet.org/files/sleep-edfx/1.0.0',
    'open',
    false,
    true,
    'data_engineering',
    'sleep_neuroscience',
    true,
    true,
    false,
    'open',
    'planned',
    'Open-access polysomnography recordings, sleep-stage hypnograms, and descriptive metadata.'
)
ON CONFLICT (source_system, dataset_version)
DO UPDATE SET
    dataset_name = EXCLUDED.dataset_name,
    base_url = EXCLUDED.base_url,
    access_model = EXCLUDED.access_model,
    credential_required = EXCLUDED.credential_required,
    active = EXCLUDED.active,
    source_owner_role = EXCLUDED.source_owner_role,
    data_domain = EXCLUDED.data_domain,
    contains_health_data = EXCLUDED.contains_health_data,
    contains_patient_level_data = EXCLUDED.contains_patient_level_data,
    contains_direct_identifier = EXCLUDED.contains_direct_identifier,
    access_policy = EXCLUDED.access_policy,
    status = EXCLUDED.status,
    notes = EXCLUDED.notes;
