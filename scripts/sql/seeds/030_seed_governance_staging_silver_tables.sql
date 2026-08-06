insert into governance.data_contract_registry (
    table_schema, table_name, contract_name, contract_version,
    contract_path, owner_role, data_layer, status
)
values
    ('staging', 'silver_recordings', 'staging_silver_recordings_contract', 'v1', 'contracts/staging_silver_recordings.yml', 'data_engineer', 'staging', 'active'),
    ('staging', 'silver_channels', 'staging_silver_channels_contract', 'v1', 'contracts/staging_silver_channels.yml', 'data_engineer', 'staging', 'active'),
    ('staging', 'silver_sleep_stage_intervals', 'staging_silver_sleep_stage_intervals_contract', 'v1', 'contracts/staging_silver_sleep_stage_intervals.yml', 'data_engineer', 'staging', 'active'),
    ('staging', 'silver_sleep_stage_epochs', 'staging_silver_sleep_stage_epochs_contract', 'v1', 'contracts/staging_silver_sleep_stage_epochs.yml', 'data_engineer', 'staging', 'active')
on conflict (table_schema, table_name, contract_version)
do update set
    contract_name = excluded.contract_name,
    contract_path = excluded.contract_path,
    owner_role = excluded.owner_role,
    data_layer = excluded.data_layer,
    status = excluded.status,
    updated_at = now();

insert into governance.column_classification (
    table_schema, table_name, column_name, data_layer,
    classification_level, contains_personal_data, contains_health_data,
    contains_direct_identifier, sensitivity_reason, access_policy,
    masking_policy
)
select
    'staging', 'silver_recordings', column_name, 'staging',
    'confidential', false, true, false,
    'Patient-level sleep dataset staging column.',
    'restricted',
    case
        when column_name in ('psg_bucket','psg_object_key','hypnogram_bucket','hypnogram_object_key')
        then 'redact'
        else 'none'
    end
from (
    values
        ('recording_id'),
        ('source_system'),
        ('psg_bucket'),
        ('psg_object_key'),
        ('hypnogram_bucket'),
        ('hypnogram_object_key'),
        ('recording_start'),
        ('duration_seconds'),
        ('channel_count'),
        ('annotation_count'),
        ('in_range_epoch_count'),
        ('out_of_range_epoch_count'),
        ('trailing_overhang_seconds')
) as columns_to_classify(column_name)
on conflict (table_schema, table_name, column_name)
do update set
    data_layer = excluded.data_layer,
    classification_level = excluded.classification_level,
    contains_personal_data = excluded.contains_personal_data,
    contains_health_data = excluded.contains_health_data,
    contains_direct_identifier = excluded.contains_direct_identifier,
    sensitivity_reason = excluded.sensitivity_reason,
    access_policy = excluded.access_policy,
    masking_policy = excluded.masking_policy,
    updated_at = now();

insert into governance.column_classification (
    table_schema, table_name, column_name, data_layer,
    classification_level, contains_personal_data, contains_health_data,
    contains_direct_identifier, sensitivity_reason, access_policy,
    masking_policy
)
select
    'staging', 'silver_channels', column_name, 'staging',
    'confidential', false, true, false,
    'Patient-level sleep dataset staging column.',
    'restricted',
    case
        when column_name in ('psg_bucket','psg_object_key','hypnogram_bucket','hypnogram_object_key')
        then 'redact'
        else 'none'
    end
from (
    values
        ('channel_id'),
        ('recording_id'),
        ('position'),
        ('source_label'),
        ('normalized_name'),
        ('sampling_frequency_hz'),
        ('physical_dimension'),
        ('physical_min'),
        ('physical_max'),
        ('digital_min'),
        ('digital_max'),
        ('samples_per_data_record'),
        ('prefiltering')
) as columns_to_classify(column_name)
on conflict (table_schema, table_name, column_name)
do update set
    data_layer = excluded.data_layer,
    classification_level = excluded.classification_level,
    contains_personal_data = excluded.contains_personal_data,
    contains_health_data = excluded.contains_health_data,
    contains_direct_identifier = excluded.contains_direct_identifier,
    sensitivity_reason = excluded.sensitivity_reason,
    access_policy = excluded.access_policy,
    masking_policy = excluded.masking_policy,
    updated_at = now();

insert into governance.column_classification (
    table_schema, table_name, column_name, data_layer,
    classification_level, contains_personal_data, contains_health_data,
    contains_direct_identifier, sensitivity_reason, access_policy,
    masking_policy
)
select
    'staging', 'silver_sleep_stage_intervals', column_name, 'staging',
    'confidential', false, true, false,
    'Patient-level sleep dataset staging column.',
    'restricted',
    case
        when column_name in ('psg_bucket','psg_object_key','hypnogram_bucket','hypnogram_object_key')
        then 'redact'
        else 'none'
    end
from (
    values
        ('interval_id'),
        ('recording_id'),
        ('source_annotation_index'),
        ('onset_seconds'),
        ('duration_seconds'),
        ('end_seconds'),
        ('source_label'),
        ('normalized_stage'),
        ('overlap_status')
) as columns_to_classify(column_name)
on conflict (table_schema, table_name, column_name)
do update set
    data_layer = excluded.data_layer,
    classification_level = excluded.classification_level,
    contains_personal_data = excluded.contains_personal_data,
    contains_health_data = excluded.contains_health_data,
    contains_direct_identifier = excluded.contains_direct_identifier,
    sensitivity_reason = excluded.sensitivity_reason,
    access_policy = excluded.access_policy,
    masking_policy = excluded.masking_policy,
    updated_at = now();

insert into governance.column_classification (
    table_schema, table_name, column_name, data_layer,
    classification_level, contains_personal_data, contains_health_data,
    contains_direct_identifier, sensitivity_reason, access_policy,
    masking_policy
)
select
    'staging', 'silver_sleep_stage_epochs', column_name, 'staging',
    'confidential', false, true, false,
    'Patient-level sleep dataset staging column.',
    'restricted',
    case
        when column_name in ('psg_bucket','psg_object_key','hypnogram_bucket','hypnogram_object_key')
        then 'redact'
        else 'none'
    end
from (
    values
        ('epoch_id'),
        ('recording_id'),
        ('source_interval_id'),
        ('source_annotation_index'),
        ('epoch_number'),
        ('start_seconds'),
        ('duration_seconds'),
        ('end_seconds'),
        ('source_label'),
        ('normalized_stage')
) as columns_to_classify(column_name)
on conflict (table_schema, table_name, column_name)
do update set
    data_layer = excluded.data_layer,
    classification_level = excluded.classification_level,
    contains_personal_data = excluded.contains_personal_data,
    contains_health_data = excluded.contains_health_data,
    contains_direct_identifier = excluded.contains_direct_identifier,
    sensitivity_reason = excluded.sensitivity_reason,
    access_policy = excluded.access_policy,
    masking_policy = excluded.masking_policy,
    updated_at = now();
