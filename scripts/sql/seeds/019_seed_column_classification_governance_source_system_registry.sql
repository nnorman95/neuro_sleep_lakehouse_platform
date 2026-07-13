insert into governance.column_classification (
    table_schema,
    table_name,
    column_name,
    data_layer,
    classification_level,
    contains_personal_data,
    contains_health_data,
    contains_direct_identifier,
    sensitivity_reason,
    access_policy,
    masking_policy
)
select
    'governance' as table_schema,
    'source_system_registry' as table_name,
    column_name,
    'governance' as data_layer,
    classification_level,
    false as contains_personal_data,
    contains_health_data,
    false as contains_direct_identifier,
    sensitivity_reason,
    access_policy,
    masking_policy
from (
    values
        (
            'source_id',
            'internal',
            false,
            'Operational identifier for a registered source system.',
            'team_only',
            'none'
        ),
        (
            'source_system',
            'internal',
            false,
            'Internal source system identifier.',
            'team_only',
            'none'
        ),
        (
            'dataset_name',
            'internal',
            false,
            'Public or internal dataset name.',
            'team_only',
            'none'
        ),
        (
            'dataset_version',
            'internal',
            false,
            'Dataset version metadata.',
            'team_only',
            'none'
        ),
        (
            'base_url',
            'internal',
            false,
            'Dataset base URL. Does not include credentials.',
            'team_only',
            'none'
        ),
        (
            'access_model',
            'internal',
            false,
            'Source access model metadata.',
            'team_only',
            'none'
        ),
        (
            'credential_required',
            'internal',
            false,
            'Boolean flag describing whether credentials are required.',
            'team_only',
            'none'
        ),
        (
            'active',
            'internal',
            false,
            'Operational active/inactive source flag.',
            'team_only',
            'none'
        ),
        (
            'source_owner_role',
            'internal',
            false,
            'Responsible platform role, not a direct personal identifier.',
            'team_only',
            'none'
        ),
        (
            'data_domain',
            'internal',
            false,
            'High-level source domain metadata.',
            'team_only',
            'none'
        ),
        (
            'contains_health_data',
            'internal',
            false,
            'Boolean governance flag about source sensitivity.',
            'team_only',
            'none'
        ),
        (
            'contains_patient_level_data',
            'internal',
            false,
            'Boolean governance flag about patient-level source granularity.',
            'team_only',
            'none'
        ),
        (
            'contains_direct_identifier',
            'internal',
            false,
            'Boolean governance flag about direct identifier expectation.',
            'team_only',
            'none'
        ),
        (
            'access_policy',
            'internal',
            false,
            'Platform access policy metadata.',
            'team_only',
            'none'
        ),
        (
            'status',
            'internal',
            false,
            'Operational source status.',
            'team_only',
            'none'
        ),
        (
            'notes',
            'internal',
            false,
            'Governance notes. Should not contain secrets or credentials.',
            'team_only',
            'none'
        ),
        (
            'created_at',
            'internal',
            false,
            'Record creation timestamp.',
            'team_only',
            'none'
        ),
        (
            'updated_at',
            'internal',
            false,
            'Record update timestamp.',
            'team_only',
            'none'
        )
) as columns_to_classify(
    column_name,
    classification_level,
    contains_health_data,
    sensitivity_reason,
    access_policy,
    masking_policy
)
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
