from neuro_sleep.db.postgres import get_postgres_connection


EXPECTED_COLUMNS = {
    "dim_subject": {
        "subject_sk",
        "subject_key",
        "source_system",
        "dataset_version",
        "collection",
        "age_years",
        "sex",
        "source_subject_id",
        "source_subject_number",
        "source_bucket",
        "source_object_key",
        "metadata_input_fingerprint",
        "first_loaded_at",
        "last_loaded_at",
    },
    "dim_recording": {
        "recording_sk",
        "recording_key",
        "subject_sk",
        "source_system",
        "dataset_version",
        "collection",
        "night_number",
        "lights_off_seconds",
        "treatment",
        "silver_recording_id",
        "recording_start",
        "duration_seconds",
        "channel_count",
        "annotation_count",
        "in_range_epoch_count",
        "out_of_range_epoch_count",
        "trailing_overhang_seconds",
        "psg_file_id",
        "hypnogram_file_id",
        "source_pair_id",
        "input_fingerprint",
        "config_id",
        "schema_version",
        "transform_version",
        "psg_checksum_sha256",
        "hypnogram_checksum_sha256",
        "silver_bucket",
        "silver_output_prefix",
        "staging_load_run_id",
        "first_loaded_at",
        "last_loaded_at",
    },
    "dim_channel": {
        "channel_sk",
        "recording_sk",
        "silver_channel_id",
        "silver_recording_id",
        "position",
        "source_label",
        "normalized_name",
        "sampling_frequency_hz",
        "physical_dimension",
        "physical_min",
        "physical_max",
        "digital_min",
        "digital_max",
        "samples_per_data_record",
        "prefiltering",
        "first_loaded_at",
        "last_loaded_at",
    },
    "dim_sleep_stage": {
        "sleep_stage_sk",
        "silver_stage_code",
        "analytical_stage_code",
    },
    "fact_sleep_epoch": {
        "sleep_epoch_sk",
        "subject_sk",
        "recording_sk",
        "sleep_stage_sk",
        "silver_epoch_id",
        "silver_recording_id",
        "source_interval_id",
        "source_annotation_index",
        "epoch_number",
        "start_seconds",
        "duration_seconds",
        "end_seconds",
        "source_label",
        "silver_stage_code",
        "staging_load_run_id",
        "loaded_at",
    },
}

EXPECTED_CONTRACT_PATHS = {
    "dim_subject": "contracts/warehouse_dim_subject.yml",
    "dim_recording": "contracts/warehouse_dim_recording.yml",
    "dim_channel": "contracts/warehouse_dim_channel.yml",
    "dim_sleep_stage": "contracts/warehouse_dim_sleep_stage.yml",
    "fact_sleep_epoch": "contracts/warehouse_fact_sleep_epoch.yml",
}

EXPECTED_SENSITIVE_POLICIES = {
    ("dim_subject", "source_subject_id"): (
        True,
        True,
        False,
        "restricted",
        "redact",
    ),
    ("dim_subject", "source_subject_number"): (
        True,
        True,
        False,
        "restricted",
        "redact",
    ),
    ("dim_subject", "age_years"): (
        True,
        True,
        False,
        "restricted",
        "aggregate_only",
    ),
    ("dim_subject", "sex"): (
        True,
        True,
        False,
        "restricted",
        "aggregate_only",
    ),
    ("dim_recording", "lights_off_seconds"): (
        True,
        True,
        False,
        "restricted",
        "aggregate_only",
    ),
    ("dim_recording", "treatment"): (
        True,
        True,
        False,
        "restricted",
        "aggregate_only",
    ),
    ("fact_sleep_epoch", "subject_sk"): (
        True,
        True,
        False,
        "restricted",
        "none",
    ),
}


def run_smoke_test() -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select table_name, column_name
                from information_schema.columns
                where table_schema = 'warehouse';
                """
            )
            physical_columns: dict[str, set[str]] = {}
            for table_name, column_name in cursor.fetchall():
                physical_columns.setdefault(table_name, set()).add(
                    column_name
                )

            for table_name, expected_columns in EXPECTED_COLUMNS.items():
                actual_columns = physical_columns.get(table_name, set())
                if actual_columns != expected_columns:
                    raise RuntimeError(
                        f"Unexpected Warehouse columns for {table_name}: "
                        f"expected={sorted(expected_columns)}, "
                        f"actual={sorted(actual_columns)}"
                    )

            cursor.execute(
                """
                select
                    table_name,
                    contract_path,
                    owner_role,
                    data_layer,
                    status
                from governance.data_contract_registry
                where table_schema = 'warehouse'
                  and contract_version = 'v1';
                """
            )
            contract_rows = {
                row[0]: row[1:]
                for row in cursor.fetchall()
            }

            if set(contract_rows) != set(EXPECTED_CONTRACT_PATHS):
                raise RuntimeError(
                    "Warehouse contract registry is incomplete: "
                    f"{contract_rows}"
                )

            for table_name, expected_path in (
                EXPECTED_CONTRACT_PATHS.items()
            ):
                contract_path, owner_role, data_layer, status = (
                    contract_rows[table_name]
                )
                if contract_path != expected_path:
                    raise RuntimeError(
                        f"Unexpected Warehouse contract path for "
                        f"{table_name}: {contract_path}"
                    )
                if owner_role != "data_engineer":
                    raise RuntimeError(
                        f"Unexpected owner for {table_name}: {owner_role}"
                    )
                if data_layer != "warehouse":
                    raise RuntimeError(
                        f"Unexpected data layer for {table_name}: "
                        f"{data_layer}"
                    )
                if status != "active":
                    raise RuntimeError(
                        f"Warehouse contract is not active for "
                        f"{table_name}: {status}"
                    )

            cursor.execute(
                """
                select
                    table_name,
                    column_name,
                    data_layer,
                    contains_direct_identifier
                from governance.column_classification
                where table_schema = 'warehouse';
                """
            )
            classified_columns: dict[str, set[str]] = {}
            for (
                table_name,
                column_name,
                data_layer,
                contains_direct_identifier,
            ) in cursor.fetchall():
                if data_layer != "warehouse":
                    raise RuntimeError(
                        f"Unexpected classification layer for "
                        f"{table_name}.{column_name}: {data_layer}"
                    )
                if contains_direct_identifier:
                    raise RuntimeError(
                        "Warehouse direct identifier flag must remain false: "
                        f"{table_name}.{column_name}"
                    )
                classified_columns.setdefault(table_name, set()).add(
                    column_name
                )

            if classified_columns != EXPECTED_COLUMNS:
                raise RuntimeError(
                    "Warehouse column classification does not exactly "
                    f"match physical columns: {classified_columns}"
                )

            cursor.execute(
                """
                select
                    table_name,
                    column_name,
                    contains_personal_data,
                    contains_health_data,
                    contains_direct_identifier,
                    access_policy,
                    masking_policy
                from governance.column_classification
                where table_schema = 'warehouse'
                  and (table_name, column_name) in (
                      ('dim_subject', 'source_subject_id'),
                      ('dim_subject', 'source_subject_number'),
                      ('dim_subject', 'age_years'),
                      ('dim_subject', 'sex'),
                      ('dim_recording', 'lights_off_seconds'),
                      ('dim_recording', 'treatment'),
                      ('fact_sleep_epoch', 'subject_sk')
                  );
                """
            )
            sensitive_rows = {
                (row[0], row[1]): row[2:]
                for row in cursor.fetchall()
            }
            if sensitive_rows != EXPECTED_SENSITIVE_POLICIES:
                raise RuntimeError(
                    "Unexpected Warehouse sensitive policies: "
                    f"{sensitive_rows}"
                )

            cursor.execute(
                """
                select
                    classification_level,
                    contains_personal_data,
                    contains_health_data,
                    access_policy,
                    masking_policy
                from governance.column_classification
                where table_schema = 'warehouse'
                  and table_name = 'dim_sleep_stage';
                """
            )
            stage_rows = cursor.fetchall()
            expected_stage_policy = (
                "internal",
                False,
                False,
                "team_only",
                "none",
            )
            if len(stage_rows) != 3 or any(
                row != expected_stage_policy
                for row in stage_rows
            ):
                raise RuntimeError(
                    "Controlled sleep-stage dimension classification "
                    "is incorrect"
                )

    print("warehouse_contracts_active=5")
    print("warehouse_classified_columns=81")
    print("warehouse_direct_identifier_columns=0")
    print("warehouse_sensitive_policy_check=true")
    print("warehouse_reference_dimension_policy_check=true")
    print("warehouse_governance_schema_smoke_status=success")


if __name__ == "__main__":
    run_smoke_test()
