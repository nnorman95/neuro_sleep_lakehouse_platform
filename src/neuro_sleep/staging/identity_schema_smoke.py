from neuro_sleep.db.postgres import (
    get_postgres_connection,
)


EXPECTED_LINEAGE_COLUMNS = {
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
    "loaded_at",
}

EXPECTED_LOGICAL_IDENTITY_COLUMNS = {
    "dataset_version",
    "collection",
    "recording_key",
}

EXPECTED_RECORDING_CONSTRAINTS = {
    "silver_recordings_psg_file_fk",
    "silver_recordings_hypnogram_file_fk",
    "silver_recordings_staging_load_run_fk",
    "silver_recordings_versioned_identity_unique",
    "silver_recordings_output_location_unique",
    "silver_recordings_dataset_version_nonempty",
    "silver_recordings_collection_check",
    "silver_recordings_recording_key_nonempty",
}


def run_smoke_test() -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    column_name,
                    is_nullable
                from information_schema.columns
                where table_schema = 'staging'
                  and table_name = 'silver_recordings';
                """
            )

            column_rows = cursor.fetchall()
            columns = {
                row[0]
                for row in column_rows
            }
            nullable_by_column = {
                row[0]: row[1]
                for row in column_rows
            }

            expected_not_null = (
                EXPECTED_LINEAGE_COLUMNS
                | EXPECTED_LOGICAL_IDENTITY_COLUMNS
            )
            missing_columns = (
                expected_not_null - columns
            )
            if missing_columns:
                raise RuntimeError(
                    "Missing Silver recording "
                    "identity/lineage columns: "
                    f"{sorted(missing_columns)}"
                )

            nullable_columns = {
                column_name
                for column_name
                in expected_not_null
                if nullable_by_column[
                    column_name
                ] != "NO"
            }
            if nullable_columns:
                raise RuntimeError(
                    "Silver recording identity/"
                    "lineage columns must be "
                    "NOT NULL: "
                    f"{sorted(nullable_columns)}"
                )

            cursor.execute(
                """
                select conname
                from pg_constraint
                where conrelid = (
                    'staging.silver_recordings'
                    ::regclass
                );
                """
            )

            recording_constraints = {
                row[0]
                for row in cursor.fetchall()
            }
            missing_constraints = (
                EXPECTED_RECORDING_CONSTRAINTS
                - recording_constraints
            )
            if missing_constraints:
                raise RuntimeError(
                    "Missing Silver recording "
                    "constraints: "
                    f"{sorted(missing_constraints)}"
                )

            if (
                "silver_recordings_"
                "source_objects_unique"
                in recording_constraints
            ):
                raise RuntimeError(
                    "Legacy source-path-only "
                    "unique constraint remains"
                )

            cursor.execute(
                """
                select indexname
                from pg_indexes
                where schemaname = 'staging'
                  and tablename = 'silver_recordings';
                """
            )
            indexes = {
                row[0]
                for row in cursor.fetchall()
            }
            if (
                "idx_silver_recordings_"
                "logical_recording"
                not in indexes
            ):
                raise RuntimeError(
                    "Logical recording index "
                    "is missing"
                )

            cursor.execute(
                """
                select conname
                from pg_constraint
                where conrelid = (
                    'staging.'
                    'silver_sleep_stage_intervals'
                    ::regclass
                );
                """
            )
            interval_constraints = {
                row[0]
                for row in cursor.fetchall()
            }
            if (
                "silver_sleep_stage_intervals_"
                "onset_nonnegative"
                in interval_constraints
            ):
                raise RuntimeError(
                    "Negative source annotation "
                    "onsets remain blocked"
                )

            cursor.execute(
                """
                select
                    table_name,
                    contract_version,
                    status
                from governance.data_contract_registry
                where table_schema = 'staging'
                  and table_name in (
                      'silver_recordings',
                      'silver_sleep_stage_intervals'
                  )
                  and contract_version in (
                      'v1',
                      'v2',
                      'v3'
                  );
                """
            )
            contract_rows = cursor.fetchall()

            status_by_contract = {
                (
                    row[0],
                    row[1],
                ): row[2]
                for row in contract_rows
            }

            expected_contract_status = {
                (
                    "silver_recordings",
                    "v1",
                ): "deprecated",
                (
                    "silver_recordings",
                    "v2",
                ): "deprecated",
                (
                    "silver_recordings",
                    "v3",
                ): "active",
                (
                    "silver_sleep_stage_intervals",
                    "v1",
                ): "deprecated",
                (
                    "silver_sleep_stage_intervals",
                    "v2",
                ): "active",
            }

            for key, expected_status in (
                expected_contract_status.items()
            ):
                if (
                    status_by_contract.get(key)
                    != expected_status
                ):
                    raise RuntimeError(
                        "Unexpected staging "
                        "contract status for "
                        f"{key}: "
                        f"{status_by_contract.get(key)}"
                    )

            cursor.execute(
                """
                select count(*)
                from governance.column_classification
                where table_schema = 'staging'
                  and table_name = 'silver_recordings';
                """
            )
            classification_count = (
                cursor.fetchone()[0]
            )
            if classification_count != 29:
                raise RuntimeError(
                    "Unexpected Silver recording "
                    "classification count: "
                    f"{classification_count}"
                )

            cursor.execute(
                """
                select
                    column_name,
                    contains_personal_data,
                    contains_health_data,
                    access_policy
                from governance.column_classification
                where table_schema = 'staging'
                  and table_name = 'silver_recordings'
                  and column_name in (
                      'dataset_version',
                      'collection',
                      'recording_key'
                  );
                """
            )
            identity_classification = {
                row[0]: row[1:]
                for row in cursor.fetchall()
            }

            if set(identity_classification) != (
                EXPECTED_LOGICAL_IDENTITY_COLUMNS
            ):
                raise RuntimeError(
                    "Logical recording identity "
                    "classification is incomplete"
                )

            if identity_classification[
                "recording_key"
            ] != (
                True,
                True,
                "restricted",
            ):
                raise RuntimeError(
                    "recording_key sensitivity "
                    "classification is incorrect"
                )

    print(
        "silver_recording_lineage_columns=13"
    )
    print(
        "silver_recording_logical_identity_columns=3"
    )
    print(
        "logical_recording_identity_not_null=true"
    )
    print(
        "logical_recording_index=true"
    )
    print(
        "source_path_only_uniqueness_removed=true"
    )
    print(
        "versioned_identity_constraint=true"
    )
    print(
        "silver_output_location_constraint=true"
    )
    print(
        "negative_interval_onset_allowed=true"
    )
    print(
        "active_silver_recordings_contract_v3=true"
    )
    print(
        "active_silver_intervals_contract_v2=true"
    )
    print(
        "silver_recording_classified_columns=29"
    )
    print(
        "staging_identity_schema_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
