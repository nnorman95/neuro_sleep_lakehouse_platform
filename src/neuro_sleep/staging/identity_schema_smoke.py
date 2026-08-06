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

EXPECTED_NEW_CONSTRAINTS = {
    "silver_recordings_psg_file_fk",
    "silver_recordings_hypnogram_file_fk",
    "silver_recordings_staging_load_run_fk",
    "silver_recordings_versioned_identity_unique",
    "silver_recordings_output_location_unique",
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

            missing_columns = (
                EXPECTED_LINEAGE_COLUMNS
                - columns
            )

            if missing_columns:
                raise RuntimeError(
                    "Missing Silver lineage "
                    f"columns: {sorted(missing_columns)}"
                )

            nullable_lineage_columns = {
                column_name
                for column_name
                in EXPECTED_LINEAGE_COLUMNS
                if nullable_by_column[
                    column_name
                ] != "NO"
            }

            if nullable_lineage_columns:
                raise RuntimeError(
                    "Silver lineage columns "
                    "must be NOT NULL: "
                    f"{sorted(nullable_lineage_columns)}"
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
                EXPECTED_NEW_CONSTRAINTS
                - recording_constraints
            )

            if missing_constraints:
                raise RuntimeError(
                    "Missing Silver identity "
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
                      'v2'
                  );
                """
            )

            contract_rows = cursor.fetchall()

            active_v2_count = sum(
                1
                for row in contract_rows
                if row[1] == "v2"
                and row[2] == "active"
            )

            deprecated_v1_count = sum(
                1
                for row in contract_rows
                if row[1] == "v1"
                and row[2] == "deprecated"
            )

            if active_v2_count != 2:
                raise RuntimeError(
                    "Expected two active staging "
                    "contract v2 rows"
                )

            if deprecated_v1_count != 2:
                raise RuntimeError(
                    "Expected two deprecated "
                    "staging contract v1 rows"
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

            if classification_count != 26:
                raise RuntimeError(
                    "Unexpected Silver recording "
                    "classification count: "
                    f"{classification_count}"
                )

    print(
        "silver_recording_lineage_columns=13"
    )
    print(
        "lineage_columns_not_null=true"
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
        "active_staging_contract_v2=2"
    )
    print(
        "deprecated_staging_contract_v1=2"
    )
    print(
        "silver_recording_classified_columns=26"
    )
    print(
        "staging_identity_schema_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
