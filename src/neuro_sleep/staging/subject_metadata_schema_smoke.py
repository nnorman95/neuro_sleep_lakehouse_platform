from neuro_sleep.db.postgres import (
    get_postgres_connection,
)


EXPECTED_COLUMNS = {
    "silver_subjects": {
        "subject_key",
        "source_system",
        "dataset_version",
        "collection",
        "source_subject_id",
        "source_subject_number",
        "age_years",
        "sex",
        "source_bucket",
        "source_object_key",
        "metadata_input_fingerprint",
        "schema_version",
        "transform_version",
        "silver_bucket",
        "silver_output_prefix",
        "staging_load_run_id",
        "loaded_at",
    },
    "silver_recording_contexts": {
        "recording_key",
        "subject_key",
        "source_system",
        "dataset_version",
        "collection",
        "night_number",
        "lights_off_seconds",
        "treatment",
        "source_bucket",
        "source_object_key",
        "metadata_input_fingerprint",
        "schema_version",
        "transform_version",
        "silver_bucket",
        "silver_output_prefix",
        "staging_load_run_id",
        "loaded_at",
    },
}

EXPECTED_NULLABLE = {
    "silver_subjects": set(),
    "silver_recording_contexts": {
        "treatment",
    },
}

EXPECTED_CONSTRAINTS = {
    "silver_subjects": {
        "silver_subjects_pkey",
        "silver_subjects_staging_load_run_fk",
        "silver_subjects_subject_key_format",
        "silver_subjects_input_fingerprint_format",
        "silver_subjects_collection_check",
        "silver_subjects_subject_number_nonnegative",
        "silver_subjects_age_range",
        "silver_subjects_sex_check",
        "silver_subjects_publication_identity_unique",
        "silver_subjects_source_identity_unique",
        "silver_subjects_output_row_unique",
    },
    "silver_recording_contexts": {
        "silver_recording_contexts_pkey",
        "silver_recording_contexts_subject_fk",
        "silver_recording_contexts_staging_load_run_fk",
        "silver_recording_contexts_subject_key_format",
        "silver_recording_contexts_input_fingerprint_format",
        "silver_recording_contexts_collection_check",
        "silver_recording_contexts_night_positive",
        "silver_recording_contexts_lights_off_range",
        "silver_recording_contexts_treatment_check",
        "silver_recording_contexts_output_row_unique",
    },
}

EXPECTED_PRIMARY_KEYS = {
    "silver_subjects": (
        "PRIMARY KEY "
        "(subject_key, metadata_input_fingerprint)"
    ),
    "silver_recording_contexts": (
        "PRIMARY KEY "
        "(source_system, dataset_version, collection, "
        "recording_key, metadata_input_fingerprint)"
    ),
}

EXPECTED_CONTRACT_PATHS = {
    "silver_subjects": (
        "contracts/staging_silver_subjects.yml"
    ),
    "silver_recording_contexts": (
        "contracts/staging_silver_recording_contexts.yml"
    ),
}

EXPECTED_SENSITIVE_POLICIES = {
    (
        "silver_subjects",
        "source_subject_id",
    ): ("restricted", "redact"),
    (
        "silver_subjects",
        "source_subject_number",
    ): ("restricted", "redact"),
    (
        "silver_subjects",
        "age_years",
    ): ("restricted", "aggregate_only"),
    (
        "silver_subjects",
        "sex",
    ): ("restricted", "aggregate_only"),
    (
        "silver_recording_contexts",
        "lights_off_seconds",
    ): ("restricted", "aggregate_only"),
    (
        "silver_recording_contexts",
        "treatment",
    ): ("restricted", "aggregate_only"),
}


def _read_columns(cursor, table_name: str) -> dict[str, str]:
    cursor.execute(
        """
        select
            column_name,
            is_nullable
        from information_schema.columns
        where table_schema = 'staging'
          and table_name = %s;
        """,
        (table_name,),
    )

    return {
        row[0]: row[1]
        for row in cursor.fetchall()
    }


def _read_constraints(cursor, table_name: str) -> set[str]:
    cursor.execute(
        """
        select conname
        from pg_constraint
        where conrelid = (
            ('staging.' || %s)::regclass
        );
        """,
        (table_name,),
    )

    return {
        row[0]
        for row in cursor.fetchall()
    }


def _read_primary_key(cursor, table_name: str) -> str:
    cursor.execute(
        """
        select pg_get_constraintdef(oid)
        from pg_constraint
        where conrelid = (
            ('staging.' || %s)::regclass
        )
          and contype = 'p';
        """,
        (table_name,),
    )

    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(
            f"Missing primary key for {table_name}"
        )

    return row[0]


def run_smoke_test() -> None:
    with get_postgres_connection() as connection:
        with connection.cursor() as cursor:
            for table_name, expected_columns in (
                EXPECTED_COLUMNS.items()
            ):
                nullable_by_column = _read_columns(
                    cursor,
                    table_name,
                )
                actual_columns = set(
                    nullable_by_column
                )

                if actual_columns != expected_columns:
                    raise RuntimeError(
                        f"Unexpected columns for {table_name}: "
                        f"expected={sorted(expected_columns)}, "
                        f"actual={sorted(actual_columns)}"
                    )

                actual_nullable = {
                    column_name
                    for column_name, is_nullable
                    in nullable_by_column.items()
                    if is_nullable == "YES"
                }
                if (
                    actual_nullable
                    != EXPECTED_NULLABLE[table_name]
                ):
                    raise RuntimeError(
                        f"Unexpected nullability for "
                        f"{table_name}: "
                        f"{sorted(actual_nullable)}"
                    )

                constraints = _read_constraints(
                    cursor,
                    table_name,
                )
                missing_constraints = (
                    EXPECTED_CONSTRAINTS[table_name]
                    - constraints
                )
                if missing_constraints:
                    raise RuntimeError(
                        f"Missing constraints for "
                        f"{table_name}: "
                        f"{sorted(missing_constraints)}"
                    )

                actual_primary_key = (
                    _read_primary_key(
                        cursor,
                        table_name,
                    )
                )
                expected_primary_key = (
                    EXPECTED_PRIMARY_KEYS[table_name]
                )
                if (
                    actual_primary_key
                    != expected_primary_key
                ):
                    raise RuntimeError(
                        f"Unexpected primary key for "
                        f"{table_name}: "
                        f"{actual_primary_key}"
                    )

            cursor.execute(
                """
                select
                    table_name,
                    contract_path,
                    status
                from governance.data_contract_registry
                where table_schema = 'staging'
                  and table_name in (
                      'silver_subjects',
                      'silver_recording_contexts'
                  )
                  and contract_version = 'v1';
                """
            )
            contract_rows = {
                row[0]: (
                    row[1],
                    row[2],
                )
                for row in cursor.fetchall()
            }

            if set(contract_rows) != set(
                EXPECTED_CONTRACT_PATHS
            ):
                raise RuntimeError(
                    "Missing subject metadata contracts: "
                    f"{contract_rows}"
                )

            for table_name, expected_path in (
                EXPECTED_CONTRACT_PATHS.items()
            ):
                actual_path, status = (
                    contract_rows[table_name]
                )
                if actual_path != expected_path:
                    raise RuntimeError(
                        f"Unexpected contract path for "
                        f"{table_name}: {actual_path}"
                    )
                if status != "active":
                    raise RuntimeError(
                        f"Contract is not active for "
                        f"{table_name}: {status}"
                    )

            cursor.execute(
                """
                select
                    table_name,
                    count(*)
                from governance.column_classification
                where table_schema = 'staging'
                  and table_name in (
                      'silver_subjects',
                      'silver_recording_contexts'
                  )
                group by table_name;
                """
            )
            classification_counts = {
                row[0]: row[1]
                for row in cursor.fetchall()
            }

            expected_counts = {
                "silver_subjects": 17,
                "silver_recording_contexts": 17,
            }
            if (
                classification_counts
                != expected_counts
            ):
                raise RuntimeError(
                    "Unexpected subject metadata "
                    "classification counts: "
                    f"{classification_counts}"
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
                where table_schema = 'staging'
                  and (
                      table_name,
                      column_name
                  ) in (
                      ('silver_subjects', 'source_subject_id'),
                      ('silver_subjects', 'source_subject_number'),
                      ('silver_subjects', 'age_years'),
                      ('silver_subjects', 'sex'),
                      ('silver_recording_contexts', 'lights_off_seconds'),
                      ('silver_recording_contexts', 'treatment')
                  );
                """
            )
            sensitive_rows = {
                (
                    row[0],
                    row[1],
                ): row[2:]
                for row in cursor.fetchall()
            }

            if set(sensitive_rows) != set(
                EXPECTED_SENSITIVE_POLICIES
            ):
                raise RuntimeError(
                    "Missing sensitive subject metadata "
                    f"classifications: {sensitive_rows}"
                )

            for key, expected_policy in (
                EXPECTED_SENSITIVE_POLICIES.items()
            ):
                (
                    contains_personal_data,
                    contains_health_data,
                    contains_direct_identifier,
                    access_policy,
                    masking_policy,
                ) = sensitive_rows[key]

                if not contains_personal_data:
                    raise RuntimeError(
                        f"Personal-data flag missing: {key}"
                    )
                if not contains_health_data:
                    raise RuntimeError(
                        f"Health-data flag missing: {key}"
                    )
                if contains_direct_identifier:
                    raise RuntimeError(
                        f"Unexpected direct identifier: {key}"
                    )
                if (
                    access_policy,
                    masking_policy,
                ) != expected_policy:
                    raise RuntimeError(
                        f"Unexpected policy for {key}: "
                        f"{access_policy}, "
                        f"{masking_policy}"
                    )

    print("subject_metadata_staging_tables=2")
    print("subject_metadata_staging_columns=34")
    print("active_subject_metadata_contracts=2")
    print("classified_subject_metadata_columns=34")
    print("subject_metadata_sensitive_policy_check=true")
    print(
        "subject_metadata_staging_schema_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
