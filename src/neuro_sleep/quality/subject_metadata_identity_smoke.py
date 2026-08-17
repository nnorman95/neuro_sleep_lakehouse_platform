from __future__ import annotations

import pyarrow as pa

from neuro_sleep.silver.subject_parquet import (
    RECORDING_CONTEXTS_SCHEMA,
    SUBJECTS_SCHEMA,
)
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
)
from neuro_sleep.staging.subject_metadata_loader import (
    SubjectMetadataDataObject,
    SubjectMetadataPublication,
    _validate_table_identity,
)


DATASET_VERSION = "1.0.0"
SUBJECT_KEY = "a" * 64
OTHER_SUBJECT_KEY = "b" * 64


def _publication() -> SubjectMetadataPublication:
    subjects_object = SubjectMetadataDataObject(
        dataset_name="subjects",
        object_key=(
            "phase12-fixtures/subjects.parquet"
        ),
        row_count=1,
        file_size_bytes=1,
        checksum_sha256="1" * 64,
    )
    contexts_object = SubjectMetadataDataObject(
        dataset_name="recording_contexts",
        object_key=(
            "phase12-fixtures/"
            "recording_contexts.parquet"
        ),
        row_count=1,
        file_size_bytes=1,
        checksum_sha256="2" * 64,
    )
    return SubjectMetadataPublication(
        silver_bucket="silver",
        output_prefix=(
            "phase12-fixtures/subject-metadata"
        ),
        input_fingerprint="3" * 64,
        source_system=SOURCE_SYSTEM,
        dataset_version=DATASET_VERSION,
        schema_version="1.0.0",
        transform_version="1.0.0",
        subject_count=1,
        recording_context_count=1,
        subjects_object=subjects_object,
        recording_contexts_object=contexts_object,
    )


def _baseline_tables() -> tuple[
    pa.Table,
    pa.Table,
]:
    subjects = pa.Table.from_pylist(
        [
            {
                "subject_key": SUBJECT_KEY,
                "source_system": SOURCE_SYSTEM,
                "dataset_version": DATASET_VERSION,
                "collection": "sleep-cassette",
                "source_subject_id": "SC4001",
                "source_subject_number": 1,
                "age_years": 33,
                "sex": "M",
                "source_bucket": "bronze",
                "source_object_key": (
                    "phase12-fixtures/"
                    "subjects.xlsx"
                ),
            }
        ],
        schema=SUBJECTS_SCHEMA,
    )

    contexts = pa.Table.from_pylist(
        [
            {
                "recording_key": (
                    "SC4001E0-PSG"
                ),
                "subject_key": SUBJECT_KEY,
                "source_system": SOURCE_SYSTEM,
                "dataset_version": DATASET_VERSION,
                "collection": "sleep-cassette",
                "night_number": 1,
                "lights_off_seconds": 0,
                "treatment": None,
                "source_bucket": "bronze",
                "source_object_key": (
                    "phase12-fixtures/"
                    "recording-contexts.xlsx"
                ),
            }
        ],
        schema=RECORDING_CONTEXTS_SCHEMA,
    )

    return subjects, contexts


def _with_rows(
    table: pa.Table,
    rows: list[dict[str, object]],
) -> pa.Table:
    return pa.Table.from_pylist(
        rows,
        schema=table.schema,
    )


def _validate(
    *,
    subjects: pa.Table,
    contexts: pa.Table,
) -> None:
    _validate_table_identity(
        publication=_publication(),
        subjects_table=subjects,
        contexts_table=contexts,
    )


def _expect_failure(
    *,
    fixture_name: str,
    expected_message: str,
    subjects: pa.Table,
    contexts: pa.Table,
    prefix_match: bool = False,
) -> None:
    try:
        _validate(
            subjects=subjects,
            contexts=contexts,
        )
    except RuntimeError as error:
        actual = str(error)
        matched = (
            actual.startswith(expected_message)
            if prefix_match
            else actual == expected_message
        )
        if not matched:
            raise RuntimeError(
                f"{fixture_name} failed for an "
                "unexpected reason: "
                f"{actual}"
            ) from error

        print(
            "subject_metadata_identity_"
            f"{fixture_name}_blocked=true"
        )
        return

    raise RuntimeError(
        "Subject-metadata identity fixture "
        f"was accepted: {fixture_name}"
    )


def run_smoke_test() -> None:
    subjects, contexts = _baseline_tables()

    _validate(
        subjects=subjects,
        contexts=contexts,
    )
    print(
        "subject_metadata_identity_"
        "valid_baseline=true"
    )

    duplicate_subject_rows = (
        subjects.to_pylist()
    )
    duplicate_subject_rows.append(
        dict(duplicate_subject_rows[0])
    )
    _expect_failure(
        fixture_name="duplicate_subject_key",
        expected_message=(
            "Duplicate subject_key values in "
            "Silver subjects Parquet"
        ),
        subjects=_with_rows(
            subjects,
            duplicate_subject_rows,
        ),
        contexts=contexts,
    )

    duplicate_context_rows = (
        contexts.to_pylist()
    )
    duplicate_context_rows.append(
        dict(duplicate_context_rows[0])
    )
    _expect_failure(
        fixture_name=(
            "duplicate_recording_identity"
        ),
        expected_message=(
            "Duplicate logical recording "
            "identities in Silver recording "
            "contexts Parquet"
        ),
        subjects=subjects,
        contexts=_with_rows(
            contexts,
            duplicate_context_rows,
        ),
    )

    missing_subject_rows = (
        contexts.to_pylist()
    )
    missing_subject_rows[0][
        "subject_key"
    ] = OTHER_SUBJECT_KEY
    _expect_failure(
        fixture_name="missing_subject_reference",
        expected_message=(
            "Recording contexts reference "
            "subjects absent from subjects "
            "Parquet: "
        ),
        subjects=subjects,
        contexts=_with_rows(
            contexts,
            missing_subject_rows,
        ),
        prefix_match=True,
    )

    wrong_source_rows = (
        contexts.to_pylist()
    )
    wrong_source_rows[0][
        "source_system"
    ] = "unexpected_source"
    _expect_failure(
        fixture_name="source_system_mismatch",
        expected_message=(
            "recording_contexts source_system "
            "does not match the manifest"
        ),
        subjects=subjects,
        contexts=_with_rows(
            contexts,
            wrong_source_rows,
        ),
    )

    wrong_version_rows = (
        subjects.to_pylist()
    )
    wrong_version_rows[0][
        "dataset_version"
    ] = "9.9.9"
    _expect_failure(
        fixture_name="dataset_version_mismatch",
        expected_message=(
            "subjects dataset_version does "
            "not match the manifest"
        ),
        subjects=_with_rows(
            subjects,
            wrong_version_rows,
        ),
        contexts=contexts,
    )

    print(
        "phase12_subject_metadata_identity_"
        "smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
