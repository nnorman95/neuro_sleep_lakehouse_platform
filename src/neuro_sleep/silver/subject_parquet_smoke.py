from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from neuro_sleep.silver.parquet_tables import (
    write_silver_parquet,
)
from neuro_sleep.silver.subject_metadata import (
    merge_subject_metadata_bundles,
    parse_sc_rows,
    parse_st_rows,
)
from neuro_sleep.silver.subject_parquet import (
    RECORDING_CONTEXTS_SCHEMA,
    SUBJECTS_SCHEMA,
    build_subject_key,
    recording_contexts_to_table,
    subjects_to_table,
)


SOURCE_SYSTEM = "physionet_sleep_edf"
DATASET_VERSION = "1.0.0"
SOURCE_BUCKET = "bronze"

SOURCE_OBJECT_KEYS = {
    "sleep-cassette": (
        "physionet/sleep-edfx/1.0.0/"
        "SC-subjects.xls"
    ),
    "sleep-telemetry": (
        "physionet/sleep-edfx/1.0.0/"
        "ST-subjects.xls"
    ),
}


def expect_value_error(
    function,
) -> None:
    try:
        function()

    except ValueError:
        return

    raise RuntimeError(
        "Expected ValueError was not raised"
    )


def run_smoke_test() -> None:
    sc_bundle = parse_sc_rows(
        (
            (
                0.0,
                1.0,
                33.0,
                1.0,
                0.02638888888888889,
            ),
            (
                0.0,
                2.0,
                33.0,
                1.0,
                0.9145833333333333,
            ),
            (
                1.0,
                1.0,
                33.0,
                2.0,
                0.9472222222222223,
            ),
            (
                1.0,
                2.0,
                33.0,
                2.0,
                0.9270833333333334,
            ),
        )
    )

    st_bundle = parse_st_rows(
        (
            (
                1.0,
                60.0,
                1.0,
                1.0,
                0.9590277777777777,
                2.0,
                0.9916666666666667,
            ),
        )
    )

    bundle = (
        merge_subject_metadata_bundles(
            sc_bundle,
            st_bundle,
        )
    )

    subjects_table = subjects_to_table(
        bundle.subjects,
        source_system=SOURCE_SYSTEM,
        dataset_version=DATASET_VERSION,
        source_bucket=SOURCE_BUCKET,
        source_object_keys=(
            SOURCE_OBJECT_KEYS
        ),
    )

    contexts_table = (
        recording_contexts_to_table(
            bundle.recording_contexts,
            source_system=SOURCE_SYSTEM,
            dataset_version=(
                DATASET_VERSION
            ),
            source_bucket=SOURCE_BUCKET,
            source_object_keys=(
                SOURCE_OBJECT_KEYS
            ),
        )
    )

    if not subjects_table.schema.equals(
        SUBJECTS_SCHEMA,
        check_metadata=True,
    ):
        raise RuntimeError(
            "Unexpected subjects schema"
        )

    if not contexts_table.schema.equals(
        RECORDING_CONTEXTS_SCHEMA,
        check_metadata=True,
    ):
        raise RuntimeError(
            "Unexpected recording-contexts "
            "schema"
        )

    if subjects_table.num_rows != 3:
        raise RuntimeError(
            "Unexpected subject row count"
        )

    if contexts_table.num_rows != 6:
        raise RuntimeError(
            "Unexpected recording-context "
            "row count"
        )

    subjects = {
        row["source_subject_id"]: row
        for row in subjects_table.to_pylist()
    }

    contexts = {
        row["recording_key"]: row
        for row in contexts_table.to_pylist()
    }

    if subjects["SC00"]["sex"] != "F":
        raise RuntimeError(
            "Cassette female normalization "
            "was not preserved"
        )

    if subjects["SC01"]["sex"] != "M":
        raise RuntimeError(
            "Cassette male normalization "
            "was not preserved"
        )

    expected_subject_key = (
        build_subject_key(
            source_system=SOURCE_SYSTEM,
            dataset_version=(
                DATASET_VERSION
            ),
            collection=(
                "sleep-telemetry"
            ),
            source_subject_id="ST01",
        )
    )

    if (
        contexts["ST7011J"][
            "subject_key"
        ]
        != expected_subject_key
    ):
        raise RuntimeError(
            "Recording-to-subject key "
            "mapping failed"
        )

    if (
        contexts["ST7011J"][
            "treatment"
        ]
        != "placebo"
    ):
        raise RuntimeError(
            "Placebo treatment was not "
            "preserved"
        )

    if (
        contexts["SC4001E"][
            "treatment"
        ]
        is not None
    ):
        raise RuntimeError(
            "Cassette treatment must be null"
        )

    repeated_subject_key = (
        build_subject_key(
            source_system=SOURCE_SYSTEM,
            dataset_version=(
                DATASET_VERSION
            ),
            collection=(
                "sleep-telemetry"
            ),
            source_subject_id="ST01",
        )
    )

    if repeated_subject_key != (
        expected_subject_key
    ):
        raise RuntimeError(
            "Subject key is not stable"
        )

    changed_subject_key = (
        build_subject_key(
            source_system=SOURCE_SYSTEM,
            dataset_version=(
                DATASET_VERSION
            ),
            collection=(
                "sleep-telemetry"
            ),
            source_subject_id="ST02",
        )
    )

    if changed_subject_key == (
        expected_subject_key
    ):
        raise RuntimeError(
            "Different subjects share a key"
        )

    with TemporaryDirectory(
        prefix=(
            "neuro_sleep_subject_parquet_"
        )
    ) as temporary_directory:
        root = Path(
            temporary_directory
        )

        for dataset_name, table in (
            (
                "subjects",
                subjects_table,
            ),
            (
                "recording_contexts",
                contexts_table,
            ),
        ):
            output_path = (
                root
                / f"{dataset_name}.parquet"
            )

            write_silver_parquet(
                table=table,
                output_path=output_path,
            )

            restored = pq.read_table(
                output_path
            )

            if restored.num_rows != (
                table.num_rows
            ):
                raise RuntimeError(
                    "Parquet row-count "
                    "round-trip failed: "
                    f"{dataset_name}"
                )

            if not restored.schema.equals(
                table.schema,
                check_metadata=True,
            ):
                raise RuntimeError(
                    "Parquet schema "
                    "round-trip failed: "
                    f"{dataset_name}"
                )

    expect_value_error(
        lambda: subjects_to_table(
            (),
            source_system=SOURCE_SYSTEM,
            dataset_version=(
                DATASET_VERSION
            ),
            source_bucket=SOURCE_BUCKET,
            source_object_keys=(
                SOURCE_OBJECT_KEYS
            ),
        )
    )

    expect_value_error(
        lambda: (
            recording_contexts_to_table(
                (),
                source_system=(
                    SOURCE_SYSTEM
                ),
                dataset_version=(
                    DATASET_VERSION
                ),
                source_bucket=(
                    SOURCE_BUCKET
                ),
                source_object_keys=(
                    SOURCE_OBJECT_KEYS
                ),
            )
        )
    )

    print("subject_schema_valid=true")
    print(
        "recording_context_schema_valid=true"
    )
    print("subject_arrow_rows=3")
    print(
        "recording_context_arrow_rows=6"
    )
    print(
        "subject_key_deterministic=true"
    )
    print(
        "recording_subject_link_valid=true"
    )
    print(
        "nullable_treatment_valid=true"
    )
    print(
        "subject_parquet_round_trip=true"
    )
    print(
        "empty_subject_datasets_blocked=true"
    )
    print(
        "subject_parquet_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
