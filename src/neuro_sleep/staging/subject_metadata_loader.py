from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal
from uuid import UUID

from botocore.client import BaseClient
import pyarrow as pa
import pyarrow.parquet as pq

from neuro_sleep.config import (
    Settings,
    get_settings,
)
from neuro_sleep.db.postgres import (
    get_postgres_connection,
)
from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.silver.parquet_schemas import (
    SCHEMA_VERSION,
)
from neuro_sleep.silver.silver_object_writer import (
    calculate_file_sha256,
)
from neuro_sleep.silver.subject_metadata_pipeline import (
    SILVER_BUCKET,
    TRANSFORM_VERSION,
    build_output_prefix,
    calculate_input_fingerprint,
    data_object_keys,
    load_source_metadata_files,
    read_success_manifest,
    validate_completed_output,
)
from neuro_sleep.silver.subject_parquet import (
    RECORDING_CONTEXTS_SCHEMA,
    SUBJECTS_SCHEMA,
)
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


SubjectMetadataStagingStatus = Literal[
    "written",
    "skipped",
]


@dataclass(frozen=True)
class SubjectMetadataDataObject:
    dataset_name: str
    object_key: str
    row_count: int
    file_size_bytes: int
    checksum_sha256: str


@dataclass(frozen=True)
class SubjectMetadataPublication:
    silver_bucket: str
    output_prefix: str
    input_fingerprint: str
    source_system: str
    dataset_version: str
    schema_version: str
    transform_version: str
    subject_count: int
    recording_context_count: int
    subjects_object: SubjectMetadataDataObject
    recording_contexts_object: (
        SubjectMetadataDataObject
    )


@dataclass(frozen=True)
class SubjectMetadataStagingLoadResult:
    status: SubjectMetadataStagingStatus
    output_prefix: str
    input_fingerprint: str
    subject_count: int
    recording_context_count: int
    rows_written: int
    files_processed: int


def build_subject_metadata_root_prefix(
    dataset_version: str,
) -> str:
    normalized_version = dataset_version.strip()

    if not normalized_version:
        raise ValueError(
            "dataset_version cannot be empty"
        )

    return (
        "physionet/sleep-edfx/"
        f"{normalized_version}/metadata"
    )


def _require_manifest_string(
    manifest: dict[str, object],
    field_name: str,
) -> str:
    value = manifest.get(field_name)

    if not isinstance(value, str):
        raise RuntimeError(
            "Subject metadata success manifest "
            f"field is invalid: {field_name}"
        )

    normalized = value.strip()
    if not normalized:
        raise RuntimeError(
            "Subject metadata success manifest "
            f"field is empty: {field_name}"
        )

    return normalized


def _require_manifest_count(
    manifest: dict[str, object],
    field_name: str,
) -> int:
    value = manifest.get(field_name)

    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise RuntimeError(
            "Subject metadata success manifest "
            f"count is invalid: {field_name}"
        )

    return value


def _validate_sha256(
    value: str,
    field_name: str,
) -> str:
    normalized = value.strip().lower()

    if (
        len(normalized) != 64
        or any(
            character not in "0123456789abcdef"
            for character in normalized
        )
    ):
        raise RuntimeError(
            f"Invalid SHA-256 value for {field_name}"
        )

    return normalized


def _parse_data_objects(
    manifest: dict[str, object],
) -> dict[str, SubjectMetadataDataObject]:
    raw_objects = manifest.get("data_objects")

    if not isinstance(raw_objects, list):
        raise RuntimeError(
            "Subject metadata success manifest "
            "has no data_objects list"
        )

    parsed: dict[
        str,
        SubjectMetadataDataObject,
    ] = {}

    for raw_object in raw_objects:
        if not isinstance(raw_object, dict):
            raise RuntimeError(
                "Invalid subject metadata "
                "data_objects entry"
            )

        dataset_name = _require_manifest_string(
            raw_object,
            "dataset_name",
        )
        object_key = _require_manifest_string(
            raw_object,
            "object_key",
        )
        row_count = _require_manifest_count(
            raw_object,
            "row_count",
        )
        file_size_bytes = _require_manifest_count(
            raw_object,
            "file_size_bytes",
        )
        checksum_sha256 = _validate_sha256(
            _require_manifest_string(
                raw_object,
                "checksum_sha256",
            ),
            (
                "data_objects."
                f"{dataset_name}.checksum_sha256"
            ),
        )

        if dataset_name in parsed:
            raise RuntimeError(
                "Duplicate subject metadata "
                f"dataset in manifest: {dataset_name}"
            )

        parsed[dataset_name] = (
            SubjectMetadataDataObject(
                dataset_name=dataset_name,
                object_key=object_key,
                row_count=row_count,
                file_size_bytes=file_size_bytes,
                checksum_sha256=(
                    checksum_sha256
                ),
            )
        )

    expected_names = {
        "subjects",
        "recording_contexts",
    }
    if set(parsed) != expected_names:
        raise RuntimeError(
            "Unexpected subject metadata "
            "datasets in manifest: "
            f"{sorted(parsed)}"
        )

    return parsed


def resolve_current_subject_metadata_publication(
    *,
    settings: Settings | None = None,
    silver_bucket: str = SILVER_BUCKET,
    client: BaseClient | None = None,
) -> SubjectMetadataPublication:
    if settings is None:
        settings = get_settings()

    source_files = load_source_metadata_files(
        settings.sleep_edf_version
    )
    input_fingerprint = (
        calculate_input_fingerprint(
            dataset_version=(
                settings.sleep_edf_version
            ),
            source_files=source_files,
        )
    )
    root_prefix = (
        build_subject_metadata_root_prefix(
            settings.sleep_edf_version
        )
    )
    output_prefix = build_output_prefix(
        root_prefix=root_prefix,
        input_fingerprint=input_fingerprint,
    )

    owns_client = client is None
    if client is None:
        client = get_object_storage_client(
            settings
        )

    try:
        completed = validate_completed_output(
            bucket=silver_bucket,
            output_prefix=output_prefix,
            input_fingerprint=(
                input_fingerprint
            ),
            client=client,
        )
        if completed is None:
            raise RuntimeError(
                "Current Silver subject metadata "
                "publication is not complete"
            )

        manifest_key = (
            f"{output_prefix}/_SUCCESS.json"
        )
        manifest = read_success_manifest(
            bucket=silver_bucket,
            object_key=manifest_key,
            client=client,
        )
        if manifest is None:
            raise RuntimeError(
                "Current Silver subject metadata "
                "success manifest is missing"
            )

        source_system = _require_manifest_string(
            manifest,
            "source_system",
        )
        dataset_version = _require_manifest_string(
            manifest,
            "dataset_version",
        )
        schema_version = _require_manifest_string(
            manifest,
            "schema_version",
        )
        transform_version = (
            _require_manifest_string(
                manifest,
                "transform_version",
            )
        )
        manifest_fingerprint = _validate_sha256(
            _require_manifest_string(
                manifest,
                "input_fingerprint",
            ),
            "input_fingerprint",
        )
        manifest_output_prefix = (
            _require_manifest_string(
                manifest,
                "output_prefix",
            )
        )
        subject_count = _require_manifest_count(
            manifest,
            "subject_count",
        )
        recording_context_count = (
            _require_manifest_count(
                manifest,
                "recording_context_count",
            )
        )

        if source_system != SOURCE_SYSTEM:
            raise RuntimeError(
                "Unexpected subject metadata "
                f"source system: {source_system}"
            )
        if (
            dataset_version
            != settings.sleep_edf_version
        ):
            raise RuntimeError(
                "Subject metadata dataset "
                "version mismatch"
            )
        if schema_version != SCHEMA_VERSION:
            raise RuntimeError(
                "Subject metadata schema "
                "version mismatch"
            )
        if (
            transform_version
            != TRANSFORM_VERSION
        ):
            raise RuntimeError(
                "Subject metadata transform "
                "version mismatch"
            )
        if (
            manifest_fingerprint
            != input_fingerprint
        ):
            raise RuntimeError(
                "Subject metadata input "
                "fingerprint mismatch"
            )
        if (
            manifest_output_prefix
            != output_prefix
        ):
            raise RuntimeError(
                "Subject metadata output "
                "prefix mismatch"
            )

        data_objects = _parse_data_objects(
            manifest
        )
        expected_subjects_key, (
            expected_contexts_key
        ) = data_object_keys(output_prefix)

        subjects_object = data_objects[
            "subjects"
        ]
        contexts_object = data_objects[
            "recording_contexts"
        ]

        if (
            subjects_object.object_key
            != expected_subjects_key
        ):
            raise RuntimeError(
                "Unexpected subjects Parquet "
                "object key"
            )
        if (
            contexts_object.object_key
            != expected_contexts_key
        ):
            raise RuntimeError(
                "Unexpected recording contexts "
                "Parquet object key"
            )
        if (
            subjects_object.row_count
            != subject_count
        ):
            raise RuntimeError(
                "Subjects manifest row count "
                "does not match subject_count"
            )
        if (
            contexts_object.row_count
            != recording_context_count
        ):
            raise RuntimeError(
                "Recording contexts manifest "
                "row count does not match "
                "recording_context_count"
            )

        return SubjectMetadataPublication(
            silver_bucket=silver_bucket,
            output_prefix=output_prefix,
            input_fingerprint=(
                input_fingerprint
            ),
            source_system=source_system,
            dataset_version=dataset_version,
            schema_version=schema_version,
            transform_version=(
                transform_version
            ),
            subject_count=subject_count,
            recording_context_count=(
                recording_context_count
            ),
            subjects_object=subjects_object,
            recording_contexts_object=(
                contexts_object
            ),
        )
    finally:
        if owns_client:
            client.close()


def _download_and_read_parquet(
    *,
    publication: SubjectMetadataPublication,
    data_object: SubjectMetadataDataObject,
    expected_schema: pa.Schema,
    destination: Path,
    client: BaseClient,
) -> pa.Table:
    run_object_storage_operation(
        operation=lambda: client.download_file(
            Bucket=publication.silver_bucket,
            Key=data_object.object_key,
            Filename=str(destination),
        ),
        operation_name=(
            "download_file:"
            f"{publication.silver_bucket}/"
            f"{data_object.object_key}"
        ),
    )

    actual_size = destination.stat().st_size
    if (
        actual_size
        != data_object.file_size_bytes
    ):
        raise RuntimeError(
            "Downloaded Silver metadata file "
            "size mismatch: "
            f"{data_object.object_key}; "
            f"expected="
            f"{data_object.file_size_bytes}, "
            f"actual={actual_size}"
        )

    actual_checksum = calculate_file_sha256(
        destination
    )
    if (
        actual_checksum
        != data_object.checksum_sha256
    ):
        raise RuntimeError(
            "Downloaded Silver metadata file "
            "checksum mismatch: "
            f"{data_object.object_key}"
        )

    table = pq.read_table(destination)

    if not table.schema.equals(
        expected_schema,
        check_metadata=True,
    ):
        raise RuntimeError(
            "Silver metadata Parquet schema "
            "does not match the expected "
            f"{data_object.dataset_name} schema"
        )
    if table.num_rows != data_object.row_count:
        raise RuntimeError(
            "Silver metadata Parquet row count "
            "does not match the success manifest: "
            f"{data_object.dataset_name}"
        )

    return table


def _validate_table_identity(
    *,
    publication: SubjectMetadataPublication,
    subjects_table: pa.Table,
    contexts_table: pa.Table,
) -> None:
    subject_rows = subjects_table.to_pylist()
    context_rows = contexts_table.to_pylist()

    subject_keys = {
        row["subject_key"]
        for row in subject_rows
    }
    if len(subject_keys) != len(subject_rows):
        raise RuntimeError(
            "Duplicate subject_key values in "
            "Silver subjects Parquet"
        )

    recording_identities = {
        (
            row["source_system"],
            row["dataset_version"],
            row["collection"],
            row["recording_key"],
        )
        for row in context_rows
    }
    if (
        len(recording_identities)
        != len(context_rows)
    ):
        raise RuntimeError(
            "Duplicate logical recording "
            "identities in Silver recording "
            "contexts Parquet"
        )

    for dataset_name, rows in (
        ("subjects", subject_rows),
        ("recording_contexts", context_rows),
    ):
        for row in rows:
            if (
                row["source_system"]
                != publication.source_system
            ):
                raise RuntimeError(
                    f"{dataset_name} source_system "
                    "does not match the manifest"
                )
            if (
                row["dataset_version"]
                != publication.dataset_version
            ):
                raise RuntimeError(
                    f"{dataset_name} dataset_version "
                    "does not match the manifest"
                )

    context_subject_keys = {
        row["subject_key"]
        for row in context_rows
    }
    missing_subject_keys = (
        context_subject_keys - subject_keys
    )
    if missing_subject_keys:
        raise RuntimeError(
            "Recording contexts reference "
            "subjects absent from subjects "
            "Parquet: "
            f"{sorted(missing_subject_keys)}"
        )


def _read_publication_rows(
    *,
    cursor,
    table_name: str,
    publication: SubjectMetadataPublication,
) -> list[tuple[object, ...]]:
    cursor.execute(
        f"""
        select
            source_system,
            dataset_version,
            schema_version,
            transform_version,
            silver_bucket,
            silver_output_prefix,
            count(*)
        from staging.{table_name}
        where metadata_input_fingerprint = %s
        group by
            source_system,
            dataset_version,
            schema_version,
            transform_version,
            silver_bucket,
            silver_output_prefix;
        """,
        (
            publication.input_fingerprint,
        ),
    )

    return cursor.fetchall()


def _publication_is_complete(
    *,
    cursor,
    publication: SubjectMetadataPublication,
) -> bool:
    expected = {
        "silver_subjects": (
            publication.subject_count
        ),
        "silver_recording_contexts": (
            publication.recording_context_count
        ),
    }

    complete_tables = 0

    for table_name, expected_count in (
        expected.items()
    ):
        rows = _read_publication_rows(
            cursor=cursor,
            table_name=table_name,
            publication=publication,
        )

        if not rows:
            continue

        if len(rows) != 1:
            raise RuntimeError(
                "Staging contains conflicting "
                "lineage for subject metadata "
                f"publication in {table_name}"
            )

        (
            source_system,
            dataset_version,
            schema_version,
            transform_version,
            silver_bucket,
            silver_output_prefix,
            row_count,
        ) = rows[0]

        expected_lineage = (
            publication.source_system,
            publication.dataset_version,
            publication.schema_version,
            publication.transform_version,
            publication.silver_bucket,
            publication.output_prefix,
        )
        actual_lineage = (
            source_system,
            dataset_version,
            schema_version,
            transform_version,
            silver_bucket,
            silver_output_prefix,
        )

        if actual_lineage != expected_lineage:
            raise RuntimeError(
                "Staging subject metadata "
                "publication lineage mismatch "
                f"in {table_name}"
            )
        if row_count != expected_count:
            raise RuntimeError(
                "Incomplete staging subject "
                "metadata publication in "
                f"{table_name}: expected="
                f"{expected_count}, actual="
                f"{row_count}"
            )

        complete_tables += 1

    if complete_tables == 0:
        return False
    if complete_tables != len(expected):
        raise RuntimeError(
            "Staging subject metadata "
            "publication is partially loaded"
        )

    return True


def _insert_subjects(
    *,
    cursor,
    rows: list[dict[str, object]],
    publication: SubjectMetadataPublication,
    run_id: UUID,
) -> None:
    parameters = [
        (
            row["subject_key"],
            row["source_system"],
            row["dataset_version"],
            row["collection"],
            row["source_subject_id"],
            row["source_subject_number"],
            row["age_years"],
            row["sex"],
            row["source_bucket"],
            row["source_object_key"],
            publication.input_fingerprint,
            publication.schema_version,
            publication.transform_version,
            publication.silver_bucket,
            publication.output_prefix,
            run_id,
        )
        for row in rows
    ]

    cursor.executemany(
        """
        insert into staging.silver_subjects (
            subject_key,
            source_system,
            dataset_version,
            collection,
            source_subject_id,
            source_subject_number,
            age_years,
            sex,
            source_bucket,
            source_object_key,
            metadata_input_fingerprint,
            schema_version,
            transform_version,
            silver_bucket,
            silver_output_prefix,
            staging_load_run_id
        )
        values (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        );
        """,
        parameters,
    )


def _insert_recording_contexts(
    *,
    cursor,
    rows: list[dict[str, object]],
    publication: SubjectMetadataPublication,
    run_id: UUID,
) -> None:
    parameters = [
        (
            row["recording_key"],
            row["subject_key"],
            row["source_system"],
            row["dataset_version"],
            row["collection"],
            row["night_number"],
            row["lights_off_seconds"],
            row["treatment"],
            row["source_bucket"],
            row["source_object_key"],
            publication.input_fingerprint,
            publication.schema_version,
            publication.transform_version,
            publication.silver_bucket,
            publication.output_prefix,
            run_id,
        )
        for row in rows
    ]

    cursor.executemany(
        """
        insert into
            staging.silver_recording_contexts (
                recording_key,
                subject_key,
                source_system,
                dataset_version,
                collection,
                night_number,
                lights_off_seconds,
                treatment,
                source_bucket,
                source_object_key,
                metadata_input_fingerprint,
                schema_version,
                transform_version,
                silver_bucket,
                silver_output_prefix,
                staging_load_run_id
            )
        values (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        );
        """,
        parameters,
    )


def load_subject_metadata_to_staging(
    *,
    run_id: UUID,
    settings: Settings | None = None,
    silver_bucket: str = SILVER_BUCKET,
    client: BaseClient | None = None,
) -> SubjectMetadataStagingLoadResult:
    if settings is None:
        settings = get_settings()

    owns_client = client is None
    if client is None:
        client = get_object_storage_client(
            settings
        )

    try:
        publication = (
            resolve_current_subject_metadata_publication(
                settings=settings,
                silver_bucket=silver_bucket,
                client=client,
            )
        )

        with get_postgres_connection(
            settings=settings
        ) as connection:
            with connection.cursor() as cursor:
                if _publication_is_complete(
                    cursor=cursor,
                    publication=publication,
                ):
                    return (
                        SubjectMetadataStagingLoadResult(
                            status="skipped",
                            output_prefix=(
                                publication.output_prefix
                            ),
                            input_fingerprint=(
                                publication
                                .input_fingerprint
                            ),
                            subject_count=(
                                publication.subject_count
                            ),
                            recording_context_count=(
                                publication
                                .recording_context_count
                            ),
                            rows_written=0,
                            files_processed=0,
                        )
                    )

        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_subject_"
                "metadata_staging_"
            )
        ) as temporary_directory:
            temporary_root = Path(
                temporary_directory
            )
            subjects_table = (
                _download_and_read_parquet(
                    publication=publication,
                    data_object=(
                        publication.subjects_object
                    ),
                    expected_schema=SUBJECTS_SCHEMA,
                    destination=(
                        temporary_root
                        / "subjects.parquet"
                    ),
                    client=client,
                )
            )
            contexts_table = (
                _download_and_read_parquet(
                    publication=publication,
                    data_object=(
                        publication
                        .recording_contexts_object
                    ),
                    expected_schema=(
                        RECORDING_CONTEXTS_SCHEMA
                    ),
                    destination=(
                        temporary_root
                        / "recording_contexts.parquet"
                    ),
                    client=client,
                )
            )

            _validate_table_identity(
                publication=publication,
                subjects_table=subjects_table,
                contexts_table=contexts_table,
            )

            subject_rows = (
                subjects_table.to_pylist()
            )
            context_rows = (
                contexts_table.to_pylist()
            )

            with get_postgres_connection(
                settings=settings
            ) as connection:
                with connection.cursor() as cursor:
                    if _publication_is_complete(
                        cursor=cursor,
                        publication=publication,
                    ):
                        return (
                            SubjectMetadataStagingLoadResult(
                                status="skipped",
                                output_prefix=(
                                    publication
                                    .output_prefix
                                ),
                                input_fingerprint=(
                                    publication
                                    .input_fingerprint
                                ),
                                subject_count=(
                                    publication
                                    .subject_count
                                ),
                                recording_context_count=(
                                    publication
                                    .recording_context_count
                                ),
                                rows_written=0,
                                files_processed=0,
                            )
                        )

                    _insert_subjects(
                        cursor=cursor,
                        rows=subject_rows,
                        publication=publication,
                        run_id=run_id,
                    )
                    _insert_recording_contexts(
                        cursor=cursor,
                        rows=context_rows,
                        publication=publication,
                        run_id=run_id,
                    )

                    if not _publication_is_complete(
                        cursor=cursor,
                        publication=publication,
                    ):
                        raise RuntimeError(
                            "Subject metadata staging "
                            "load did not produce a "
                            "complete publication"
                        )

        rows_written = (
            publication.subject_count
            + publication.recording_context_count
        )

        return SubjectMetadataStagingLoadResult(
            status="written",
            output_prefix=(
                publication.output_prefix
            ),
            input_fingerprint=(
                publication.input_fingerprint
            ),
            subject_count=(
                publication.subject_count
            ),
            recording_context_count=(
                publication.recording_context_count
            ),
            rows_written=rows_written,
            files_processed=2,
        )
    finally:
        if owns_client:
            client.close()
