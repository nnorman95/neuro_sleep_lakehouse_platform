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
from neuro_sleep.silver.batch_discovery import (
    build_dataset_prefix,
    build_silver_root_prefix,
)
from neuro_sleep.silver.idempotency import (
    SILVER_TRANSFORM_VERSION,
    SUCCESS_OBJECT_NAME,
    build_idempotent_output_prefix,
    read_success_manifest,
)
from neuro_sleep.silver.parquet_schemas import (
    SCHEMA_VERSION,
    get_silver_schema,
)
from neuro_sleep.silver.silver_object_writer import (
    calculate_file_sha256,
)
from neuro_sleep.silver.silver_recording_writer import (
    build_metadata_object_keys,
)
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
)
from neuro_sleep.sources.sleep_edf_manifest import (
    classify_sleep_edf_source_file,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
)


SILVER_BUCKET = "silver"

METADATA_DATASETS = (
    "recordings",
    "channels",
    "sleep_stage_intervals",
    "sleep_stage_epochs",
)

RecordingStagingStatus = Literal[
    "written",
    "skipped",
]


@dataclass(frozen=True)
class RecordingDataObject:
    dataset_name: str
    bucket: str
    object_key: str
    row_count: int
    file_size_bytes: int
    checksum_sha256: str


@dataclass(frozen=True)
class RecordingPublication:
    silver_bucket: str
    output_prefix: str

    source_system: str
    dataset_version: str
    collection: str
    recording_key: str

    recording_id: UUID
    source_pair_id: str
    input_fingerprint: str
    config_id: str
    schema_version: str
    transform_version: str

    psg_file_id: UUID
    hypnogram_file_id: UUID
    psg_bucket: str
    psg_object_key: str
    hypnogram_bucket: str
    hypnogram_object_key: str
    psg_checksum_sha256: str
    hypnogram_checksum_sha256: str

    data_objects: tuple[
        RecordingDataObject,
        ...,
    ]

    @property
    def expected_rows(self) -> int:
        return sum(
            item.row_count
            for item in self.data_objects
        )

    @property
    def expected_files(self) -> int:
        return len(self.data_objects)

    def object_for(
        self,
        dataset_name: str,
    ) -> RecordingDataObject:
        for item in self.data_objects:
            if item.dataset_name == dataset_name:
                return item

        raise KeyError(
            f"Missing publication dataset: "
            f"{dataset_name}"
        )


@dataclass(frozen=True)
class LoadedRecordingPublication:
    publication: RecordingPublication
    recordings_table: pa.Table
    channels_table: pa.Table
    intervals_table: pa.Table
    epochs_table: pa.Table


@dataclass(frozen=True)
class RecordingStagingLoadResult:
    status: RecordingStagingStatus
    publication_count: int
    publications_written: int
    publications_skipped: int
    recordings_count: int
    channels_count: int
    interval_count: int
    epoch_count: int
    rows_written: int
    files_processed: int


def _require_dict(
    value: object,
    field_name: str,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RuntimeError(
            f"Expected object for {field_name}"
        )

    return value


def _require_list(
    value: object,
    field_name: str,
) -> list[object]:
    if not isinstance(value, list):
        raise RuntimeError(
            f"Expected list for {field_name}"
        )

    return value


def _require_string(
    value: object,
    field_name: str,
) -> str:
    if not isinstance(value, str):
        raise RuntimeError(
            f"Expected string for {field_name}"
        )

    normalized = value.strip()
    if not normalized:
        raise RuntimeError(
            f"Expected non-empty string for "
            f"{field_name}"
        )

    return normalized


def _require_nonnegative_int(
    value: object,
    field_name: str,
) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise RuntimeError(
            "Expected non-negative integer for "
            f"{field_name}"
        )

    return value


def _require_uuid(
    value: object,
    field_name: str,
) -> UUID:
    raw_value = _require_string(
        value,
        field_name,
    )

    try:
        return UUID(raw_value)
    except ValueError as error:
        raise RuntimeError(
            f"Invalid UUID for {field_name}: "
            f"{raw_value}"
        ) from error


def _require_sha256(
    value: object,
    field_name: str,
) -> str:
    normalized = _require_string(
        value,
        field_name,
    ).lower()

    if (
        len(normalized) != 64
        or any(
            character not in "0123456789abcdef"
            for character in normalized
        )
    ):
        raise RuntimeError(
            f"Invalid SHA-256 for {field_name}"
        )

    return normalized


def _parse_current_publication(
    *,
    manifest: dict[str, object],
    output_prefix: str,
    dataset_version: str,
) -> RecordingPublication | None:
    if manifest.get("status") != "complete":
        return None

    if (
        manifest.get("schema_version")
        != SCHEMA_VERSION
        or manifest.get("transform_version")
        != SILVER_TRANSFORM_VERSION
    ):
        return None

    source_pair_id = _require_sha256(
        manifest.get("source_pair_id"),
        "source_pair_id",
    )
    input_fingerprint = _require_sha256(
        manifest.get("input_fingerprint"),
        "input_fingerprint",
    )
    config_id = _require_sha256(
        manifest.get("config_id"),
        "config_id",
    )
    recording_id = _require_uuid(
        manifest.get("recording_id"),
        "recording_id",
    )

    source = _require_dict(
        manifest.get("source"),
        "source",
    )

    source_system = _require_string(
        source.get("source_system"),
        "source.source_system",
    )
    if source_system != SOURCE_SYSTEM:
        raise RuntimeError(
            "Unexpected Silver recording "
            f"source_system: {source_system}"
        )

    psg_file_id = _require_uuid(
        source.get("psg_file_id"),
        "source.psg_file_id",
    )
    hypnogram_file_id = _require_uuid(
        source.get("hypnogram_file_id"),
        "source.hypnogram_file_id",
    )
    psg_bucket = _require_string(
        source.get("psg_bucket"),
        "source.psg_bucket",
    )
    psg_object_key = _require_string(
        source.get("psg_object_key"),
        "source.psg_object_key",
    )
    hypnogram_bucket = _require_string(
        source.get("hypnogram_bucket"),
        "source.hypnogram_bucket",
    )
    hypnogram_object_key = _require_string(
        source.get("hypnogram_object_key"),
        "source.hypnogram_object_key",
    )
    psg_checksum_sha256 = _require_sha256(
        source.get("psg_checksum_sha256"),
        "source.psg_checksum_sha256",
    )
    hypnogram_checksum_sha256 = _require_sha256(
        source.get(
            "hypnogram_checksum_sha256"
        ),
        "source.hypnogram_checksum_sha256",
    )

    dataset_prefix = build_dataset_prefix(
        dataset_version
    )

    if not psg_object_key.startswith(
        dataset_prefix
    ):
        raise RuntimeError(
            "PSG object is outside the "
            "configured Sleep-EDF dataset prefix"
        )
    if not hypnogram_object_key.startswith(
        dataset_prefix
    ):
        raise RuntimeError(
            "Hypnogram object is outside the "
            "configured Sleep-EDF dataset prefix"
        )

    psg_source = classify_sleep_edf_source_file(
        relative_path=psg_object_key[
            len(dataset_prefix):
        ],
        checksum_sha256=psg_checksum_sha256,
        dataset_version=dataset_version,
    )
    hypnogram_source = (
        classify_sleep_edf_source_file(
            relative_path=hypnogram_object_key[
                len(dataset_prefix):
            ],
            checksum_sha256=(
                hypnogram_checksum_sha256
            ),
            dataset_version=dataset_version,
        )
    )

    if psg_source.file_role != "psg":
        raise RuntimeError(
            "Manifest PSG object does not "
            "classify as PSG"
        )
    if (
        hypnogram_source.file_role
        != "hypnogram"
    ):
        raise RuntimeError(
            "Manifest Hypnogram object does not "
            "classify as Hypnogram"
        )

    if (
        psg_source.recording_key is None
        or psg_source.study_folder is None
    ):
        raise RuntimeError(
            "Could not derive logical recording "
            "identity from PSG object"
        )
    if (
        hypnogram_source.recording_key
        != psg_source.recording_key
        or hypnogram_source.study_folder
        != psg_source.study_folder
    ):
        raise RuntimeError(
            "PSG and Hypnogram logical recording "
            "identities do not match"
        )

    collection = psg_source.study_folder
    recording_key = psg_source.recording_key

    expected_output_prefix = (
        build_idempotent_output_prefix(
            root_prefix=(
                build_silver_root_prefix(
                    psg_object_key
                )
            ),
            source_pair_id=source_pair_id,
            input_fingerprint=(
                input_fingerprint
            ),
            config_id=config_id,
        )
    )
    if output_prefix != expected_output_prefix:
        raise RuntimeError(
            "Silver recording output prefix "
            "does not match manifest identity"
        )

    expected_object_keys = (
        build_metadata_object_keys(
            output_prefix
        )
    )
    raw_objects = _require_list(
        manifest.get("objects"),
        "objects",
    )

    parsed_by_dataset: dict[
        str,
        RecordingDataObject,
    ] = {}

    for raw_item in raw_objects:
        item = _require_dict(
            raw_item,
            "objects[]",
        )
        dataset_name = _require_string(
            item.get("dataset_name"),
            "objects[].dataset_name",
        )

        if dataset_name not in (
            METADATA_DATASETS
        ):
            continue

        if dataset_name in parsed_by_dataset:
            raise RuntimeError(
                "Duplicate metadata dataset in "
                "Silver success manifest: "
                f"{dataset_name}"
            )

        bucket = _require_string(
            item.get("bucket"),
            (
                "objects[]."
                f"{dataset_name}.bucket"
            ),
        )
        if bucket != SILVER_BUCKET:
            raise RuntimeError(
                "Unexpected Silver metadata "
                f"bucket: {bucket}"
            )

        object_key = _require_string(
            item.get("object_key"),
            (
                "objects[]."
                f"{dataset_name}.object_key"
            ),
        )
        if (
            object_key
            != expected_object_keys[
                dataset_name
            ]
        ):
            raise RuntimeError(
                "Unexpected Silver metadata "
                f"object key for {dataset_name}"
            )

        parsed_by_dataset[
            dataset_name
        ] = RecordingDataObject(
            dataset_name=dataset_name,
            bucket=bucket,
            object_key=object_key,
            row_count=(
                _require_nonnegative_int(
                    item.get("row_count"),
                    (
                        "objects[]."
                        f"{dataset_name}.row_count"
                    ),
                )
            ),
            file_size_bytes=(
                _require_nonnegative_int(
                    item.get("file_size_bytes"),
                    (
                        "objects[]."
                        f"{dataset_name}."
                        "file_size_bytes"
                    ),
                )
            ),
            checksum_sha256=(
                _require_sha256(
                    item.get(
                        "checksum_sha256"
                    ),
                    (
                        "objects[]."
                        f"{dataset_name}."
                        "checksum_sha256"
                    ),
                )
            ),
        )

    if set(parsed_by_dataset) != set(
        METADATA_DATASETS
    ):
        missing = (
            set(METADATA_DATASETS)
            - set(parsed_by_dataset)
        )
        raise RuntimeError(
            "Silver recording manifest is "
            "missing metadata datasets: "
            f"{sorted(missing)}"
        )

    data_objects = tuple(
        parsed_by_dataset[name]
        for name in METADATA_DATASETS
    )

    if (
        parsed_by_dataset[
            "recordings"
        ].row_count
        != 1
    ):
        raise RuntimeError(
            "Each Silver recording publication "
            "must contain exactly one "
            "recordings row"
        )

    return RecordingPublication(
        silver_bucket=SILVER_BUCKET,
        output_prefix=output_prefix,
        source_system=source_system,
        dataset_version=dataset_version,
        collection=collection,
        recording_key=recording_key,
        recording_id=recording_id,
        source_pair_id=source_pair_id,
        input_fingerprint=input_fingerprint,
        config_id=config_id,
        schema_version=SCHEMA_VERSION,
        transform_version=(
            SILVER_TRANSFORM_VERSION
        ),
        psg_file_id=psg_file_id,
        hypnogram_file_id=(
            hypnogram_file_id
        ),
        psg_bucket=psg_bucket,
        psg_object_key=psg_object_key,
        hypnogram_bucket=hypnogram_bucket,
        hypnogram_object_key=(
            hypnogram_object_key
        ),
        psg_checksum_sha256=(
            psg_checksum_sha256
        ),
        hypnogram_checksum_sha256=(
            hypnogram_checksum_sha256
        ),
        data_objects=data_objects,
    )


def discover_current_recording_publications(
    *,
    settings: Settings | None = None,
    client: BaseClient | None = None,
) -> tuple[RecordingPublication, ...]:
    if settings is None:
        settings = get_settings()

    owns_client = client is None
    if client is None:
        client = get_object_storage_client(
            settings
        )

    dataset_prefix = build_dataset_prefix(
        settings.sleep_edf_version
    )

    try:
        summaries = list_object_summaries(
            bucket=SILVER_BUCKET,
            prefix=dataset_prefix,
            client=client,
        )
        success_keys = sorted(
            item.object_key
            for item in summaries
            if item.object_key.endswith(
                f"/{SUCCESS_OBJECT_NAME}"
            )
            and "/metadata/" not in item.object_key
            and "/smoke-tests/" not in item.object_key
        )

        publications: list[
            RecordingPublication
        ] = []

        for success_key in success_keys:
            output_prefix = success_key[
                : -len(
                    f"/{SUCCESS_OBJECT_NAME}"
                )
            ]
            manifest = read_success_manifest(
                bucket=SILVER_BUCKET,
                output_prefix=output_prefix,
                client=client,
            )

            publication = (
                _parse_current_publication(
                    manifest=manifest,
                    output_prefix=output_prefix,
                    dataset_version=(
                        settings.sleep_edf_version
                    ),
                )
            )
            if publication is not None:
                publications.append(
                    publication
                )

        if not publications:
            raise RuntimeError(
                "No current compatible Silver "
                "recording publications were found"
            )

        by_logical_identity: dict[
            tuple[str, str, str, str],
            RecordingPublication,
        ] = {}

        for publication in publications:
            logical_identity = (
                publication.source_system,
                publication.dataset_version,
                publication.collection,
                publication.recording_key,
            )
            previous = by_logical_identity.get(
                logical_identity
            )
            if previous is not None:
                raise RuntimeError(
                    "More than one current "
                    "compatible Silver publication "
                    "exists for logical recording "
                    f"{logical_identity}: "
                    f"{previous.output_prefix}, "
                    f"{publication.output_prefix}"
                )

            by_logical_identity[
                logical_identity
            ] = publication

        return tuple(
            sorted(
                publications,
                key=lambda item: (
                    item.collection,
                    item.recording_key,
                    item.output_prefix,
                ),
            )
        )
    finally:
        if owns_client:
            client.close()


def _download_and_read_metadata_object(
    *,
    data_object: RecordingDataObject,
    destination: Path,
    client: BaseClient,
) -> pa.Table:
    run_object_storage_operation(
        operation=lambda: client.download_file(
            Bucket=data_object.bucket,
            Key=data_object.object_key,
            Filename=str(destination),
        ),
        operation_name=(
            "download_file:"
            f"{data_object.bucket}/"
            f"{data_object.object_key}"
        ),
    )

    actual_size = destination.stat().st_size
    if actual_size != (
        data_object.file_size_bytes
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
    expected_schema = get_silver_schema(
        data_object.dataset_name
    )

    if not table.schema.equals(
        expected_schema,
        check_metadata=True,
    ):
        raise RuntimeError(
            "Silver metadata Parquet schema "
            "does not match expected schema: "
            f"{data_object.dataset_name}"
        )

    if table.num_rows != data_object.row_count:
        raise RuntimeError(
            "Silver metadata Parquet row count "
            "does not match manifest: "
            f"{data_object.dataset_name}"
        )

    return table


def _validate_loaded_publication(
    *,
    publication: RecordingPublication,
    recordings_table: pa.Table,
    channels_table: pa.Table,
    intervals_table: pa.Table,
    epochs_table: pa.Table,
) -> None:
    recording_rows = (
        recordings_table.to_pylist()
    )
    channel_rows = channels_table.to_pylist()
    interval_rows = (
        intervals_table.to_pylist()
    )
    epoch_rows = epochs_table.to_pylist()

    if len(recording_rows) != 1:
        raise RuntimeError(
            "Silver recordings Parquet must "
            "contain exactly one row"
        )

    recording = recording_rows[0]

    if (
        UUID(recording["recording_id"])
        != publication.recording_id
    ):
        raise RuntimeError(
            "recording_id does not match "
            "success manifest"
        )

    expected_recording_lineage = {
        "source_system": (
            publication.source_system
        ),
        "psg_bucket": publication.psg_bucket,
        "psg_object_key": (
            publication.psg_object_key
        ),
        "hypnogram_bucket": (
            publication.hypnogram_bucket
        ),
        "hypnogram_object_key": (
            publication.hypnogram_object_key
        ),
    }

    for (
        field_name,
        expected_value,
    ) in expected_recording_lineage.items():
        if recording[field_name] != (
            expected_value
        ):
            raise RuntimeError(
                "Recording Parquet lineage "
                f"mismatch for {field_name}"
            )

    recording_id_text = str(
        publication.recording_id
    )

    for dataset_name, rows in (
        ("channels", channel_rows),
        ("sleep_stage_intervals", interval_rows),
        ("sleep_stage_epochs", epoch_rows),
    ):
        mismatched = [
            row["recording_id"]
            for row in rows
            if row["recording_id"]
            != recording_id_text
        ]
        if mismatched:
            raise RuntimeError(
                f"{dataset_name} contains rows "
                "for a different recording_id"
            )

    channel_ids = {
        row["channel_id"]
        for row in channel_rows
    }
    if len(channel_ids) != len(channel_rows):
        raise RuntimeError(
            "Duplicate channel_id values in "
            "Silver channels Parquet"
        )

    interval_ids = {
        row["interval_id"]
        for row in interval_rows
    }
    if len(interval_ids) != len(
        interval_rows
    ):
        raise RuntimeError(
            "Duplicate interval_id values in "
            "Silver intervals Parquet"
        )

    epoch_ids = {
        row["epoch_id"]
        for row in epoch_rows
    }
    if len(epoch_ids) != len(epoch_rows):
        raise RuntimeError(
            "Duplicate epoch_id values in "
            "Silver epochs Parquet"
        )

    orphan_interval_ids = {
        row["source_interval_id"]
        for row in epoch_rows
        if row["source_interval_id"]
        not in interval_ids
    }
    if orphan_interval_ids:
        raise RuntimeError(
            "Silver epochs reference intervals "
            "absent from the same publication"
        )

    if len(channel_rows) != (
        recording["channel_count"]
    ):
        raise RuntimeError(
            "Channel row count does not match "
            "recording.channel_count"
        )

    if len(interval_rows) != (
        recording["annotation_count"]
    ):
        raise RuntimeError(
            "Interval row count does not match "
            "recording.annotation_count"
        )

    if len(epoch_rows) != (
        recording["in_range_epoch_count"]
    ):
        raise RuntimeError(
            "Epoch row count does not match "
            "recording.in_range_epoch_count"
        )


def _load_publication_files(
    *,
    publication: RecordingPublication,
    temporary_root: Path,
    client: BaseClient,
) -> LoadedRecordingPublication:
    publication_root = (
        temporary_root
        / publication.recording_key
        / publication.input_fingerprint
    )
    publication_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    tables: dict[str, pa.Table] = {}

    for data_object in (
        publication.data_objects
    ):
        destination = (
            publication_root
            / f"{data_object.dataset_name}.parquet"
        )
        tables[
            data_object.dataset_name
        ] = (
            _download_and_read_metadata_object(
                data_object=data_object,
                destination=destination,
                client=client,
            )
        )

    loaded = LoadedRecordingPublication(
        publication=publication,
        recordings_table=tables[
            "recordings"
        ],
        channels_table=tables["channels"],
        intervals_table=tables[
            "sleep_stage_intervals"
        ],
        epochs_table=tables[
            "sleep_stage_epochs"
        ],
    )

    _validate_loaded_publication(
        publication=publication,
        recordings_table=(
            loaded.recordings_table
        ),
        channels_table=(
            loaded.channels_table
        ),
        intervals_table=(
            loaded.intervals_table
        ),
        epochs_table=loaded.epochs_table,
    )

    return loaded


def _read_staged_publication_counts(
    *,
    cursor,
    publication: RecordingPublication,
) -> tuple[int, int, int, int]:
    cursor.execute(
        """
        select count(*)
        from staging.silver_recordings
        where recording_id = %s
          and source_system = %s
          and dataset_version = %s
          and collection = %s
          and recording_key = %s
          and source_pair_id = %s
          and input_fingerprint = %s
          and config_id = %s
          and schema_version = %s
          and transform_version = %s
          and silver_bucket = %s
          and silver_output_prefix = %s;
        """,
        (
            publication.recording_id,
            publication.source_system,
            publication.dataset_version,
            publication.collection,
            publication.recording_key,
            publication.source_pair_id,
            publication.input_fingerprint,
            publication.config_id,
            publication.schema_version,
            publication.transform_version,
            publication.silver_bucket,
            publication.output_prefix,
        ),
    )
    recording_count = cursor.fetchone()[0]

    cursor.execute(
        """
        select count(*)
        from staging.silver_channels
        where recording_id = %s;
        """,
        (publication.recording_id,),
    )
    channel_count = cursor.fetchone()[0]

    cursor.execute(
        """
        select count(*)
        from staging.silver_sleep_stage_intervals
        where recording_id = %s;
        """,
        (publication.recording_id,),
    )
    interval_count = cursor.fetchone()[0]

    cursor.execute(
        """
        select count(*)
        from staging.silver_sleep_stage_epochs
        where recording_id = %s;
        """,
        (publication.recording_id,),
    )
    epoch_count = cursor.fetchone()[0]

    return (
        recording_count,
        channel_count,
        interval_count,
        epoch_count,
    )


def _publication_is_complete(
    *,
    cursor,
    publication: RecordingPublication,
) -> bool:
    counts = _read_staged_publication_counts(
        cursor=cursor,
        publication=publication,
    )

    expected_counts = (
        publication.object_for(
            "recordings"
        ).row_count,
        publication.object_for(
            "channels"
        ).row_count,
        publication.object_for(
            "sleep_stage_intervals"
        ).row_count,
        publication.object_for(
            "sleep_stage_epochs"
        ).row_count,
    )

    if counts == (0, 0, 0, 0):
        cursor.execute(
            """
            select
                recording_id,
                silver_output_prefix
            from staging.silver_recordings
            where recording_id = %s
               or (
                    silver_bucket = %s
                    and silver_output_prefix = %s
               );
            """,
            (
                publication.recording_id,
                publication.silver_bucket,
                publication.output_prefix,
            ),
        )
        conflicts = cursor.fetchall()
        if conflicts:
            raise RuntimeError(
                "Conflicting staged recording "
                "identity exists for publication "
                f"{publication.output_prefix}"
            )

        return False

    if counts != expected_counts:
        raise RuntimeError(
            "Staging contains a partial or "
            "conflicting recording publication: "
            f"{publication.recording_key}; "
            f"expected={expected_counts}, "
            f"actual={counts}"
        )

    return True


def _insert_recording(
    *,
    cursor,
    loaded: LoadedRecordingPublication,
    run_id: UUID,
) -> None:
    publication = loaded.publication
    row = loaded.recordings_table.to_pylist()[
        0
    ]

    cursor.execute(
        """
        insert into staging.silver_recordings (
            recording_id,
            source_system,
            dataset_version,
            collection,
            recording_key,
            psg_bucket,
            psg_object_key,
            hypnogram_bucket,
            hypnogram_object_key,
            recording_start,
            duration_seconds,
            channel_count,
            annotation_count,
            in_range_epoch_count,
            out_of_range_epoch_count,
            trailing_overhang_seconds,
            psg_file_id,
            hypnogram_file_id,
            source_pair_id,
            input_fingerprint,
            config_id,
            schema_version,
            transform_version,
            psg_checksum_sha256,
            hypnogram_checksum_sha256,
            silver_bucket,
            silver_output_prefix,
            staging_load_run_id
        )
        values (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        );
        """,
        (
            publication.recording_id,
            publication.source_system,
            publication.dataset_version,
            publication.collection,
            publication.recording_key,
            row["psg_bucket"],
            row["psg_object_key"],
            row["hypnogram_bucket"],
            row["hypnogram_object_key"],
            row["recording_start"],
            row["duration_seconds"],
            row["channel_count"],
            row["annotation_count"],
            row["in_range_epoch_count"],
            row["out_of_range_epoch_count"],
            row[
                "trailing_overhang_seconds"
            ],
            publication.psg_file_id,
            publication.hypnogram_file_id,
            publication.source_pair_id,
            publication.input_fingerprint,
            publication.config_id,
            publication.schema_version,
            publication.transform_version,
            publication.psg_checksum_sha256,
            (
                publication
                .hypnogram_checksum_sha256
            ),
            publication.silver_bucket,
            publication.output_prefix,
            run_id,
        ),
    )


def _insert_channels(
    *,
    cursor,
    loaded: LoadedRecordingPublication,
) -> None:
    rows = loaded.channels_table.to_pylist()

    cursor.executemany(
        """
        insert into staging.silver_channels (
            channel_id,
            recording_id,
            position,
            source_label,
            normalized_name,
            sampling_frequency_hz,
            physical_dimension,
            physical_min,
            physical_max,
            digital_min,
            digital_max,
            samples_per_data_record,
            prefiltering
        )
        values (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        );
        """,
        [
            (
                UUID(row["channel_id"]),
                UUID(row["recording_id"]),
                row["position"],
                row["source_label"],
                row["normalized_name"],
                row["sampling_frequency_hz"],
                row["physical_dimension"],
                row["physical_min"],
                row["physical_max"],
                row["digital_min"],
                row["digital_max"],
                row[
                    "samples_per_data_record"
                ],
                row["prefiltering"],
            )
            for row in rows
        ],
    )


def _insert_intervals(
    *,
    cursor,
    loaded: LoadedRecordingPublication,
) -> None:
    rows = loaded.intervals_table.to_pylist()

    cursor.executemany(
        """
        insert into
            staging.silver_sleep_stage_intervals (
                interval_id,
                recording_id,
                source_annotation_index,
                onset_seconds,
                duration_seconds,
                end_seconds,
                source_label,
                normalized_stage,
                overlap_status
            )
        values (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        );
        """,
        [
            (
                UUID(row["interval_id"]),
                UUID(row["recording_id"]),
                row[
                    "source_annotation_index"
                ],
                row["onset_seconds"],
                row["duration_seconds"],
                row["end_seconds"],
                row["source_label"],
                row["normalized_stage"],
                row["overlap_status"],
            )
            for row in rows
        ],
    )


def _insert_epochs(
    *,
    cursor,
    loaded: LoadedRecordingPublication,
) -> None:
    rows = loaded.epochs_table.to_pylist()

    cursor.executemany(
        """
        insert into
            staging.silver_sleep_stage_epochs (
                epoch_id,
                recording_id,
                source_interval_id,
                source_annotation_index,
                epoch_number,
                start_seconds,
                duration_seconds,
                end_seconds,
                source_label,
                normalized_stage
            )
        values (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        );
        """,
        [
            (
                UUID(row["epoch_id"]),
                UUID(row["recording_id"]),
                UUID(
                    row["source_interval_id"]
                ),
                row[
                    "source_annotation_index"
                ],
                row["epoch_number"],
                row["start_seconds"],
                row["duration_seconds"],
                row["end_seconds"],
                row["source_label"],
                row["normalized_stage"],
            )
            for row in rows
        ],
    )


def load_recording_metadata_to_staging(
    *,
    run_id: UUID,
    settings: Settings | None = None,
    client: BaseClient | None = None,
) -> RecordingStagingLoadResult:
    if settings is None:
        settings = get_settings()

    owns_client = client is None
    if client is None:
        client = get_object_storage_client(
            settings
        )

    try:
        publications = (
            discover_current_recording_publications(
                settings=settings,
                client=client,
            )
        )

        already_complete: set[UUID] = set()

        with get_postgres_connection(
            settings=settings
        ) as connection:
            with connection.cursor() as cursor:
                for publication in publications:
                    if _publication_is_complete(
                        cursor=cursor,
                        publication=publication,
                    ):
                        already_complete.add(
                            publication.recording_id
                        )

        missing_publications = [
            publication
            for publication in publications
            if publication.recording_id
            not in already_complete
        ]

        if not missing_publications:
            return RecordingStagingLoadResult(
                status="skipped",
                publication_count=len(
                    publications
                ),
                publications_written=0,
                publications_skipped=len(
                    publications
                ),
                recordings_count=sum(
                    item.object_for(
                        "recordings"
                    ).row_count
                    for item in publications
                ),
                channels_count=sum(
                    item.object_for(
                        "channels"
                    ).row_count
                    for item in publications
                ),
                interval_count=sum(
                    item.object_for(
                        "sleep_stage_intervals"
                    ).row_count
                    for item in publications
                ),
                epoch_count=sum(
                    item.object_for(
                        "sleep_stage_epochs"
                    ).row_count
                    for item in publications
                ),
                rows_written=0,
                files_processed=0,
            )

        with TemporaryDirectory(
            prefix=(
                "neuro_sleep_recording_"
                "staging_"
            )
        ) as temporary_directory:
            temporary_root = Path(
                temporary_directory
            )

            loaded_publications = [
                _load_publication_files(
                    publication=publication,
                    temporary_root=temporary_root,
                    client=client,
                )
                for publication
                in missing_publications
            ]

            written_publications: list[
                RecordingPublication
            ] = []

            with get_postgres_connection(
                settings=settings
            ) as connection:
                with connection.cursor() as cursor:
                    for loaded in (
                        loaded_publications
                    ):
                        publication = (
                            loaded.publication
                        )

                        if _publication_is_complete(
                            cursor=cursor,
                            publication=publication,
                        ):
                            continue

                        _insert_recording(
                            cursor=cursor,
                            loaded=loaded,
                            run_id=run_id,
                        )
                        _insert_channels(
                            cursor=cursor,
                            loaded=loaded,
                        )
                        _insert_intervals(
                            cursor=cursor,
                            loaded=loaded,
                        )
                        _insert_epochs(
                            cursor=cursor,
                            loaded=loaded,
                        )

                        if not _publication_is_complete(
                            cursor=cursor,
                            publication=publication,
                        ):
                            raise RuntimeError(
                                "Recording publication "
                                "did not load completely: "
                                f"{publication.recording_key}"
                            )

                        written_publications.append(
                            publication
                        )

        rows_written = sum(
            publication.expected_rows
            for publication
            in written_publications
        )
        files_processed = sum(
            publication.expected_files
            for publication
            in written_publications
        )

        return RecordingStagingLoadResult(
            status=(
                "written"
                if written_publications
                else "skipped"
            ),
            publication_count=len(
                publications
            ),
            publications_written=len(
                written_publications
            ),
            publications_skipped=(
                len(publications)
                - len(written_publications)
            ),
            recordings_count=sum(
                item.object_for(
                    "recordings"
                ).row_count
                for item in publications
            ),
            channels_count=sum(
                item.object_for(
                    "channels"
                ).row_count
                for item in publications
            ),
            interval_count=sum(
                item.object_for(
                    "sleep_stage_intervals"
                ).row_count
                for item in publications
            ),
            epoch_count=sum(
                item.object_for(
                    "sleep_stage_epochs"
                ).row_count
                for item in publications
            ),
            rows_written=rows_written,
            files_processed=files_processed,
        )
    finally:
        if owns_client:
            client.close()
