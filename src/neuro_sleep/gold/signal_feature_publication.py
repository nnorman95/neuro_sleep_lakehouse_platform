from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Literal

from botocore.client import BaseClient

from neuro_sleep.spark.signal_features import (
    FEATURE_VERSION,
    WINDOW_SECONDS,
)
from neuro_sleep.spark.signal_input import (
    SelectedSignalInput,
)
from neuro_sleep.storage.object_storage import (
    delete_object,
    get_object_metadata,
    list_object_summaries,
)
from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)


GOLD_BUCKET = "gold"
GOLD_DATASET_NAME = "signal_features"
GOLD_SCHEMA_VERSION = "1.0.0"
SUCCESS_OBJECT_NAME = "_SUCCESS.json"
SPARK_SUCCESS_OBJECT_NAME = "_SUCCESS"

PublicationStatus = Literal[
    "written",
    "skipped",
]


class PartialGoldOutputError(
    RuntimeError
):
    """Raised for an incomplete Gold prefix."""


class InvalidGoldPublicationError(
    RuntimeError
):
    """Raised when a completed Gold prefix is invalid."""


@dataclass(frozen=True)
class GoldSignalFeaturePublication:
    status: PublicationStatus
    output_prefix: str
    recording_key: str
    recording_id: str
    row_count: int
    partial_window_count: int
    data_object_count: int
    recovered_partial_output: bool = False
    recovered_object_count: int = 0


def build_gold_output_prefix(
    item: SelectedSignalInput,
) -> str:
    return (
        "physionet/sleep-edfx/"
        f"{item.dataset_version}/"
        f"{GOLD_DATASET_NAME}/"
        f"{item.collection}/"
        f"{item.recording_key}/"
        "schema_version="
        f"{GOLD_SCHEMA_VERSION}/"
        "feature_version="
        f"{FEATURE_VERSION}/"
        "input_recording_id="
        f"{item.recording_id}"
    )


def build_success_object_key(
    output_prefix: str,
) -> str:
    return (
        f"{output_prefix}/"
        f"{SUCCESS_OBJECT_NAME}"
    )


def build_data_prefix(
    output_prefix: str,
) -> str:
    return (
        f"{output_prefix}/data"
    )


def _list_prefix_objects(
    *,
    output_prefix: str,
    client: BaseClient,
):
    return list_object_summaries(
        bucket=GOLD_BUCKET,
        prefix=f"{output_prefix}/",
        client=client,
    )


def _read_json_object(
    *,
    object_key: str,
    client: BaseClient,
) -> dict[str, object]:
    response = run_object_storage_operation(
        operation=lambda: client.get_object(
            Bucket=GOLD_BUCKET,
            Key=object_key,
        ),
        operation_name=(
            "get_object:"
            f"{GOLD_BUCKET}/{object_key}"
        ),
    )

    try:
        body = response["Body"].read()
    finally:
        response["Body"].close()

    value = json.loads(
        body.decode("utf-8")
    )
    if not isinstance(value, dict):
        raise InvalidGoldPublicationError(
            "Gold success manifest must "
            "be a JSON object"
        )

    return value


def _delete_prefix_objects(
    *,
    output_prefix: str,
    client: BaseClient,
) -> int:
    objects = _list_prefix_objects(
        output_prefix=output_prefix,
        client=client,
    )

    for item in objects:
        delete_object(
            bucket=GOLD_BUCKET,
            object_key=item.object_key,
            client=client,
        )

    remaining = _list_prefix_objects(
        output_prefix=output_prefix,
        client=client,
    )
    if remaining:
        raise PartialGoldOutputError(
            "Gold partial-output recovery "
            "did not remove every object"
        )

    return len(objects)


def recover_partial_gold_prefix(
    *,
    output_prefix: str,
    client: BaseClient,
) -> int:
    objects = _list_prefix_objects(
        output_prefix=output_prefix,
        client=client,
    )
    if not objects:
        return 0

    success_key = build_success_object_key(
        output_prefix
    )
    existing_keys = {
        item.object_key
        for item in objects
    }

    if success_key in existing_keys:
        raise InvalidGoldPublicationError(
            "Automatic recovery refuses to "
            "delete a Gold prefix that has "
            f"{SUCCESS_OBJECT_NAME}: "
            f"{output_prefix}"
        )

    return _delete_prefix_objects(
        output_prefix=output_prefix,
        client=client,
    )


def remove_spark_success_marker(
    *,
    output_prefix: str,
    client: BaseClient,
) -> None:
    marker_key = (
        f"{build_data_prefix(output_prefix)}/"
        f"{SPARK_SUCCESS_OBJECT_NAME}"
    )

    keys = {
        item.object_key
        for item in _list_prefix_objects(
            output_prefix=output_prefix,
            client=client,
        )
    }

    if marker_key in keys:
        delete_object(
            bucket=GOLD_BUCKET,
            object_key=marker_key,
            client=client,
        )


def inspect_written_data_object(
    *,
    output_prefix: str,
    client: BaseClient,
) -> dict[str, object]:
    data_prefix = (
        f"{build_data_prefix(output_prefix)}/"
    )
    objects = list_object_summaries(
        bucket=GOLD_BUCKET,
        prefix=data_prefix,
        client=client,
    )

    parquet_objects = [
        item
        for item in objects
        if item.object_key.endswith(
            ".parquet"
        )
    ]

    unexpected = [
        item.object_key
        for item in objects
        if (
            not item.object_key.endswith(
                ".parquet"
            )
            and not (
                item.object_key.endswith("/")
                and item.content_length == 0
            )
        )
    ]

    if unexpected:
        raise PartialGoldOutputError(
            "Unexpected objects remain in "
            "Gold data prefix: "
            + ", ".join(unexpected)
        )

    if len(parquet_objects) != 1:
        raise PartialGoldOutputError(
            "Gold signal features must publish "
            "exactly one Parquet data object "
            "per recording; found "
            f"{len(parquet_objects)}"
        )

    data_object = parquet_objects[0]
    metadata = get_object_metadata(
        bucket=GOLD_BUCKET,
        object_key=data_object.object_key,
        client=client,
    )

    return {
        "bucket": GOLD_BUCKET,
        "object_key": data_object.object_key,
        "file_size_bytes": (
            data_object.content_length
        ),
        "etag": metadata.get("etag"),
    }


def build_success_manifest(
    *,
    item: SelectedSignalInput,
    output_prefix: str,
    row_count: int,
    partial_window_count: int,
    data_object: dict[str, object],
    spark_version: str,
) -> dict[str, object]:
    return {
        "status": "complete",
        "lakehouse_layer": "gold",
        "dataset_name": (
            GOLD_DATASET_NAME
        ),
        "schema_version": (
            GOLD_SCHEMA_VERSION
        ),
        "feature_version": (
            FEATURE_VERSION
        ),
        "window_seconds": (
            WINDOW_SECONDS
        ),
        "published_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "recording": {
            "source_system": (
                item.source_system
            ),
            "dataset_version": (
                item.dataset_version
            ),
            "collection": (
                item.collection
            ),
            "recording_key": (
                item.recording_key
            ),
            "recording_id": (
                item.recording_id
            ),
        },
        "source_silver": {
            "bucket": item.bucket,
            "output_prefix": (
                item.output_prefix
            ),
            "recording_id": (
                item.recording_id
            ),
            "signal_file_count": (
                item.signal_file_count
            ),
            "signal_row_count": (
                item.signal_row_count
            ),
            "signal_size_bytes": (
                item.signal_size_bytes
            ),
        },
        "engine": {
            "name": "apache_spark",
            "version": spark_version,
        },
        "grain": [
            "recording_id",
            "channel_id",
            "epoch_number",
        ],
        "row_count": row_count,
        "partial_window_count": (
            partial_window_count
        ),
        "data_object_count": 1,
        "objects": [
            data_object
        ],
        "output_prefix": output_prefix,
    }


def upload_success_manifest(
    *,
    output_prefix: str,
    manifest: dict[str, object],
    client: BaseClient,
) -> None:
    object_key = build_success_object_key(
        output_prefix
    )
    body = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    run_object_storage_operation(
        operation=lambda: client.put_object(
            Bucket=GOLD_BUCKET,
            Key=object_key,
            Body=body,
            ContentLength=len(body),
            ContentType="application/json",
            Metadata={
                "lakehouse_layer": "gold",
                "artifact": (
                    "success_manifest"
                ),
                "dataset_name": (
                    GOLD_DATASET_NAME
                ),
                "schema_version": (
                    GOLD_SCHEMA_VERSION
                ),
                "feature_version": (
                    FEATURE_VERSION
                ),
            },
        ),
        operation_name=(
            "put_object:"
            f"{GOLD_BUCKET}/{object_key}"
        ),
    )


def _require_int(
    value: object,
    *,
    field_name: str,
) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise InvalidGoldPublicationError(
            f"{field_name} must be a "
            "non-negative integer"
        )

    return value


def validate_existing_publication(
    *,
    item: SelectedSignalInput,
    output_prefix: str,
    expected_row_count: int,
    expected_partial_window_count: int,
    client: BaseClient,
) -> GoldSignalFeaturePublication:
    objects = _list_prefix_objects(
        output_prefix=output_prefix,
        client=client,
    )
    success_key = build_success_object_key(
        output_prefix
    )
    existing_keys = {
        value.object_key
        for value in objects
        if not (
            value.object_key.endswith("/")
            and value.content_length == 0
        )
    }

    if success_key not in existing_keys:
        raise PartialGoldOutputError(
            "Gold prefix has data but no "
            f"{SUCCESS_OBJECT_NAME}: "
            f"{output_prefix}"
        )

    manifest = _read_json_object(
        object_key=success_key,
        client=client,
    )

    required_equalities = {
        "status": "complete",
        "lakehouse_layer": "gold",
        "dataset_name": (
            GOLD_DATASET_NAME
        ),
        "schema_version": (
            GOLD_SCHEMA_VERSION
        ),
        "feature_version": (
            FEATURE_VERSION
        ),
        "window_seconds": (
            WINDOW_SECONDS
        ),
        "output_prefix": output_prefix,
        "row_count": (
            expected_row_count
        ),
        "partial_window_count": (
            expected_partial_window_count
        ),
        "data_object_count": 1,
    }

    for key, expected in (
        required_equalities.items()
    ):
        if manifest.get(key) != expected:
            raise InvalidGoldPublicationError(
                "Gold success manifest "
                f"{key} mismatch: "
                f"expected={expected!r} "
                f"actual={manifest.get(key)!r}"
            )

    recording = manifest.get(
        "recording"
    )
    if not isinstance(recording, dict):
        raise InvalidGoldPublicationError(
            "Gold manifest recording "
            "section is invalid"
        )

    if (
        recording.get("recording_key")
        != item.recording_key
        or recording.get("recording_id")
        != item.recording_id
        or recording.get("collection")
        != item.collection
        or recording.get(
            "dataset_version"
        )
        != item.dataset_version
        or recording.get(
            "source_system"
        )
        != item.source_system
    ):
        raise InvalidGoldPublicationError(
            "Gold manifest recording "
            "identity does not match current "
            "selected Silver input"
        )

    source_silver = manifest.get(
        "source_silver"
    )
    if not isinstance(
        source_silver,
        dict,
    ):
        raise InvalidGoldPublicationError(
            "Gold manifest source_silver "
            "section is invalid"
        )

    if (
        source_silver.get("bucket")
        != item.bucket
        or source_silver.get(
            "output_prefix"
        )
        != item.output_prefix
        or source_silver.get(
            "recording_id"
        )
        != item.recording_id
        or source_silver.get(
            "signal_file_count"
        )
        != item.signal_file_count
        or source_silver.get(
            "signal_row_count"
        )
        != item.signal_row_count
        or source_silver.get(
            "signal_size_bytes"
        )
        != item.signal_size_bytes
    ):
        raise InvalidGoldPublicationError(
            "Gold manifest Silver lineage "
            "does not match current selected "
            "Silver input"
        )

    manifest_objects = manifest.get(
        "objects"
    )
    if (
        not isinstance(
            manifest_objects,
            list,
        )
        or len(manifest_objects) != 1
        or not isinstance(
            manifest_objects[0],
            dict,
        )
    ):
        raise InvalidGoldPublicationError(
            "Gold manifest must list exactly "
            "one data object"
        )

    data_object = manifest_objects[0]

    if (
        data_object.get("bucket")
        != GOLD_BUCKET
    ):
        raise InvalidGoldPublicationError(
            "Gold manifest data object bucket "
            "does not match the Gold bucket"
        )

    object_key = data_object.get(
        "object_key"
    )
    if not isinstance(
        object_key,
        str,
    ):
        raise InvalidGoldPublicationError(
            "Gold manifest data object key "
            "is invalid"
        )

    expected_keys = {
        success_key,
        object_key,
    }
    if existing_keys != expected_keys:
        raise InvalidGoldPublicationError(
            "Stored Gold objects do not match "
            "the success manifest: "
            f"expected={len(expected_keys)} "
            f"actual={len(existing_keys)}"
        )

    metadata = get_object_metadata(
        bucket=GOLD_BUCKET,
        object_key=object_key,
        client=client,
    )

    expected_size = _require_int(
        data_object.get(
            "file_size_bytes"
        ),
        field_name=(
            "manifest.objects[0]."
            "file_size_bytes"
        ),
    )

    if (
        metadata.get(
            "content_length"
        )
        != expected_size
    ):
        raise InvalidGoldPublicationError(
            "Gold data object size does "
            "not match manifest"
        )

    if (
        data_object.get("etag")
        != metadata.get("etag")
    ):
        raise InvalidGoldPublicationError(
            "Gold data object ETag does "
            "not match manifest"
        )

    return GoldSignalFeaturePublication(
        status="skipped",
        output_prefix=output_prefix,
        recording_key=(
            item.recording_key
        ),
        recording_id=item.recording_id,
        row_count=expected_row_count,
        partial_window_count=(
            expected_partial_window_count
        ),
        data_object_count=1,
    )


def inspect_publication_state(
    *,
    item: SelectedSignalInput,
    expected_row_count: int,
    expected_partial_window_count: int,
    client: BaseClient,
) -> (
    GoldSignalFeaturePublication
    | tuple[str, int]
):
    output_prefix = (
        build_gold_output_prefix(item)
    )
    objects = _list_prefix_objects(
        output_prefix=output_prefix,
        client=client,
    )

    if not objects:
        return ("write", 0)

    success_key = build_success_object_key(
        output_prefix
    )
    keys = {
        value.object_key
        for value in objects
    }

    if success_key in keys:
        return (
            validate_existing_publication(
                item=item,
                output_prefix=(
                    output_prefix
                ),
                expected_row_count=(
                    expected_row_count
                ),
                expected_partial_window_count=(
                    expected_partial_window_count
                ),
                client=client,
            )
        )

    recovered_count = (
        recover_partial_gold_prefix(
            output_prefix=(
                output_prefix
            ),
            client=client,
        )
    )

    return (
        "write",
        recovered_count,
    )
