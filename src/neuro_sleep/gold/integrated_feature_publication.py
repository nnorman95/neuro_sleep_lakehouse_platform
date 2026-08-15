from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Literal

from botocore.client import BaseClient

from neuro_sleep.gold.signal_feature_publication import (
    GOLD_BUCKET,
    GoldSignalFeaturePublication,
    build_data_prefix as build_source_data_prefix,
    build_gold_output_prefix as build_source_gold_output_prefix,
    build_success_object_key as build_source_success_object_key,
    inspect_publication_state as inspect_source_publication_state,
    inspect_written_data_object as inspect_source_data_object,
)
from neuro_sleep.reliability.object_storage_retry import (
    run_object_storage_operation,
)
from neuro_sleep.spark.feature_integration import (
    INTEGRATION_VERSION,
    EpochLabelIntegrationContext,
    RecordingChannelIntegrationContext,
)
from neuro_sleep.spark.signal_features import (
    FEATURE_VERSION,
    WINDOW_SECONDS,
)
from neuro_sleep.spark.signal_input import SelectedSignalInput
from neuro_sleep.storage.object_storage import (
    delete_object,
    get_object_metadata,
    list_object_summaries,
)


INTEGRATED_DATASET_NAME = "integrated_signal_features"
INTEGRATED_SCHEMA_VERSION = "1.0.0"
SUCCESS_OBJECT_NAME = "_SUCCESS.json"
SPARK_SUCCESS_OBJECT_NAME = "_SUCCESS"

PublicationStatus = Literal["written", "skipped"]


class PartialIntegratedGoldOutputError(RuntimeError):
    """Raised for an incomplete integrated Gold prefix."""


class InvalidIntegratedGoldPublicationError(RuntimeError):
    """Raised when a completed integrated Gold prefix is invalid."""


@dataclass(frozen=True)
class SourceGoldLineage:
    output_prefix: str
    success_object_key: str
    success_etag: str | None
    data_object_key: str
    data_object_etag: str | None
    row_count: int
    partial_window_count: int


@dataclass(frozen=True)
class IntegratedFeaturePublication:
    status: PublicationStatus
    output_prefix: str
    recording_key: str
    recording_id: str
    warehouse_context_sha256: str
    row_count: int
    labeled_row_count: int
    unlabeled_row_count: int
    partial_window_count: int
    data_object_count: int
    recovered_partial_output: bool = False
    recovered_object_count: int = 0


def build_warehouse_context_fingerprint(
    *,
    recording_contexts: tuple[RecordingChannelIntegrationContext, ...],
    epoch_contexts: tuple[EpochLabelIntegrationContext, ...],
) -> str:
    if not recording_contexts:
        raise ValueError(
            "At least one recording/channel context is required "
            "to fingerprint Warehouse integration state"
        )

    payload = {
        "recording_channels": [
            asdict(item)
            for item in sorted(
                recording_contexts,
                key=lambda value: (
                    value.recording_id,
                    value.channel_id,
                ),
            )
        ],
        "sleep_epochs": [
            asdict(item)
            for item in sorted(
                epoch_contexts,
                key=lambda value: (
                    value.recording_id,
                    value.epoch_number,
                ),
            )
        ],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def inspect_source_gold_lineage(
    *,
    item: SelectedSignalInput,
    expected_row_count: int,
    expected_partial_window_count: int,
    client: BaseClient,
) -> SourceGoldLineage:
    state = inspect_source_publication_state(
        item=item,
        expected_row_count=expected_row_count,
        expected_partial_window_count=expected_partial_window_count,
        client=client,
    )
    if not isinstance(state, GoldSignalFeaturePublication):
        raise RuntimeError(
            "Integrated publication requires a completed source Gold "
            f"signal-feature publication: {item.recording_key}"
        )

    output_prefix = build_source_gold_output_prefix(item)
    success_key = build_source_success_object_key(output_prefix)
    success_metadata = get_object_metadata(
        bucket=GOLD_BUCKET,
        object_key=success_key,
        client=client,
    )
    data_object = inspect_source_data_object(
        output_prefix=output_prefix,
        client=client,
    )

    return SourceGoldLineage(
        output_prefix=output_prefix,
        success_object_key=success_key,
        success_etag=success_metadata.get("etag"),
        data_object_key=str(data_object["object_key"]),
        data_object_etag=(
            None
            if data_object.get("etag") is None
            else str(data_object["etag"])
        ),
        row_count=expected_row_count,
        partial_window_count=expected_partial_window_count,
    )


def build_integrated_output_prefix(
    *,
    item: SelectedSignalInput,
    warehouse_context_sha256: str,
) -> str:
    if len(warehouse_context_sha256) != 64:
        raise ValueError(
            "warehouse_context_sha256 must be a SHA-256 hex digest"
        )
    return (
        "physionet/sleep-edfx/"
        f"{item.dataset_version}/"
        f"{INTEGRATED_DATASET_NAME}/"
        f"{item.collection}/"
        f"{item.recording_key}/"
        f"schema_version={INTEGRATED_SCHEMA_VERSION}/"
        f"feature_version={FEATURE_VERSION}/"
        f"integration_version={INTEGRATION_VERSION}/"
        f"input_recording_id={item.recording_id}/"
        f"warehouse_context_sha256={warehouse_context_sha256}"
    )


def build_success_object_key(output_prefix: str) -> str:
    return f"{output_prefix}/{SUCCESS_OBJECT_NAME}"


def build_data_prefix(output_prefix: str) -> str:
    return f"{output_prefix}/data"


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
        operation_name=f"get_object:{GOLD_BUCKET}/{object_key}",
    )
    try:
        body = response["Body"].read()
    finally:
        response["Body"].close()

    value = json.loads(body.decode("utf-8"))
    if not isinstance(value, dict):
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold success manifest must be a JSON object"
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
        raise PartialIntegratedGoldOutputError(
            "Integrated Gold partial-output recovery did not "
            "remove every object"
        )
    return len(objects)


def recover_partial_integrated_prefix(
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

    success_key = build_success_object_key(output_prefix)
    keys = {item.object_key for item in objects}
    if success_key in keys:
        raise InvalidIntegratedGoldPublicationError(
            "Automatic recovery refuses to delete an integrated Gold "
            f"prefix that has {SUCCESS_OBJECT_NAME}: {output_prefix}"
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
    data_prefix = f"{build_data_prefix(output_prefix)}/"
    objects = list_object_summaries(
        bucket=GOLD_BUCKET,
        prefix=data_prefix,
        client=client,
    )
    parquet_objects = [
        item
        for item in objects
        if item.object_key.endswith(".parquet")
    ]
    unexpected = [
        item.object_key
        for item in objects
        if (
            not item.object_key.endswith(".parquet")
            and not (
                item.object_key.endswith("/")
                and item.content_length == 0
            )
        )
    ]
    if unexpected:
        raise PartialIntegratedGoldOutputError(
            "Unexpected objects remain in integrated Gold data prefix: "
            + ", ".join(unexpected)
        )
    if len(parquet_objects) != 1:
        raise PartialIntegratedGoldOutputError(
            "Integrated signal features must publish exactly one "
            f"Parquet data object per recording; found {len(parquet_objects)}"
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
        "file_size_bytes": data_object.content_length,
        "etag": metadata.get("etag"),
    }


def build_success_manifest(
    *,
    item: SelectedSignalInput,
    output_prefix: str,
    warehouse_context_sha256: str,
    recording_context_count: int,
    epoch_context_count: int,
    row_count: int,
    labeled_row_count: int,
    unlabeled_row_count: int,
    partial_window_count: int,
    source_gold: SourceGoldLineage,
    data_object: dict[str, object],
    spark_version: str,
) -> dict[str, object]:
    return {
        "status": "complete",
        "lakehouse_layer": "gold",
        "dataset_name": INTEGRATED_DATASET_NAME,
        "schema_version": INTEGRATED_SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "integration_version": INTEGRATION_VERSION,
        "window_seconds": WINDOW_SECONDS,
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "recording": {
            "source_system": item.source_system,
            "dataset_version": item.dataset_version,
            "collection": item.collection,
            "recording_key": item.recording_key,
            "recording_id": item.recording_id,
        },
        "source_gold": {
            "dataset_name": "signal_features",
            "output_prefix": source_gold.output_prefix,
            "success_object_key": source_gold.success_object_key,
            "success_etag": source_gold.success_etag,
            "data_object_key": source_gold.data_object_key,
            "data_object_etag": source_gold.data_object_etag,
            "row_count": source_gold.row_count,
            "partial_window_count": source_gold.partial_window_count,
        },
        "warehouse_context": {
            "sha256": warehouse_context_sha256,
            "recording_channel_row_count": recording_context_count,
            "sleep_epoch_row_count": epoch_context_count,
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
        "labeled_row_count": labeled_row_count,
        "unlabeled_row_count": unlabeled_row_count,
        "partial_window_count": partial_window_count,
        "data_object_count": 1,
        "objects": [data_object],
        "output_prefix": output_prefix,
    }


def upload_success_manifest(
    *,
    output_prefix: str,
    manifest: dict[str, object],
    client: BaseClient,
) -> None:
    object_key = build_success_object_key(output_prefix)
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
                "artifact": "success_manifest",
                "dataset_name": INTEGRATED_DATASET_NAME,
                "schema_version": INTEGRATED_SCHEMA_VERSION,
                "feature_version": FEATURE_VERSION,
                "integration_version": INTEGRATION_VERSION,
            },
        ),
        operation_name=f"put_object:{GOLD_BUCKET}/{object_key}",
    )


def _require_non_negative_int(
    value: object,
    *,
    field_name: str,
) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise InvalidIntegratedGoldPublicationError(
            f"{field_name} must be a non-negative integer"
        )
    return value


def validate_existing_publication(
    *,
    item: SelectedSignalInput,
    output_prefix: str,
    warehouse_context_sha256: str,
    recording_context_count: int,
    epoch_context_count: int,
    expected_row_count: int,
    expected_labeled_row_count: int,
    expected_unlabeled_row_count: int,
    expected_partial_window_count: int,
    source_gold: SourceGoldLineage,
    client: BaseClient,
) -> IntegratedFeaturePublication:
    objects = _list_prefix_objects(
        output_prefix=output_prefix,
        client=client,
    )
    success_key = build_success_object_key(output_prefix)
    existing_keys = {
        value.object_key
        for value in objects
        if not (
            value.object_key.endswith("/")
            and value.content_length == 0
        )
    }
    if success_key not in existing_keys:
        raise PartialIntegratedGoldOutputError(
            "Integrated Gold prefix has data but no "
            f"{SUCCESS_OBJECT_NAME}: {output_prefix}"
        )

    manifest = _read_json_object(
        object_key=success_key,
        client=client,
    )
    required_equalities = {
        "status": "complete",
        "lakehouse_layer": "gold",
        "dataset_name": INTEGRATED_DATASET_NAME,
        "schema_version": INTEGRATED_SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "integration_version": INTEGRATION_VERSION,
        "window_seconds": WINDOW_SECONDS,
        "output_prefix": output_prefix,
        "row_count": expected_row_count,
        "labeled_row_count": expected_labeled_row_count,
        "unlabeled_row_count": expected_unlabeled_row_count,
        "partial_window_count": expected_partial_window_count,
        "data_object_count": 1,
    }
    for key, expected in required_equalities.items():
        if manifest.get(key) != expected:
            raise InvalidIntegratedGoldPublicationError(
                "Integrated Gold success manifest "
                f"{key} mismatch: expected={expected!r} "
                f"actual={manifest.get(key)!r}"
            )

    recording = manifest.get("recording")
    if not isinstance(recording, dict):
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold manifest recording section is invalid"
        )
    expected_recording = {
        "source_system": item.source_system,
        "dataset_version": item.dataset_version,
        "collection": item.collection,
        "recording_key": item.recording_key,
        "recording_id": item.recording_id,
    }
    if recording != expected_recording:
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold recording identity does not match "
            "current selected input"
        )

    source = manifest.get("source_gold")
    if not isinstance(source, dict):
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold manifest source_gold section is invalid"
        )
    expected_source = {
        "dataset_name": "signal_features",
        "output_prefix": source_gold.output_prefix,
        "success_object_key": source_gold.success_object_key,
        "success_etag": source_gold.success_etag,
        "data_object_key": source_gold.data_object_key,
        "data_object_etag": source_gold.data_object_etag,
        "row_count": source_gold.row_count,
        "partial_window_count": source_gold.partial_window_count,
    }
    if source != expected_source:
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold source lineage does not match "
            "current validated Gold signal features"
        )

    warehouse = manifest.get("warehouse_context")
    if not isinstance(warehouse, dict):
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold manifest warehouse_context section is invalid"
        )
    expected_warehouse = {
        "sha256": warehouse_context_sha256,
        "recording_channel_row_count": recording_context_count,
        "sleep_epoch_row_count": epoch_context_count,
    }
    if warehouse != expected_warehouse:
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold Warehouse context lineage mismatch"
        )

    manifest_objects = manifest.get("objects")
    if (
        not isinstance(manifest_objects, list)
        or len(manifest_objects) != 1
        or not isinstance(manifest_objects[0], dict)
    ):
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold manifest must list exactly one data object"
        )

    data_object = manifest_objects[0]
    if data_object.get("bucket") != GOLD_BUCKET:
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold data object bucket mismatch"
        )
    object_key = data_object.get("object_key")
    if not isinstance(object_key, str):
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold manifest data object key is invalid"
        )
    if not object_key.startswith(
        f"{build_data_prefix(output_prefix)}/"
    ):
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold data object is outside the expected data prefix"
        )

    expected_keys = {success_key, object_key}
    if existing_keys != expected_keys:
        raise InvalidIntegratedGoldPublicationError(
            "Stored integrated Gold objects do not match the success manifest: "
            f"expected={len(expected_keys)} actual={len(existing_keys)}"
        )

    metadata = get_object_metadata(
        bucket=GOLD_BUCKET,
        object_key=object_key,
        client=client,
    )
    expected_size = _require_non_negative_int(
        data_object.get("file_size_bytes"),
        field_name="manifest.objects[0].file_size_bytes",
    )
    if metadata.get("content_length") != expected_size:
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold data object size does not match manifest"
        )
    if data_object.get("etag") != metadata.get("etag"):
        raise InvalidIntegratedGoldPublicationError(
            "Integrated Gold data object ETag does not match manifest"
        )

    return IntegratedFeaturePublication(
        status="skipped",
        output_prefix=output_prefix,
        recording_key=item.recording_key,
        recording_id=item.recording_id,
        warehouse_context_sha256=warehouse_context_sha256,
        row_count=expected_row_count,
        labeled_row_count=expected_labeled_row_count,
        unlabeled_row_count=expected_unlabeled_row_count,
        partial_window_count=expected_partial_window_count,
        data_object_count=1,
    )


def inspect_publication_state(
    *,
    item: SelectedSignalInput,
    warehouse_context_sha256: str,
    recording_context_count: int,
    epoch_context_count: int,
    expected_row_count: int,
    expected_labeled_row_count: int,
    expected_unlabeled_row_count: int,
    expected_partial_window_count: int,
    source_gold: SourceGoldLineage,
    client: BaseClient,
) -> IntegratedFeaturePublication | tuple[str, int]:
    output_prefix = build_integrated_output_prefix(
        item=item,
        warehouse_context_sha256=warehouse_context_sha256,
    )
    objects = _list_prefix_objects(
        output_prefix=output_prefix,
        client=client,
    )
    if not objects:
        return ("write", 0)

    success_key = build_success_object_key(output_prefix)
    keys = {value.object_key for value in objects}
    if success_key in keys:
        return validate_existing_publication(
            item=item,
            output_prefix=output_prefix,
            warehouse_context_sha256=warehouse_context_sha256,
            recording_context_count=recording_context_count,
            epoch_context_count=epoch_context_count,
            expected_row_count=expected_row_count,
            expected_labeled_row_count=expected_labeled_row_count,
            expected_unlabeled_row_count=expected_unlabeled_row_count,
            expected_partial_window_count=expected_partial_window_count,
            source_gold=source_gold,
            client=client,
        )

    recovered_count = recover_partial_integrated_prefix(
        output_prefix=output_prefix,
        client=client,
    )
    return ("write", recovered_count)
