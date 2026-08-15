from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from botocore.client import BaseClient

from neuro_sleep.config import Settings, get_settings
from neuro_sleep.db.postgres import get_postgres_connection
from neuro_sleep.silver.idempotency import read_success_manifest
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
)


@dataclass(frozen=True)
class SelectedSignalInput:
    source_system: str
    dataset_version: str
    collection: str
    recording_key: str
    recording_id: str
    bucket: str
    output_prefix: str
    signal_object_keys: tuple[str, ...]
    signal_file_count: int
    signal_row_count: int
    signal_size_bytes: int


def _require_string(
    value: object,
    *,
    field_name: str,
) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")

    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} cannot be empty")

    return cleaned


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
        raise ValueError(
            f"{field_name} must be a non-negative integer"
        )

    return value


def _fetch_warehouse_recordings(
    settings: Settings,
) -> list[tuple[Any, ...]]:
    with get_postgres_connection(settings=settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    source_system,
                    dataset_version,
                    collection,
                    recording_key,
                    silver_recording_id,
                    silver_bucket,
                    silver_output_prefix
                from warehouse.dim_recording
                where silver_recording_id is not null
                  and silver_bucket is not null
                  and silver_output_prefix is not null
                order by
                    source_system,
                    dataset_version,
                    collection,
                    recording_key;
                """
            )
            return cursor.fetchall()


def _validate_manifest_header(
    *,
    manifest: dict[str, object],
    warehouse_recording_id: str,
    recording_key: str,
) -> bool:
    if manifest.get("status") != "complete":
        raise RuntimeError(
            "Warehouse-selected Silver representation is not complete: "
            f"{recording_key}"
        )

    if manifest.get("lakehouse_layer") != "silver":
        raise RuntimeError(
            "Warehouse-selected representation does not identify itself "
            f"as Silver: {recording_key}"
        )

    manifest_recording_id = _require_string(
        manifest.get("recording_id"),
        field_name="manifest.recording_id",
    )
    if manifest_recording_id != warehouse_recording_id:
        raise RuntimeError(
            "Warehouse silver_recording_id does not match the selected "
            f"Silver success manifest: {recording_key}"
        )

    transform_config = manifest.get("transform_config")
    if not isinstance(transform_config, dict):
        raise RuntimeError(
            "Silver success manifest has no valid transform_config: "
            f"{recording_key}"
        )

    if "include_signals" in transform_config:
        include_signals = transform_config["include_signals"]

        if include_signals is False:
            return False

        if include_signals is True:
            return True

        raise RuntimeError(
            "Silver success manifest has an invalid include_signals value: "
            f"{recording_key}"
        )

    # Backward compatibility for full-signal Silver publications written
    # before transform_config.include_signals was introduced.
    #
    # Missing include_signals is accepted only when the immutable success
    # manifest itself proves that signal Parquet was published.
    objects = manifest.get("objects")
    if not isinstance(objects, list):
        raise RuntimeError(
            "Legacy Silver success manifest has no valid object list: "
            f"{recording_key}"
        )

    legacy_signal_objects = [
        item
        for item in objects
        if isinstance(item, dict)
        and isinstance(item.get("object_key"), str)
        and "/signals/" in item["object_key"]
        and item["object_key"].endswith(".parquet")
    ]

    if legacy_signal_objects:
        return True

    raise RuntimeError(
        "Silver success manifest has no include_signals marker and publishes "
        "no signal Parquet; refusing to infer signal mode: "
        f"{recording_key}"
    )


def _signal_objects_from_manifest(
    *,
    manifest: dict[str, object],
    bucket: str,
    output_prefix: str,
    recording_key: str,
) -> tuple[tuple[str, ...], int, int]:
    objects = manifest.get("objects")
    if not isinstance(objects, list):
        raise RuntimeError(
            "Silver success manifest has no valid object list: "
            f"{recording_key}"
        )

    signal_prefix = f"{output_prefix}/signals/"
    keys: list[str] = []
    row_count = 0
    size_bytes = 0

    for item in objects:
        if not isinstance(item, dict):
            raise RuntimeError(
                "Silver success manifest contains an invalid object entry: "
                f"{recording_key}"
            )

        object_key = item.get("object_key")
        if not isinstance(object_key, str):
            raise RuntimeError(
                "Silver success manifest contains an object without a valid "
                f"key: {recording_key}"
            )

        if not object_key.startswith(signal_prefix):
            continue

        if not object_key.endswith(".parquet"):
            raise RuntimeError(
                "Selected Silver signal object is not Parquet: "
                f"{object_key}"
            )

        object_bucket = _require_string(
            item.get("bucket"),
            field_name="manifest.object.bucket",
        )
        if object_bucket != bucket:
            raise RuntimeError(
                "Selected Silver signal object bucket does not match "
                f"Warehouse: {recording_key}"
            )

        row_count += _require_non_negative_int(
            item.get("row_count"),
            field_name="manifest.object.row_count",
        )
        size_bytes += _require_non_negative_int(
            item.get("file_size_bytes"),
            field_name="manifest.object.file_size_bytes",
        )
        keys.append(object_key)

    if not keys:
        raise RuntimeError(
            "Silver representation says signals are included but publishes "
            f"no signal Parquet: {recording_key}"
        )

    if len(keys) != len(set(keys)):
        raise RuntimeError(
            "Duplicate signal object keys in Silver success manifest: "
            f"{recording_key}"
        )

    return tuple(sorted(keys)), row_count, size_bytes


def _verify_live_signal_objects(
    *,
    client: BaseClient,
    bucket: str,
    output_prefix: str,
    manifest_keys: tuple[str, ...],
    recording_key: str,
) -> None:
    live_objects = list_object_summaries(
        bucket=bucket,
        prefix=f"{output_prefix}/signals/",
        client=client,
    )
    live_keys = {
        item.object_key
        for item in live_objects
        if item.object_key.endswith(".parquet")
    }
    expected_keys = set(manifest_keys)

    if live_keys == expected_keys:
        return

    missing_count = len(expected_keys - live_keys)
    unexpected_count = len(live_keys - expected_keys)

    raise RuntimeError(
        "Selected Silver signal objects do not match the success manifest "
        f"for {recording_key}: missing={missing_count} "
        f"unexpected={unexpected_count}"
    )


def discover_selected_signal_inputs(
    *,
    settings: Settings | None = None,
    verify_live_objects: bool = True,
) -> tuple[SelectedSignalInput, ...]:
    if settings is None:
        settings = get_settings()

    rows = _fetch_warehouse_recordings(settings)
    if not rows:
        raise RuntimeError(
            "Warehouse contains no selected Silver recordings"
        )

    client = get_object_storage_client(settings=settings)
    selected: list[SelectedSignalInput] = []

    try:
        for row in rows:
            (
                source_system,
                dataset_version,
                collection,
                recording_key,
                silver_recording_id,
                silver_bucket,
                silver_output_prefix,
            ) = row

            recording_key_text = str(recording_key)
            recording_id_text = str(silver_recording_id)
            bucket_text = _require_string(
                silver_bucket,
                field_name="warehouse.silver_bucket",
            )
            output_prefix_text = _require_string(
                silver_output_prefix,
                field_name="warehouse.silver_output_prefix",
            )

            manifest = read_success_manifest(
                bucket=bucket_text,
                output_prefix=output_prefix_text,
                client=client,
            )

            has_signals = _validate_manifest_header(
                manifest=manifest,
                warehouse_recording_id=recording_id_text,
                recording_key=recording_key_text,
            )
            if not has_signals:
                continue

            signal_keys, signal_rows, signal_bytes = (
                _signal_objects_from_manifest(
                    manifest=manifest,
                    bucket=bucket_text,
                    output_prefix=output_prefix_text,
                    recording_key=recording_key_text,
                )
            )

            if verify_live_objects:
                _verify_live_signal_objects(
                    client=client,
                    bucket=bucket_text,
                    output_prefix=output_prefix_text,
                    manifest_keys=signal_keys,
                    recording_key=recording_key_text,
                )

            selected.append(
                SelectedSignalInput(
                    source_system=str(source_system),
                    dataset_version=str(dataset_version),
                    collection=str(collection),
                    recording_key=recording_key_text,
                    recording_id=recording_id_text,
                    bucket=bucket_text,
                    output_prefix=output_prefix_text,
                    signal_object_keys=signal_keys,
                    signal_file_count=len(signal_keys),
                    signal_row_count=signal_rows,
                    signal_size_bytes=signal_bytes,
                )
            )
    finally:
        client.close()

    if not selected:
        raise RuntimeError(
            "No Warehouse-selected Silver representations contain signals"
        )

    return tuple(selected)
