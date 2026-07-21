from uuid import UUID

from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.idempotency import (
    SUCCESS_OBJECT_NAME,
    read_success_manifest,
    write_silver_recording_idempotent,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
)


BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

PSG_OBJECT_KEY = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
    "SC4001E0-PSG.edf"
)

HYPNOGRAM_OBJECT_KEY = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/"
    "SC4001EC-Hypnogram.edf"
)


def run_smoke_test() -> None:
    smoke_run_id = new_uuid7()

    root_prefix = (
        "smoke-tests/"
        "silver-idempotency/"
        f"run_id={smoke_run_id}"
    )

    client = get_object_storage_client()

    output_prefix: str | None = None

    try:
        first_result = (
            write_silver_recording_idempotent(
                psg_bucket=BRONZE_BUCKET,
                psg_object_key=(
                    PSG_OBJECT_KEY
                ),
                hypnogram_bucket=(
                    BRONZE_BUCKET
                ),
                hypnogram_object_key=(
                    HYPNOGRAM_OBJECT_KEY
                ),
                silver_bucket=(
                    SILVER_BUCKET
                ),
                root_prefix=root_prefix,
                signal_chunk_duration_seconds=(
                    30.0
                ),
                signal_start_seconds=0.0,
                signal_stop_seconds=60.0,
                client=client,
            )
        )

        output_prefix = (
            first_result.output_prefix
        )

        second_result = (
            write_silver_recording_idempotent(
                psg_bucket=BRONZE_BUCKET,
                psg_object_key=(
                    PSG_OBJECT_KEY
                ),
                hypnogram_bucket=(
                    BRONZE_BUCKET
                ),
                hypnogram_object_key=(
                    HYPNOGRAM_OBJECT_KEY
                ),
                silver_bucket=(
                    SILVER_BUCKET
                ),
                root_prefix=root_prefix,
                signal_chunk_duration_seconds=(
                    30.0
                ),
                signal_start_seconds=0.0,
                signal_stop_seconds=60.0,
                client=client,
            )
        )

        if first_result.status != "written":
            raise RuntimeError(
                "First idempotent write was "
                "not executed"
            )

        if second_result.status != (
            "skipped"
        ):
            raise RuntimeError(
                "Second idempotent write was "
                "not skipped"
            )

        if first_result.write_result is None:
            raise RuntimeError(
                "First result has no writer "
                "result"
            )

        if second_result.write_result is not (
            None
        ):
            raise RuntimeError(
                "Skipped result unexpectedly "
                "contains a writer result"
            )

        if (
            first_result.source_pair_id
            != second_result.source_pair_id
        ):
            raise RuntimeError(
                "Source pair IDs differ"
            )

        if (
            first_result.config_id
            != second_result.config_id
        ):
            raise RuntimeError(
                "Transform config IDs differ"
            )

        if (
            first_result.output_prefix
            != second_result.output_prefix
        ):
            raise RuntimeError(
                "Idempotent output prefixes "
                "differ"
            )

        if (
            first_result.recording_id
            != second_result.recording_id
        ):
            raise RuntimeError(
                "Skipped result did not reuse "
                "the original recording_id"
            )

        if first_result.recording_id.version != 7:
            raise RuntimeError(
                "Recording ID is not UUIDv7"
            )

        if (
            first_result.data_object_count
            != 18
            or second_result
            .data_object_count
            != 18
        ):
            raise RuntimeError(
                "Unexpected data object count"
            )

        if (
            first_result.total_object_count
            != 19
            or second_result
            .total_object_count
            != 19
        ):
            raise RuntimeError(
                "Unexpected total object count"
            )

        stored_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    output_prefix + "/"
                ),
                client=client,
            )
        )

        stored_keys = [
            item.object_key
            for item in stored_objects
        ]

        if len(stored_keys) != 19:
            raise RuntimeError(
                "Repeated run changed MinIO "
                "object count"
            )

        if len(stored_keys) != len(
            set(stored_keys)
        ):
            raise RuntimeError(
                "Duplicate MinIO object keys "
                "were detected"
            )

        success_key = (
            f"{output_prefix}/"
            f"{SUCCESS_OBJECT_NAME}"
        )

        if success_key not in stored_keys:
            raise RuntimeError(
                "Success manifest is missing"
            )

        manifest = read_success_manifest(
            bucket=SILVER_BUCKET,
            output_prefix=output_prefix,
            client=client,
        )

        if (
            manifest.get(
                "data_object_count"
            )
            != 18
        ):
            raise RuntimeError(
                "Manifest data object count "
                "is incorrect"
            )

        if manifest.get("row_count") != (
            21052
        ):
            raise RuntimeError(
                "Manifest row count is "
                "incorrect"
            )

        manifest_recording_id = UUID(
            str(
                manifest[
                    "recording_id"
                ]
            )
        )

        if (
            manifest_recording_id
            != first_result.recording_id
        ):
            raise RuntimeError(
                "Manifest recording ID "
                "mismatch"
            )

        print(
            "first_silver_run_status=written"
        )
        print(
            "second_silver_run_status=skipped"
        )
        print(
            "source_pair_id_stable=true"
        )
        print(
            "transform_config_id_stable=true"
        )
        print(
            "output_prefix_stable=true"
        )
        print(
            "recording_uuid7_reused=true"
        )
        print(
            "silver_data_object_count=18"
        )
        print(
            "silver_success_manifest_count=1"
        )
        print(
            "silver_total_object_count=19"
        )
        print(
            "duplicate_object_keys_found=false"
        )
        print(
            "success_manifest_valid=true"
        )

    finally:
        stored_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    root_prefix + "/"
                ),
                client=client,
            )
        )

        for item in stored_objects:
            client.delete_object(
                Bucket=SILVER_BUCKET,
                Key=item.object_key,
            )

        remaining_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    root_prefix + "/"
                ),
                client=client,
            )
        )

        client.close()

    if remaining_objects:
        raise RuntimeError(
            "Idempotency smoke objects were "
            "not cleaned up"
        )

    print(
        "silver_idempotency_cleanup=true"
    )
    print(
        "silver_idempotency_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
