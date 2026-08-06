from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.idempotency import (
    read_success_manifest,
    write_silver_recording_idempotent,
)
from neuro_sleep.silver.source_lineage import (
    build_input_fingerprint,
    resolve_silver_source_lineage,
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
    lineage = resolve_silver_source_lineage(
        psg_bucket=BRONZE_BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BRONZE_BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    )

    second_lineage = (
        resolve_silver_source_lineage(
            psg_bucket=BRONZE_BUCKET,
            psg_object_key=PSG_OBJECT_KEY,
            hypnogram_bucket=(
                BRONZE_BUCKET
            ),
            hypnogram_object_key=(
                HYPNOGRAM_OBJECT_KEY
            ),
        )
    )

    if lineage != second_lineage:
        raise RuntimeError(
            "Resolved source lineage is "
            "not stable"
        )

    changed_fingerprint = (
        build_input_fingerprint(
            psg_checksum_sha256=(
                "0" * 64
            ),
            hypnogram_checksum_sha256=(
                lineage
                .hypnogram_checksum_sha256
            ),
        )
    )

    if (
        changed_fingerprint
        == lineage.input_fingerprint
    ):
        raise RuntimeError(
            "Changed source checksum did not "
            "change input_fingerprint"
        )

    smoke_run_id = new_uuid7()

    root_prefix = (
        "smoke-tests/"
        "silver-input-fingerprint/"
        f"run_id={smoke_run_id}"
    )

    client = get_object_storage_client()

    try:
        write_result = (
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

        if (
            write_result.input_fingerprint
            != lineage.input_fingerprint
        ):
            raise RuntimeError(
                "Silver write used the wrong "
                "input_fingerprint"
            )

        fingerprint_segment = (
            "input_fingerprint="
            f"{lineage.input_fingerprint}"
        )

        if fingerprint_segment not in (
            write_result.output_prefix
        ):
            raise RuntimeError(
                "Silver output prefix has no "
                "input_fingerprint"
            )

        manifest = read_success_manifest(
            bucket=SILVER_BUCKET,
            output_prefix=(
                write_result.output_prefix
            ),
            client=client,
        )

        if (
            manifest.get(
                "input_fingerprint"
            )
            != lineage.input_fingerprint
        ):
            raise RuntimeError(
                "Success manifest has the wrong "
                "input_fingerprint"
            )

        source = manifest.get("source")

        if not isinstance(source, dict):
            raise RuntimeError(
                "Success manifest source "
                "lineage is invalid"
            )

        if (
            source.get(
                "psg_checksum_sha256"
            )
            != lineage.psg_checksum_sha256
        ):
            raise RuntimeError(
                "Manifest PSG checksum mismatch"
            )

        if (
            source.get(
                "hypnogram_checksum_sha256"
            )
            != lineage
            .hypnogram_checksum_sha256
        ):
            raise RuntimeError(
                "Manifest Hypnogram checksum "
                "mismatch"
            )

        if (
            source.get("psg_file_id")
            != str(lineage.psg_file_id)
        ):
            raise RuntimeError(
                "Manifest PSG file ID mismatch"
            )

        if (
            source.get(
                "hypnogram_file_id"
            )
            != str(
                lineage.hypnogram_file_id
            )
        ):
            raise RuntimeError(
                "Manifest Hypnogram file ID "
                "mismatch"
            )

        print(
            "source_pair_id_path_based=true"
        )
        print(
            "input_fingerprint_checksum_based=true"
        )
        print(
            "input_fingerprint_stable=true"
        )
        print(
            "changed_checksum_changes_fingerprint=true"
        )
        print(
            "input_fingerprint_in_output_prefix=true"
        )
        print(
            "source_checksums_in_manifest=true"
        )
        print(
            "source_file_ids_in_manifest=true"
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
            "Input-fingerprint smoke objects "
            "were not cleaned up"
        )

    print(
        "source_lineage_cleanup=true"
    )
    print(
        "silver_source_lineage_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
