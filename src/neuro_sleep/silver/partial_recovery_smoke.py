from neuro_sleep.identifiers import (
    new_uuid7,
)
from neuro_sleep.silver.idempotency import (
    PartialSilverOutputError,
    build_config_id,
    build_idempotent_output_prefix,
    build_source_pair_id,
)
from neuro_sleep.silver.silver_pipeline import (
    run_silver_pipeline,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
    put_text_object,
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


def build_smoke_output_prefix(
    root_prefix: str,
) -> str:
    source_pair_id = build_source_pair_id(
        psg_bucket=BRONZE_BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BRONZE_BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    )

    config_id = build_config_id(
        signal_chunk_duration_seconds=30.0,
        signal_start_seconds=0.0,
        signal_stop_seconds=60.0,
    )

    return build_idempotent_output_prefix(
        root_prefix=root_prefix,
        source_pair_id=source_pair_id,
        config_id=config_id,
    )


def run_smoke_test() -> None:
    smoke_run_id = new_uuid7()

    root_prefix = (
        "smoke-tests/"
        "silver-partial-recovery/"
        f"run_id={smoke_run_id}"
    )

    output_prefix = build_smoke_output_prefix(
        root_prefix=root_prefix
    )

    orphan_object_key = (
        f"{output_prefix}/"
        "orphaned-write/"
        "part-00000.parquet"
    )

    unexpected_object_key = (
        f"{output_prefix}/"
        "unexpected-object.txt"
    )

    client = get_object_storage_client()

    remaining_objects = []

    try:
        put_text_object(
            bucket=SILVER_BUCKET,
            object_key=orphan_object_key,
            text=(
                "Simulated object left by "
                "a hard process crash."
            ),
            client=client,
        )

        partial_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    output_prefix + "/"
                ),
                client=client,
            )
        )

        if len(partial_objects) != 1:
            raise RuntimeError(
                "Partial-output fixture was "
                "not created correctly"
            )

        recovered_result = (
            run_silver_pipeline(
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
                verify_payload_checksums=True,
                client=client,
            )
        )

        if recovered_result.status != "written":
            raise RuntimeError(
                "Recovered Silver run was "
                "not written"
            )

        if not (
            recovered_result
            .recovered_partial_output
        ):
            raise RuntimeError(
                "Partial-output recovery was "
                "not reported"
            )

        if (
            recovered_result
            .recovered_object_count
            != 1
        ):
            raise RuntimeError(
                "Recovered object count is "
                "incorrect"
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

        stored_keys = {
            item.object_key
            for item in stored_objects
        }

        if orphan_object_key in stored_keys:
            raise RuntimeError(
                "Orphan object survived "
                "recovery"
            )

        if len(stored_objects) != 19:
            raise RuntimeError(
                "Recovered Silver output has "
                "an unexpected object count"
            )

        second_result = run_silver_pipeline(
            psg_bucket=BRONZE_BUCKET,
            psg_object_key=PSG_OBJECT_KEY,
            hypnogram_bucket=BRONZE_BUCKET,
            hypnogram_object_key=(
                HYPNOGRAM_OBJECT_KEY
            ),
            silver_bucket=SILVER_BUCKET,
            root_prefix=root_prefix,
            signal_chunk_duration_seconds=30.0,
            signal_start_seconds=0.0,
            signal_stop_seconds=60.0,
            verify_payload_checksums=True,
            client=client,
        )

        if second_result.status != "skipped":
            raise RuntimeError(
                "Completed recovered output "
                "was not skipped"
            )

        if (
            second_result
            .recovered_partial_output
        ):
            raise RuntimeError(
                "Skipped run incorrectly "
                "reported recovery"
            )

        put_text_object(
            bucket=SILVER_BUCKET,
            object_key=unexpected_object_key,
            text=(
                "Unexpected object beside a "
                "valid success manifest."
            ),
            client=client,
        )

        try:
            run_silver_pipeline(
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
                verify_payload_checksums=True,
                client=client,
            )

        except PartialSilverOutputError:
            pass

        else:
            raise RuntimeError(
                "Completed-prefix corruption "
                "was not blocked"
            )

        protected_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    output_prefix + "/"
                ),
                client=client,
            )
        )

        protected_keys = {
            item.object_key
            for item in protected_objects
        }

        success_object_key = (
            f"{output_prefix}/"
            "_SUCCESS.json"
        )

        if (
            success_object_key
            not in protected_keys
        ):
            raise RuntimeError(
                "Recovery deleted a completed "
                "success manifest"
            )

        if (
            unexpected_object_key
            not in protected_keys
        ):
            raise RuntimeError(
                "Completed-prefix corruption "
                "was automatically deleted"
            )

        if len(protected_objects) != 20:
            raise RuntimeError(
                "Completed-prefix protection "
                "changed the object set"
            )

        print(
            "partial_prefix_detected=true"
        )
        print(
            "partial_prefix_cleaned=true"
        )
        print(
            "partial_prefix_rebuilt=true"
        )
        print(
            "recovered_object_count=1"
        )
        print(
            "recovered_output_reconciled=true"
        )
        print(
            "second_recovered_run_skipped=true"
        )
        print(
            "completed_prefix_auto_delete=false"
        )
        print(
            "success_manifest_preserved=true"
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
            "Partial-recovery smoke objects "
            "were not cleaned up"
        )

    print(
        "partial_recovery_cleanup=true"
    )
    print(
        "silver_partial_recovery_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
