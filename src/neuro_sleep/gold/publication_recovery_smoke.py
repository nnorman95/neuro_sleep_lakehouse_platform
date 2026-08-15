from __future__ import annotations

from uuid import uuid4

from neuro_sleep.gold.signal_feature_publication import (
    GOLD_BUCKET,
    InvalidGoldPublicationError,
    build_gold_output_prefix,
    build_success_object_key,
    inspect_publication_state,
    recover_partial_gold_prefix,
)
from neuro_sleep.spark.signal_input import (
    SelectedSignalInput,
)
from neuro_sleep.storage.object_storage import (
    delete_object,
    get_object_storage_client,
    list_object_summaries,
    put_bytes_object,
    put_text_object,
)


def _fake_input(
    *,
    test_id: str,
    suffix: str,
) -> SelectedSignalInput:
    recording_id = (
        f"{test_id}-{suffix}"
    )

    return SelectedSignalInput(
        source_system="smoke",
        dataset_version=(
            f"smoke-{test_id}"
        ),
        collection="gold-reliability",
        recording_key=(
            f"RECOVERY_{suffix.upper()}"
        ),
        recording_id=recording_id,
        bucket="silver",
        output_prefix=(
            "smoke-tests/"
            f"{test_id}/{suffix}/silver"
        ),
        signal_object_keys=(
            f"smoke-tests/{test_id}/"
            f"{suffix}/signal.parquet",
        ),
        signal_file_count=1,
        signal_row_count=1,
        signal_size_bytes=1,
    )


def _cleanup_prefix(
    *,
    prefix: str,
    client,
) -> int:
    objects = list_object_summaries(
        bucket=GOLD_BUCKET,
        prefix=f"{prefix}/",
        client=client,
    )

    for item in objects:
        delete_object(
            bucket=GOLD_BUCKET,
            object_key=item.object_key,
            client=client,
        )

    remaining = list_object_summaries(
        bucket=GOLD_BUCKET,
        prefix=f"{prefix}/",
        client=client,
    )

    if remaining:
        raise RuntimeError(
            "Gold reliability smoke cleanup "
            "left objects behind"
        )

    return len(objects)


def run_smoke_test() -> None:
    test_id = uuid4().hex
    client = get_object_storage_client()

    partial_item = _fake_input(
        test_id=test_id,
        suffix="partial",
    )
    protected_item = _fake_input(
        test_id=test_id,
        suffix="protected",
    )

    partial_prefix = (
        build_gold_output_prefix(
            partial_item
        )
    )
    protected_prefix = (
        build_gold_output_prefix(
            protected_item
        )
    )

    cleanup_count = 0

    try:
        partial_object_key = (
            f"{partial_prefix}/data/"
            "orphan.parquet"
        )

        put_bytes_object(
            bucket=GOLD_BUCKET,
            object_key=partial_object_key,
            data=b"incomplete",
            client=client,
        )

        state = inspect_publication_state(
            item=partial_item,
            expected_row_count=1,
            expected_partial_window_count=0,
            client=client,
        )

        if state != ("write", 1):
            raise RuntimeError(
                "Incomplete Gold prefix did "
                "not recover as expected: "
                f"{state!r}"
            )

        remaining_partial = (
            list_object_summaries(
                bucket=GOLD_BUCKET,
                prefix=(
                    f"{partial_prefix}/"
                ),
                client=client,
            )
        )
        if remaining_partial:
            raise RuntimeError(
                "Recovered Gold partial prefix "
                "is not empty"
            )

        print(
            "gold_partial_prefix_recovery="
            "success"
        )
        print(
            "gold_partial_recovered_objects=1"
        )

        protected_data_key = (
            f"{protected_prefix}/data/"
            "part-smoke.parquet"
        )
        protected_success_key = (
            build_success_object_key(
                protected_prefix
            )
        )

        put_bytes_object(
            bucket=GOLD_BUCKET,
            object_key=protected_data_key,
            data=b"completed-data",
            client=client,
        )
        put_text_object(
            bucket=GOLD_BUCKET,
            object_key=(
                protected_success_key
            ),
            text="{}",
            client=client,
        )

        try:
            recover_partial_gold_prefix(
                output_prefix=(
                    protected_prefix
                ),
                client=client,
            )
        except InvalidGoldPublicationError:
            pass
        else:
            raise RuntimeError(
                "Recovery deleted or accepted "
                "a prefix protected by "
                "_SUCCESS.json"
            )

        protected_after_recovery = (
            list_object_summaries(
                bucket=GOLD_BUCKET,
                prefix=(
                    f"{protected_prefix}/"
                ),
                client=client,
            )
        )
        if len(
            protected_after_recovery
        ) != 2:
            raise RuntimeError(
                "Protected completed prefix "
                "was modified by recovery"
            )

        print(
            "gold_completed_prefix_"
            "recovery_blocked=success"
        )

        try:
            inspect_publication_state(
                item=protected_item,
                expected_row_count=1,
                expected_partial_window_count=0,
                client=client,
            )
        except InvalidGoldPublicationError:
            pass
        else:
            raise RuntimeError(
                "Invalid completed Gold "
                "publication did not fail closed"
            )

        protected_after_validation = (
            list_object_summaries(
                bucket=GOLD_BUCKET,
                prefix=(
                    f"{protected_prefix}/"
                ),
                client=client,
            )
        )
        if len(
            protected_after_validation
        ) != 2:
            raise RuntimeError(
                "Fail-closed validation "
                "modified protected objects"
            )

        print(
            "gold_invalid_completed_"
            "publication_fail_closed=success"
        )

    finally:
        cleanup_count += _cleanup_prefix(
            prefix=partial_prefix,
            client=client,
        )
        cleanup_count += _cleanup_prefix(
            prefix=protected_prefix,
            client=client,
        )
        client.close()

    print(
        "gold_reliability_smoke_"
        f"cleanup_objects={cleanup_count}"
    )
    print(
        "gold_reliability_smoke_"
        "status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
