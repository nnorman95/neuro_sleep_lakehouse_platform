from __future__ import annotations

from uuid import uuid4

from neuro_sleep.gold.integrated_feature_publication import (
    GOLD_BUCKET,
    InvalidIntegratedGoldPublicationError,
    SourceGoldLineage,
    build_integrated_output_prefix,
    build_success_object_key,
    inspect_publication_state,
    recover_partial_integrated_prefix,
)
from neuro_sleep.spark.signal_input import SelectedSignalInput
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
    return SelectedSignalInput(
        source_system="smoke",
        dataset_version=f"smoke-{test_id}",
        collection="integrated-gold-reliability",
        recording_key=f"INTEGRATED_{suffix.upper()}",
        recording_id=f"{test_id}-{suffix}",
        bucket="silver",
        output_prefix=(
            f"smoke-tests/{test_id}/{suffix}/silver"
        ),
        signal_object_keys=(
            f"smoke-tests/{test_id}/{suffix}/signal.parquet",
        ),
        signal_file_count=1,
        signal_row_count=1,
        signal_size_bytes=1,
    )


def _fake_source_gold(
    *,
    test_id: str,
    suffix: str,
) -> SourceGoldLineage:
    prefix = (
        f"smoke-tests/{test_id}/{suffix}/"
        "source-gold"
    )
    return SourceGoldLineage(
        output_prefix=prefix,
        success_object_key=f"{prefix}/_SUCCESS.json",
        success_etag='"source-success-etag"',
        data_object_key=f"{prefix}/data/part-smoke.parquet",
        data_object_etag='"source-data-etag"',
        row_count=1,
        partial_window_count=0,
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
            "Integrated Gold reliability smoke cleanup "
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

    partial_fingerprint = "a" * 64
    protected_fingerprint = "b" * 64

    partial_prefix = build_integrated_output_prefix(
        item=partial_item,
        warehouse_context_sha256=partial_fingerprint,
    )
    protected_prefix = build_integrated_output_prefix(
        item=protected_item,
        warehouse_context_sha256=protected_fingerprint,
    )

    partial_source = _fake_source_gold(
        test_id=test_id,
        suffix="partial",
    )
    protected_source = _fake_source_gold(
        test_id=test_id,
        suffix="protected",
    )

    cleanup_count = 0

    try:
        partial_object_key = (
            f"{partial_prefix}/data/orphan.parquet"
        )
        put_bytes_object(
            bucket=GOLD_BUCKET,
            object_key=partial_object_key,
            data=b"incomplete-integrated-gold",
            client=client,
        )

        state = inspect_publication_state(
            item=partial_item,
            warehouse_context_sha256=partial_fingerprint,
            recording_context_count=1,
            epoch_context_count=1,
            expected_row_count=1,
            expected_labeled_row_count=1,
            expected_unlabeled_row_count=0,
            expected_partial_window_count=0,
            source_gold=partial_source,
            client=client,
        )
        if state != ("write", 1):
            raise RuntimeError(
                "Incomplete integrated Gold prefix did not "
                f"recover as expected: {state!r}"
            )

        remaining_partial = list_object_summaries(
            bucket=GOLD_BUCKET,
            prefix=f"{partial_prefix}/",
            client=client,
        )
        if remaining_partial:
            raise RuntimeError(
                "Recovered integrated Gold partial prefix "
                "is not empty"
            )

        print(
            "integrated_gold_partial_prefix_recovery=success"
        )
        print(
            "integrated_gold_partial_recovered_objects=1"
        )

        protected_data_key = (
            f"{protected_prefix}/data/part-smoke.parquet"
        )
        protected_success_key = build_success_object_key(
            protected_prefix
        )

        put_bytes_object(
            bucket=GOLD_BUCKET,
            object_key=protected_data_key,
            data=b"completed-integrated-data",
            client=client,
        )
        put_text_object(
            bucket=GOLD_BUCKET,
            object_key=protected_success_key,
            text="{}",
            client=client,
        )

        try:
            recover_partial_integrated_prefix(
                output_prefix=protected_prefix,
                client=client,
            )
        except InvalidIntegratedGoldPublicationError:
            pass
        else:
            raise RuntimeError(
                "Integrated Gold recovery deleted or accepted "
                "a prefix protected by _SUCCESS.json"
            )

        protected_after_recovery = list_object_summaries(
            bucket=GOLD_BUCKET,
            prefix=f"{protected_prefix}/",
            client=client,
        )
        if len(protected_after_recovery) != 2:
            raise RuntimeError(
                "Protected integrated Gold prefix was modified "
                "by recovery"
            )

        print(
            "integrated_gold_completed_prefix_"
            "recovery_blocked=success"
        )

        try:
            inspect_publication_state(
                item=protected_item,
                warehouse_context_sha256=protected_fingerprint,
                recording_context_count=1,
                epoch_context_count=1,
                expected_row_count=1,
                expected_labeled_row_count=1,
                expected_unlabeled_row_count=0,
                expected_partial_window_count=0,
                source_gold=protected_source,
                client=client,
            )
        except InvalidIntegratedGoldPublicationError:
            pass
        else:
            raise RuntimeError(
                "Invalid completed integrated Gold publication "
                "did not fail closed"
            )

        protected_after_validation = list_object_summaries(
            bucket=GOLD_BUCKET,
            prefix=f"{protected_prefix}/",
            client=client,
        )
        if len(protected_after_validation) != 2:
            raise RuntimeError(
                "Fail-closed integrated Gold validation "
                "modified protected objects"
            )

        print(
            "integrated_gold_invalid_completed_"
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
        "integrated_gold_reliability_smoke_"
        f"cleanup_objects={cleanup_count}"
    )
    print(
        "integrated_gold_reliability_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
