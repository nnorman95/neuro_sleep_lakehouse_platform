from __future__ import annotations

from neuro_sleep.config import (
    get_settings,
)
from neuro_sleep.ops.pipeline_run import (
    get_pipeline_run_status,
)
from neuro_sleep.silver.subject_metadata_job import (
    run_tracked_subject_metadata_job,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
    list_object_summaries,
)


SILVER_BUCKET = "silver"
SMOKE_ROOT_PREFIX = (
    "smoke-tests/"
    "subject-metadata-pipeline"
)


def delete_prefix(
    *,
    client,
    prefix: str,
) -> None:
    objects = list_object_summaries(
        bucket=SILVER_BUCKET,
        prefix=f"{prefix}/",
        client=client,
    )

    for item in objects:
        client.delete_object(
            Bucket=SILVER_BUCKET,
            Key=item.object_key,
        )


def run_smoke_test() -> None:
    settings = get_settings()
    client = get_object_storage_client(
        settings
    )

    try:
        delete_prefix(
            client=client,
            prefix=SMOKE_ROOT_PREFIX,
        )

        first = (
            run_tracked_subject_metadata_job(
                silver_bucket=(
                    SILVER_BUCKET
                ),
                root_prefix=(
                    SMOKE_ROOT_PREFIX
                ),
                settings=settings,
                client=client,
            )
        )

        if (
            first.pipeline_result.status
            != "written"
        ):
            raise RuntimeError(
                "First metadata run must "
                "write output"
            )

        if (
            first.pipeline_result
            .subject_count
            != 100
        ):
            raise RuntimeError(
                "Expected 100 subjects"
            )

        if (
            first.pipeline_result
            .recording_context_count
            != 197
        ):
            raise RuntimeError(
                "Expected 197 recording "
                "contexts"
            )

        if (
            first.pipeline_result
            .total_object_count
            != 3
        ):
            raise RuntimeError(
                "Expected two Parquet objects "
                "and one success manifest"
            )

        first_run = get_pipeline_run_status(
            first.run_id
        )

        if first_run.status != "success":
            raise RuntimeError(
                "Written metadata run was "
                "not tracked as success"
            )

        second = (
            run_tracked_subject_metadata_job(
                silver_bucket=(
                    SILVER_BUCKET
                ),
                root_prefix=(
                    SMOKE_ROOT_PREFIX
                ),
                settings=settings,
                client=client,
            )
        )

        if (
            second.pipeline_result.status
            != "skipped"
        ):
            raise RuntimeError(
                "Second metadata run must "
                "be skipped"
            )

        second_run = (
            get_pipeline_run_status(
                second.run_id
            )
        )

        if second_run.status != "skipped":
            raise RuntimeError(
                "Idempotent metadata run was "
                "not tracked as skipped"
            )

        stored_objects = (
            list_object_summaries(
                bucket=SILVER_BUCKET,
                prefix=(
                    first.pipeline_result
                    .output_prefix
                    + "/"
                ),
                client=client,
            )
        )

        if len(stored_objects) != 3:
            raise RuntimeError(
                "Unexpected metadata object "
                "count in Silver"
            )

        print(
            "subject_metadata_written=true"
        )
        print(
            "subject_metadata_skipped_on_"
            "rerun=true"
        )
        print("subject_count=100")
        print(
            "recording_context_count=197"
        )
        print(
            "subject_metadata_object_count=3"
        )
        print(
            "subject_metadata_success_"
            "tracked=true"
        )
        print(
            "subject_metadata_skip_"
            "tracked=true"
        )
        print(
            "subject_metadata_pipeline_"
            "smoke_status=success"
        )

    finally:
        delete_prefix(
            client=client,
            prefix=SMOKE_ROOT_PREFIX,
        )
        client.close()

    print(
        "subject_metadata_pipeline_"
        "cleanup=true"
    )


if __name__ == "__main__":
    run_smoke_test()
