from __future__ import annotations

from neuro_sleep.config import (
    get_settings,
)
from neuro_sleep.reliability.errors import (
    ConcurrentPipelineRunError,
)
from neuro_sleep.silver.subject_metadata_job import (
    run_tracked_subject_metadata_job,
)


SILVER_BUCKET = "silver"


def main() -> None:
    settings = get_settings()

    root_prefix = (
        "physionet/sleep-edfx/"
        f"{settings.sleep_edf_version}/"
        "metadata"
    )

    tracked = (
        run_tracked_subject_metadata_job(
            silver_bucket=SILVER_BUCKET,
            root_prefix=root_prefix,
            settings=settings,
        )
    )

    result = tracked.pipeline_result

    print(
        f"pipeline_run_id={tracked.run_id}"
    )
    print(
        "subject_metadata_run_status="
        f"{result.status}"
    )
    print(
        f"output_prefix="
        f"{result.output_prefix}"
    )
    print(
        f"input_fingerprint="
        f"{result.input_fingerprint}"
    )
    print(
        f"subject_count="
        f"{result.subject_count}"
    )
    print(
        "recording_context_count="
        f"{result.recording_context_count}"
    )
    print(
        "data_object_count="
        f"{result.data_object_count}"
    )
    print(
        "total_object_count="
        f"{result.total_object_count}"
    )
    print(
        "recovered_partial_output="
        f"{str(
            result.recovered_partial_output
        ).lower()}"
    )
    print(
        "subject_metadata_job_status="
        "success"
    )


if __name__ == "__main__":
    try:
        main()

    except ConcurrentPipelineRunError:
        raise SystemExit(2) from None
