from __future__ import annotations

from neuro_sleep.reliability.errors import (
    ConcurrentPipelineRunError,
)
from neuro_sleep.silver.silver_job import (
    run_tracked_silver_job,
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

SILVER_ROOT_PREFIX = (
    "physionet/sleep-edfx/1.0.0/"
    "sleep-cassette/SC4001E0"
)


def main() -> None:
    tracked_result = run_tracked_silver_job(
        psg_bucket=BRONZE_BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BRONZE_BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
        silver_bucket=SILVER_BUCKET,
        root_prefix=SILVER_ROOT_PREFIX,
        signal_chunk_duration_seconds=(
            1800.0
        ),
        signal_start_seconds=0.0,
        signal_stop_seconds=None,
        verify_payload_checksums=True,
    )

    result = tracked_result.pipeline_result
    report = result.reconciliation_report

    print(
        f"pipeline_run_id="
        f"{tracked_result.run_id}"
    )
    print(
        f"full_silver_run_status="
        f"{result.status}"
    )
    print(
        f"recording_id="
        f"{result.recording_id}"
    )
    print(
        f"output_prefix="
        f"{result.output_prefix}"
    )
    print(
        f"silver_data_object_count="
        f"{result.data_object_count}"
    )
    print(
        f"silver_total_object_count="
        f"{result.total_object_count}"
    )
    print(
        f"silver_total_row_count="
        f"{result.row_count}"
    )
    print(
        f"reconciliation_passed="
        f"{str(report.passed).lower()}"
    )
    print(
        f"reconciliation_error_count="
        f"{report.error_count}"
    )
    print(
        "verified_payload_checksum_count="
        f"{report.verified_payload_checksum_count}"
    )
    print(
        "full_silver_recording_run_status="
        "success"
    )


if __name__ == "__main__":
    try:
        main()

    except ConcurrentPipelineRunError:
        raise SystemExit(2) from None
