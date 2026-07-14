from collections.abc import Callable
from dataclasses import replace

from neuro_sleep.silver.quality_checks import (
    run_silver_quality_checks,
)
from neuro_sleep.silver.recording_builder import (
    build_silver_recording,
)


BUCKET = "bronze"

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


def expect_value_error(
    operation: Callable[[], object],
    check_name: str,
) -> None:
    try:
        operation()

    except ValueError:
        print(f"{check_name}=true")
        return

    raise RuntimeError(
        f"Expected ValueError: {check_name}"
    )


def run_smoke_test() -> None:
    bundle = build_silver_recording(
        psg_bucket=BUCKET,
        psg_object_key=PSG_OBJECT_KEY,
        hypnogram_bucket=BUCKET,
        hypnogram_object_key=(
            HYPNOGRAM_OBJECT_KEY
        ),
    )

    report = run_silver_quality_checks(
        bundle
    )

    if not report.passed:
        raise RuntimeError(
            "Valid recording failed Silver "
            "quality checks"
        )

    if report.error_count != 0:
        raise RuntimeError(
            "Unexpected quality errors"
        )

    expected_warning_codes = {
        "TRAILING_HYPNOGRAM_OVERHANG",
        "MISSING_CHANNEL_UNITS",
        "SPECIAL_SLEEP_STAGE_LABELS",
    }

    actual_warning_codes = {
        issue.code
        for issue in report.issues
        if issue.severity == "warning"
    }

    if (
        actual_warning_codes
        != expected_warning_codes
    ):
        raise RuntimeError(
            "Unexpected quality warnings: "
            f"{actual_warning_codes}"
        )

    if report.warning_count != 3:
        raise RuntimeError(
            "Unexpected warning count"
        )

    print("silver_quality_passed=true")
    print("silver_quality_error_count=0")
    print("silver_quality_warning_count=3")
    print(
        "trailing_overhang_warning=true"
    )
    print(
        "missing_channel_units_warning=true"
    )
    print(
        "special_stage_labels_warning=true"
    )

    bad_recording = replace(
        bundle.recording,
        channel_count=99,
    )

    bad_count_bundle = replace(
        bundle,
        recording=bad_recording,
    )

    bad_count_report = (
        run_silver_quality_checks(
            bad_count_bundle
        )
    )

    if bad_count_report.passed:
        raise RuntimeError(
            "Channel count mismatch was "
            "not detected"
        )

    if not any(
        issue.code
        == "CHANNEL_COUNT_MISMATCH"
        for issue in bad_count_report.issues
    ):
        raise RuntimeError(
            "Expected channel count issue "
            "was not produced"
        )

    print(
        "channel_count_mismatch_detected=true"
    )

    duplicate_epoch = replace(
        bundle.epochs[1],
        epoch_number=0,
    )

    duplicate_epoch_bundle = replace(
        bundle,
        epochs=(
            bundle.epochs[0],
            duplicate_epoch,
            *bundle.epochs[2:],
        ),
    )

    duplicate_epoch_report = (
        run_silver_quality_checks(
            duplicate_epoch_bundle
        )
    )

    if not any(
        issue.code
        == "DUPLICATE_EPOCH_NUMBER"
        for issue
        in duplicate_epoch_report.issues
    ):
        raise RuntimeError(
            "Duplicate epoch number was "
            "not detected"
        )

    print(
        "duplicate_epoch_number_detected=true"
    )

    expect_value_error(
        operation=(
            bad_count_report
            .raise_for_errors
        ),
        check_name=(
            "quality_errors_block_pipeline"
        ),
    )

    report.raise_for_errors()

    print(
        "silver_quality_checks_smoke_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
