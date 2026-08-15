from __future__ import annotations

from dataclasses import replace

from neuro_sleep.silver.quality_checks import (
    check_epochs,
    run_silver_quality_checks,
)
from neuro_sleep.silver.recording_builder import (
    build_silver_recording,
)


CASES = (
    (
        "ST7091J",
        "ST7091J0-PSG.edf",
        "ST7091JE-Hypnogram.edf",
        1,
    ),
    (
        "ST7161J",
        "ST7161J0-PSG.edf",
        "ST7161JM-Hypnogram.edf",
        14,
    ),
)


def build_case(
    psg_file: str,
    hypnogram_file: str,
):
    prefix = (
        "physionet/sleep-edfx/1.0.0/"
        "sleep-telemetry/"
    )

    return build_silver_recording(
        psg_bucket="bronze",
        psg_object_key=prefix + psg_file,
        hypnogram_bucket="bronze",
        hypnogram_object_key=(
            prefix + hypnogram_file
        ),
    )


def run_smoke_test() -> None:
    first_bundle = None

    for (
        recording_key,
        psg_file,
        hypnogram_file,
        expected_first_epoch,
    ) in CASES:
        bundle = build_case(
            psg_file,
            hypnogram_file,
        )

        if first_bundle is None:
            first_bundle = bundle

        actual_first_epoch = (
            bundle.epochs[0].epoch_number
        )

        if actual_first_epoch != (
            expected_first_epoch
        ):
            raise RuntimeError(
                f"{recording_key}: expected "
                f"first epoch "
                f"{expected_first_epoch}, got "
                f"{actual_first_epoch}"
            )

        report = run_silver_quality_checks(
            bundle
        )

        error_codes = {
            issue.code
            for issue in report.issues
            if issue.severity == "error"
        }

        if error_codes:
            raise RuntimeError(
                f"{recording_key}: unexpected "
                f"quality errors: "
                f"{sorted(error_codes)}"
            )

        warning_codes = {
            issue.code
            for issue in report.issues
            if issue.severity == "warning"
        }

        if "UNANNOTATED_PSG_HEAD" not in (
            warning_codes
        ):
            raise RuntimeError(
                f"{recording_key}: expected "
                "UNANNOTATED_PSG_HEAD warning"
            )

        print(
            f"{recording_key}_first_epoch="
            f"{actual_first_epoch}"
        )
        print(
            f"{recording_key}_quality_errors=0"
        )
        print(
            f"{recording_key}_head_warning=true"
        )

    if first_bundle is None:
        raise RuntimeError(
            "No real test bundle was built"
        )

    epochs = first_bundle.epochs

    if len(epochs) < 3:
        raise RuntimeError(
            "Not enough epochs for gap test"
        )

    gapped_bundle = replace(
        first_bundle,
        epochs=(
            epochs[0],
            *epochs[2:],
        ),
    )

    issues = []
    check_epochs(
        bundle=gapped_bundle,
        issues=issues,
    )

    error_codes = {
        issue.code
        for issue in issues
        if issue.severity == "error"
    }

    required_gap_errors = {
        "NON_CONTIGUOUS_EPOCHS",
        "EPOCH_TIMELINE_GAP",
    }

    if not required_gap_errors.issubset(
        error_codes
    ):
        raise RuntimeError(
            "Real internal gap was not "
            "fully blocked: "
            f"{sorted(error_codes)}"
        )

    print(
        "real_epoch_number_gap_blocked=true"
    )
    print(
        "real_epoch_timeline_gap_blocked=true"
    )
    print(
        "real_nonzero_epoch_cases_status="
        "success"
    )


if __name__ == "__main__":
    run_smoke_test()
