from __future__ import annotations

from uuid import UUID

from neuro_sleep.quality.check_results import (
    NewQualityCheckResult,
    create_quality_check_result,
    create_quality_check_results,
)
from neuro_sleep.silver.quality_checks import (
    SilverQualityReport,
)
from neuro_sleep.silver.recording_builder import (
    SilverRecordingBundle,
)


DATA_LAYER = "silver"
DATASET_NAME = "silver_recording"
SUMMARY_CHECK_NAME = "silver_quality_gate"


def _summary_status(
    report: SilverQualityReport,
) -> str:
    if report.error_count > 0:
        return "failed"

    if report.warning_count > 0:
        return "warning"

    return "passed"


def _summary_severity(
    report: SilverQualityReport,
) -> str:
    if report.error_count > 0:
        return "error"

    if report.warning_count > 0:
        return "warning"

    return "info"


def build_silver_quality_results(
    *,
    pipeline_run_id: UUID,
    source_system: str,
    bundle: SilverRecordingBundle,
    report: SilverQualityReport,
    output_prefix: str,
) -> tuple[NewQualityCheckResult, ...]:
    summary = NewQualityCheckResult(
        pipeline_run_id=pipeline_run_id,
        source_system=source_system,
        data_layer=DATA_LAYER,
        dataset_name=DATASET_NAME,
        recording_id=(
            bundle.recording_id
        ),
        record_key=output_prefix,
        check_name=SUMMARY_CHECK_NAME,
        severity=_summary_severity(
            report
        ),
        status=_summary_status(report),
        rows_checked=1,
        rows_failed=(
            1 if report.error_count > 0 else 0
        ),
        message=(
            "Silver quality gate completed."
        ),
        details={
            "error_count": report.error_count,
            "warning_count": (
                report.warning_count
            ),
            "issue_count": len(report.issues),
            "channel_count": len(
                bundle.channels
            ),
            "interval_count": len(
                bundle.intervals
            ),
            "epoch_count": len(
                bundle.epochs
            ),
        },
    )

    issues = tuple(
        NewQualityCheckResult(
            pipeline_run_id=pipeline_run_id,
            source_system=source_system,
            data_layer=DATA_LAYER,
            dataset_name=DATASET_NAME,
            recording_id=(
                bundle.recording_id
            ),
            record_key=output_prefix,
            check_name=issue.code.lower(),
            severity=issue.severity,
            status=(
                "failed"
                if issue.severity == "error"
                else "warning"
            ),
            rows_checked=1,
            rows_failed=1,
            error_code=issue.code,
            message=issue.message,
            details={
                "quality_gate": (
                    SUMMARY_CHECK_NAME
                ),
            },
        )
        for issue in report.issues
    )

    return (summary, *issues)


def persist_silver_quality_report(
    *,
    pipeline_run_id: UUID,
    source_system: str,
    bundle: SilverRecordingBundle,
    report: SilverQualityReport,
    output_prefix: str,
) -> tuple[UUID, ...]:
    results = build_silver_quality_results(
        pipeline_run_id=pipeline_run_id,
        source_system=source_system,
        bundle=bundle,
        report=report,
        output_prefix=output_prefix,
    )

    return create_quality_check_results(
        results
    )


def persist_skipped_silver_quality_result(
    *,
    pipeline_run_id: UUID,
    source_system: str,
    recording_id: UUID,
    output_prefix: str,
    reconciliation_passed: bool,
    data_object_count: int,
    total_object_count: int,
) -> UUID:
    return create_quality_check_result(
        pipeline_run_id=pipeline_run_id,
        source_system=source_system,
        data_layer=DATA_LAYER,
        dataset_name=DATASET_NAME,
        recording_id=recording_id,
        record_key=output_prefix,
        check_name=SUMMARY_CHECK_NAME,
        severity="info",
        status="skipped",
        rows_checked=0,
        rows_failed=0,
        message=(
            "Silver quality checks were not "
            "rerun because a completed output "
            "was reused and reconciliation "
            "passed."
        ),
        details={
            "reconciliation_passed": (
                reconciliation_passed
            ),
            "data_object_count": (
                data_object_count
            ),
            "total_object_count": (
                total_object_count
            ),
        },
    )
