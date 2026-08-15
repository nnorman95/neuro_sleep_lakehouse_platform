from __future__ import annotations

from uuid import UUID

from neuro_sleep.quality.quarantine import (
    resolve_active_quarantine_record,
    upsert_active_quarantine_record,
)
from neuro_sleep.silver.quality_checks import (
    SilverQualityReport,
)
from neuro_sleep.silver.recording_builder import (
    SilverRecordingBundle,
)


SILVER_QUALITY_ERROR_CODE = (
    "SILVER_QUALITY_GATE_FAILED"
)


def build_silver_quarantine_record_key(
    *,
    silver_bucket: str,
    output_prefix: str,
) -> str:
    cleaned_bucket = silver_bucket.strip().strip("/")
    cleaned_prefix = output_prefix.strip().strip("/")

    if not cleaned_bucket:
        raise ValueError(
            "silver_bucket cannot be empty"
        )

    if not cleaned_prefix:
        raise ValueError(
            "output_prefix cannot be empty"
        )

    return f"{cleaned_bucket}/{cleaned_prefix}"


def route_failed_silver_quality_report(
    *,
    pipeline_run_id: UUID,
    source_system: str,
    bundle: SilverRecordingBundle,
    report: SilverQualityReport,
    silver_bucket: str,
    output_prefix: str,
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
) -> UUID:
    if report.error_count <= 0:
        raise ValueError(
            "Only failed Silver quality "
            "reports can be quarantined"
        )

    error_codes = sorted(
        {
            issue.code
            for issue in report.issues
            if issue.severity == "error"
        }
    )

    record_key = build_silver_quarantine_record_key(
        silver_bucket=silver_bucket,
        output_prefix=output_prefix,
    )

    raw_payload = {
        "recording_id": str(bundle.recording_id),
        "silver_bucket": silver_bucket,
        "silver_output_prefix": output_prefix,
        "psg": {
            "bucket": psg_bucket,
            "object_key": psg_object_key,
        },
        "hypnogram": {
            "bucket": hypnogram_bucket,
            "object_key": hypnogram_object_key,
        },
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "issues": [
            {
                "code": issue.code,
                "severity": issue.severity,
                "message": issue.message,
            }
            for issue in report.issues
        ],
    }

    return upsert_active_quarantine_record(
        source_system=source_system,
        record_key=record_key,
        error_code=SILVER_QUALITY_ERROR_CODE,
        error_message=(
            "Silver quality gate failed: "
            + ", ".join(error_codes)
        ),
        severity="error",
        raw_payload=raw_payload,
        pipeline_run_id=pipeline_run_id,
    )


def resolve_silver_quality_quarantine(
    *,
    source_system: str,
    silver_bucket: str,
    output_prefix: str,
) -> int:
    record_key = build_silver_quarantine_record_key(
        silver_bucket=silver_bucket,
        output_prefix=output_prefix,
    )

    return resolve_active_quarantine_record(
        source_system=source_system,
        record_key=record_key,
        error_code=SILVER_QUALITY_ERROR_CODE,
    )
