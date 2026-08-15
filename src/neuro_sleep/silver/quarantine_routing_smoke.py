from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

import neuro_sleep.silver.silver_job as silver_job
from neuro_sleep.identifiers import new_uuid7
from neuro_sleep.ops.pipeline_run import (
    finish_pipeline_run_failed,
    finish_pipeline_run_success,
    start_pipeline_run,
)
from neuro_sleep.quality.quarantine import (
    delete_quarantine_record_for_smoke_test,
    get_quarantine_record,
)
from neuro_sleep.silver.quality_checks import (
    QualityIssue,
    SilverQualityError,
    SilverQualityReport,
)
from neuro_sleep.silver.quarantine_routing import (
    SILVER_QUALITY_ERROR_CODE,
    build_silver_quarantine_record_key,
    resolve_silver_quality_quarantine,
    route_failed_silver_quality_report,
)
from neuro_sleep.sources.sleep_edf import (
    SOURCE_SYSTEM,
)


PIPELINE_NAME = "silver_quarantine_routing_smoke_test"
TASK_NAME = "route_refresh_resolve_quality_failure"
SILVER_BUCKET = "silver"
OUTPUT_PREFIX = (
    "smoke-tests/silver-quarantine/"
    "schema_version=1.0.0/"
    "transform_version=smoke/"
    "source_pair_id=smoke/"
    "input_fingerprint=smoke/"
    "config_id=smoke"
)
PSG_BUCKET = "bronze"
PSG_OBJECT_KEY = "smoke-tests/source/test-PSG.edf"
HYPNOGRAM_BUCKET = "bronze"
HYPNOGRAM_OBJECT_KEY = (
    "smoke-tests/source/test-Hypnogram.edf"
)


class FakeLock:
    def __init__(self, pipeline_name, settings=None):
        self.pipeline_name = pipeline_name
        self.settings = settings

    def acquire(self):
        return None

    def release(self):
        return None


class FakeHeartbeat:
    def __init__(
        self,
        run_id,
        pipeline_name,
        interval_seconds,
    ):
        self.run_id = run_id

    def start(self):
        return None

    def stop(self):
        return None


def build_failed_report() -> SilverQualityReport:
    return SilverQualityReport(
        issues=(
            QualityIssue(
                code="SMOKE_INVALID_RECORD",
                severity="error",
                message=(
                    "Simulated data-quality failure."
                ),
            ),
            QualityIssue(
                code="SMOKE_WARNING",
                severity="warning",
                message="Simulated warning.",
            ),
        )
    )


def run_storage_lifecycle_case() -> None:
    record_key = build_silver_quarantine_record_key(
        silver_bucket=SILVER_BUCKET,
        output_prefix=OUTPUT_PREFIX,
    )

    delete_quarantine_record_for_smoke_test(
        source_system=SOURCE_SYSTEM,
        record_key=record_key,
        error_code=SILVER_QUALITY_ERROR_CODE,
    )

    run_id = start_pipeline_run(
        pipeline_name=PIPELINE_NAME,
        task_name=TASK_NAME,
        source_system=SOURCE_SYSTEM,
    )

    try:
        bundle = SimpleNamespace(
            recording_id=new_uuid7(),
        )
        report = build_failed_report()

        kwargs = dict(
            pipeline_run_id=run_id,
            source_system=SOURCE_SYSTEM,
            bundle=bundle,
            report=report,
            silver_bucket=SILVER_BUCKET,
            output_prefix=OUTPUT_PREFIX,
            psg_bucket=PSG_BUCKET,
            psg_object_key=PSG_OBJECT_KEY,
            hypnogram_bucket=HYPNOGRAM_BUCKET,
            hypnogram_object_key=(
                HYPNOGRAM_OBJECT_KEY
            ),
        )

        first_id = route_failed_silver_quality_report(
            **kwargs
        )
        second_id = route_failed_silver_quality_report(
            **kwargs
        )

        if first_id != second_id:
            raise RuntimeError(
                "Repeated failure created "
                "a duplicate active incident"
            )

        active_row = get_quarantine_record(first_id)

        if active_row[9] != "open":
            raise RuntimeError(
                "Active quarantine status "
                "is not open"
            )

        payload = active_row[4]

        if payload["error_count"] != 1:
            raise RuntimeError(
                "Unexpected quarantine error_count"
            )

        if (
            payload["psg"]["object_key"]
            != PSG_OBJECT_KEY
        ):
            raise RuntimeError(
                "PSG lineage was not preserved"
            )

        resolved_count = (
            resolve_silver_quality_quarantine(
                source_system=SOURCE_SYSTEM,
                silver_bucket=SILVER_BUCKET,
                output_prefix=OUTPUT_PREFIX,
            )
        )

        if resolved_count != 1:
            raise RuntimeError(
                "Expected one active incident "
                "to resolve"
            )

        resolved_row = get_quarantine_record(
            first_id
        )

        if resolved_row[9] != "resolved":
            raise RuntimeError(
                "Quarantine incident was "
                "not resolved"
            )

        third_id = route_failed_silver_quality_report(
            **kwargs
        )

        if third_id == first_id:
            raise RuntimeError(
                "Resolved history row was "
                "incorrectly reopened"
            )

        fourth_id = (
            route_failed_silver_quality_report(
                **kwargs
            )
        )

        if fourth_id != third_id:
            raise RuntimeError(
                "New active incident is "
                "not idempotent"
            )

        print(
            "active_quarantine_upsert_"
            "idempotent=true"
        )
        print(
            "silver_quality_lineage_"
            "preserved=true"
        )
        print(
            "active_quarantine_resolved=true"
        )
        print(
            "resolved_history_preserved=true"
        )
        print(
            "new_incident_after_resolution="
            "true"
        )

        delete_quarantine_record_for_smoke_test(
            source_system=SOURCE_SYSTEM,
            record_key=record_key,
            error_code=SILVER_QUALITY_ERROR_CODE,
        )

        finish_pipeline_run_success(
            run_id=run_id,
            rows_read=1,
            rows_written=0,
            files_processed=0,
            records_quarantined=1,
        )

    except Exception as exc:
        delete_quarantine_record_for_smoke_test(
            source_system=SOURCE_SYSTEM,
            record_key=record_key,
            error_code=SILVER_QUALITY_ERROR_CODE,
        )

        finish_pipeline_run_failed(
            run_id=run_id,
            error_message=str(exc),
            rows_read=1,
            rows_written=0,
            files_processed=0,
            records_quarantined=1,
        )
        raise


def run_job_wiring_case() -> None:
    run_id = new_uuid7()
    recording_id = new_uuid7()
    report = build_failed_report()
    route_mock = Mock(return_value=new_uuid7())
    finish_failed = Mock()

    def fake_run_silver_pipeline(**kwargs):
        handler = kwargs["quality_report_handler"]
        handler(
            SimpleNamespace(
                recording_id=recording_id,
            ),
            report,
            OUTPUT_PREFIX,
        )
        raise SilverQualityError(report)

    with (
        patch.object(
            silver_job,
            "get_settings",
            return_value=SimpleNamespace(),
        ),
        patch.object(
            silver_job,
            "PipelineRunLock",
            FakeLock,
        ),
        patch.object(
            silver_job,
            "PipelineHeartbeat",
            FakeHeartbeat,
        ),
        patch.object(
            silver_job,
            "start_pipeline_run",
            return_value=run_id,
        ),
        patch.object(
            silver_job,
            "run_silver_pipeline",
            side_effect=fake_run_silver_pipeline,
        ),
        patch.object(
            silver_job,
            "persist_silver_quality_report",
        ),
        patch.object(
            silver_job,
            "route_failed_silver_quality_report",
            route_mock,
        ),
        patch.object(
            silver_job,
            "finish_pipeline_run_failed",
            finish_failed,
        ),
        patch.object(
            silver_job,
            "emit_event",
        ),
        patch.object(
            silver_job,
            "emit_exception",
        ),
    ):
        try:
            silver_job.run_tracked_silver_job(
                psg_bucket=PSG_BUCKET,
                psg_object_key=PSG_OBJECT_KEY,
                hypnogram_bucket=(
                    HYPNOGRAM_BUCKET
                ),
                hypnogram_object_key=(
                    HYPNOGRAM_OBJECT_KEY
                ),
                silver_bucket=SILVER_BUCKET,
                root_prefix="smoke-tests/root",
            )
        except SilverQualityError:
            pass
        else:
            raise RuntimeError(
                "Quality failure was not propagated"
            )

    if route_mock.call_count != 1:
        raise RuntimeError(
            "Silver quality failure was not "
            "routed exactly once"
        )

    finish_failed.assert_called_once_with(
        run_id=run_id,
        error_message=str(SilverQualityError(report)),
        rows_read=0,
        rows_written=0,
        files_processed=0,
        records_quarantined=1,
    )

    print(
        "silver_job_quality_failure_routed=true"
    )
    print(
        "silver_job_records_quarantined=1"
    )


def run_runtime_failure_case() -> None:
    run_id = new_uuid7()
    runtime_error = RuntimeError(
        "Simulated runtime failure"
    )
    route_mock = Mock()
    finish_failed = Mock()

    with (
        patch.object(
            silver_job,
            "get_settings",
            return_value=SimpleNamespace(),
        ),
        patch.object(
            silver_job,
            "PipelineRunLock",
            FakeLock,
        ),
        patch.object(
            silver_job,
            "PipelineHeartbeat",
            FakeHeartbeat,
        ),
        patch.object(
            silver_job,
            "start_pipeline_run",
            return_value=run_id,
        ),
        patch.object(
            silver_job,
            "run_silver_pipeline",
            side_effect=runtime_error,
        ),
        patch.object(
            silver_job,
            "route_failed_silver_quality_report",
            route_mock,
        ),
        patch.object(
            silver_job,
            "finish_pipeline_run_failed",
            finish_failed,
        ),
        patch.object(
            silver_job,
            "emit_event",
        ),
        patch.object(
            silver_job,
            "emit_exception",
        ),
    ):
        try:
            silver_job.run_tracked_silver_job(
                psg_bucket=PSG_BUCKET,
                psg_object_key=PSG_OBJECT_KEY,
                hypnogram_bucket=(
                    HYPNOGRAM_BUCKET
                ),
                hypnogram_object_key=(
                    HYPNOGRAM_OBJECT_KEY
                ),
                silver_bucket=SILVER_BUCKET,
                root_prefix="smoke-tests/root",
            )
        except RuntimeError as exc:
            if exc is not runtime_error:
                raise
        else:
            raise RuntimeError(
                "Runtime failure was not propagated"
            )

    if route_mock.call_count != 0:
        raise RuntimeError(
            "Runtime failure was incorrectly "
            "quarantined"
        )

    finish_failed.assert_called_once_with(
        run_id=run_id,
        error_message=str(runtime_error),
        rows_read=0,
        rows_written=0,
        files_processed=0,
        records_quarantined=0,
    )

    print(
        "runtime_failure_not_quarantined=true"
    )


def run_smoke_test() -> None:
    run_storage_lifecycle_case()
    run_job_wiring_case()
    run_runtime_failure_case()

    print(
        "silver_quarantine_routing_"
        "smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
