from __future__ import annotations

from dataclasses import dataclass

from botocore.client import BaseClient

from neuro_sleep.silver.idempotency import (
    IdempotencyStatus,
    SilverIdempotentWriteResult,
    write_silver_recording_idempotent,
)
from neuro_sleep.silver.reconciliation import (
    SilverReconciliationReport,
    reconcile_silver_output,
)
from neuro_sleep.silver.signal_extractor import (
    DEFAULT_CHUNK_DURATION_SECONDS,
)
from neuro_sleep.silver.silver_recording_writer import (
    QualityReportHandler,
)
from neuro_sleep.storage.object_storage import (
    get_object_storage_client,
)


@dataclass(frozen=True)
class SilverPipelineResult:
    write_result: SilverIdempotentWriteResult
    reconciliation_report: (
        SilverReconciliationReport
    )

    @property
    def status(self) -> IdempotencyStatus:
        return self.write_result.status

    @property
    def recording_id(self):
        return self.write_result.recording_id

    @property
    def output_prefix(self) -> str:
        return self.write_result.output_prefix

    @property
    def source_pair_id(self) -> str:
        return (
            self.write_result.source_pair_id
        )

    @property
    def input_fingerprint(self) -> str:
        return (
            self.write_result
            .input_fingerprint
        )

    @property
    def data_object_count(self) -> int:
        return (
            self.write_result
            .data_object_count
        )

    @property
    def total_object_count(self) -> int:
        return (
            self.write_result
            .total_object_count
        )

    @property
    def row_count(self) -> int:
        return (
            self.reconciliation_report
            .expected_row_count
        )

    @property
    def recovered_partial_output(
        self,
    ) -> bool:
        return (
            self.write_result
            .recovered_partial_output
        )

    @property
    def recovered_object_count(
        self,
    ) -> int:
        return (
            self.write_result
            .recovered_object_count
        )


def run_silver_pipeline(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
    silver_bucket: str,
    root_prefix: str,
    *,
    signal_chunk_duration_seconds: float = (
        DEFAULT_CHUNK_DURATION_SECONDS
    ),
    signal_start_seconds: float = 0.0,
    signal_stop_seconds: float | None = None,
    include_signals: bool = True,
    verify_payload_checksums: bool = True,
    quality_report_handler: (
        QualityReportHandler | None
    ) = None,
    client: BaseClient | None = None,
) -> SilverPipelineResult:
    owns_client = client is None

    if client is None:
        client = get_object_storage_client()

    try:
        write_result = (
            write_silver_recording_idempotent(
                psg_bucket=psg_bucket,
                psg_object_key=psg_object_key,
                hypnogram_bucket=(
                    hypnogram_bucket
                ),
                hypnogram_object_key=(
                    hypnogram_object_key
                ),
                silver_bucket=silver_bucket,
                root_prefix=root_prefix,
                signal_chunk_duration_seconds=(
                    signal_chunk_duration_seconds
                ),
                signal_start_seconds=(
                    signal_start_seconds
                ),
                signal_stop_seconds=(
                    signal_stop_seconds
                ),
                include_signals=include_signals,
                quality_report_handler=(
                    quality_report_handler
                ),
                client=client,
            )
        )

        reconciliation_report = (
            reconcile_silver_output(
                bucket=silver_bucket,
                output_prefix=(
                    write_result.output_prefix
                ),
                verify_payload_checksums=(
                    verify_payload_checksums
                ),
                client=client,
            )
        )

        reconciliation_report.raise_for_errors()

        return SilverPipelineResult(
            write_result=write_result,
            reconciliation_report=(
                reconciliation_report
            ),
        )

    finally:
        if owns_client:
            client.close()
