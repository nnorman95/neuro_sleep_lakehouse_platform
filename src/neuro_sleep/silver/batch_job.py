from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Literal
from uuid import UUID

from neuro_sleep.config import (
    Settings,
    get_settings,
)
from neuro_sleep.observability.structured_logging import (
    emit_event,
    emit_exception,
)
from neuro_sleep.silver.batch_discovery import (
    SleepEdfRecordingPair,
    discover_sleep_edf_recording_pairs,
)
from neuro_sleep.silver.silver_job import (
    run_tracked_silver_job,
)
from neuro_sleep.silver.signal_extractor import (
    DEFAULT_CHUNK_DURATION_SECONDS,
)


BatchItemStatus = Literal[
    "written",
    "skipped",
    "failed",
]


@dataclass(frozen=True)
class SilverBatchItemResult:
    pair: SleepEdfRecordingPair
    status: BatchItemStatus

    run_id: UUID | None = None
    recording_id: UUID | None = None
    output_prefix: str | None = None

    row_count: int = 0
    data_object_count: int = 0
    total_object_count: int = 0

    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class SilverBatchResult:
    items: tuple[SilverBatchItemResult, ...]

    @property
    def recording_count(self) -> int:
        return len(self.items)

    @property
    def written_count(self) -> int:
        return sum(
            item.status == "written"
            for item in self.items
        )

    @property
    def skipped_count(self) -> int:
        return sum(
            item.status == "skipped"
            for item in self.items
        )

    @property
    def failed_count(self) -> int:
        return sum(
            item.status == "failed"
            for item in self.items
        )

    @property
    def successful_count(self) -> int:
        return (
            self.written_count
            + self.skipped_count
        )

    @property
    def total_row_count(self) -> int:
        return sum(
            item.row_count
            for item in self.items
            if item.status != "failed"
        )

    @property
    def passed(self) -> bool:
        return self.failed_count == 0



def format_batch_progress(
    *,
    completed_count: int,
    recording_count: int,
    recording_key: str,
    state: str,
    elapsed_seconds: float | None = None,
    row_count: int | None = None,
) -> str:
    if recording_count <= 0:
        raise ValueError(
            "recording_count must be positive"
        )

    if not (
        0
        <= completed_count
        <= recording_count
    ):
        raise ValueError(
            "completed_count is outside "
            "the batch range"
        )

    percent = round(
        completed_count
        / recording_count
        * 100
    )

    parts = [
        "Silver batch",
        f"[{completed_count}/{recording_count}]",
        f"{percent}%",
        recording_key,
        state,
    ]

    if row_count is not None:
        parts.append(
            f"rows={row_count:,}"
        )

    if elapsed_seconds is not None:
        parts.append(
            f"elapsed={elapsed_seconds:.1f}s"
        )

    return " | ".join(parts)

def run_silver_batch(
    *,
    settings: Settings | None = None,
    continue_on_error: bool = True,
    signal_chunk_duration_seconds: float = (
        DEFAULT_CHUNK_DURATION_SECONDS
    ),
    signal_start_seconds: float = 0.0,
    signal_stop_seconds: float | None = None,
    verify_payload_checksums: bool = True,
) -> SilverBatchResult:
    if settings is None:
        settings = get_settings()

    pairs = discover_sleep_edf_recording_pairs(
        settings=settings
    )

    print(
        format_batch_progress(
            completed_count=0,
            recording_count=len(pairs),
            recording_key="all-recordings",
            state="started",
        ),
        flush=True,
    )

    emit_event(
        event="silver_batch_started",
        recording_count=len(pairs),
        dataset_version=(
            settings.sleep_edf_version
        ),
        data_profile=settings.data_profile,
    )

    items: list[SilverBatchItemResult] = []

    for index, pair in enumerate(
        pairs,
        start=1,
    ):
        item_started_at = perf_counter()

        print(
            format_batch_progress(
                completed_count=index - 1,
                recording_count=len(pairs),
                recording_key=(
                    pair.recording_key
                ),
                state="processing",
            ),
            flush=True,
        )

        emit_event(
            event="silver_batch_item_started",
            item_index=index,
            recording_count=len(pairs),
            study_folder=pair.study_folder,
            recording_key=pair.recording_key,
            psg_object_key=(
                pair.psg_object_key
            ),
            hypnogram_object_key=(
                pair.hypnogram_object_key
            ),
        )

        try:
            tracked_result = (
                run_tracked_silver_job(
                    psg_bucket=(
                        pair.psg_bucket
                    ),
                    psg_object_key=(
                        pair.psg_object_key
                    ),
                    hypnogram_bucket=(
                        pair.hypnogram_bucket
                    ),
                    hypnogram_object_key=(
                        pair.hypnogram_object_key
                    ),
                    silver_bucket="silver",
                    root_prefix=(
                        pair.silver_root_prefix
                    ),
                    signal_chunk_duration_seconds=(
                        signal_chunk_duration_seconds
                    ),
                    signal_start_seconds=(
                        signal_start_seconds
                    ),
                    signal_stop_seconds=(
                        signal_stop_seconds
                    ),
                    verify_payload_checksums=(
                        verify_payload_checksums
                    ),
                    settings=settings,
                )
            )

        except Exception as error:
            failed_item = SilverBatchItemResult(
                pair=pair,
                status="failed",
                error_type=type(error).__name__,
                error_message=str(error),
            )

            items.append(failed_item)

            emit_exception(
                event="silver_batch_item_failed",
                error=error,
                item_index=index,
                recording_count=len(pairs),
                study_folder=pair.study_folder,
                recording_key=(
                    pair.recording_key
                ),
                psg_object_key=(
                    pair.psg_object_key
                ),
                hypnogram_object_key=(
                    pair.hypnogram_object_key
                ),
            )

            print(
                format_batch_progress(
                    completed_count=index,
                    recording_count=len(pairs),
                    recording_key=(
                        pair.recording_key
                    ),
                    state="failed",
                    elapsed_seconds=(
                        perf_counter()
                        - item_started_at
                    ),
                ),
                flush=True,
            )

            if not continue_on_error:
                raise

            continue

        pipeline_result = (
            tracked_result.pipeline_result
        )

        item = SilverBatchItemResult(
            pair=pair,
            status=tracked_result.status,
            run_id=tracked_result.run_id,
            recording_id=(
                tracked_result.recording_id
            ),
            output_prefix=(
                tracked_result.output_prefix
            ),
            row_count=(
                tracked_result.row_count
            ),
            data_object_count=(
                pipeline_result
                .data_object_count
            ),
            total_object_count=(
                pipeline_result
                .total_object_count
            ),
        )

        items.append(item)

        print(
            format_batch_progress(
                completed_count=index,
                recording_count=len(pairs),
                recording_key=(
                    pair.recording_key
                ),
                state=item.status,
                row_count=item.row_count,
                elapsed_seconds=(
                    perf_counter()
                    - item_started_at
                ),
            ),
            flush=True,
        )

        emit_event(
            event="silver_batch_item_completed",
            item_index=index,
            recording_count=len(pairs),
            study_folder=pair.study_folder,
            recording_key=pair.recording_key,
            status=item.status,
            run_id=item.run_id,
            recording_id=item.recording_id,
            output_prefix=item.output_prefix,
            row_count=item.row_count,
            data_object_count=(
                item.data_object_count
            ),
            total_object_count=(
                item.total_object_count
            ),
        )

    result = SilverBatchResult(
        items=tuple(items)
    )

    print(
        format_batch_progress(
            completed_count=(
                result.recording_count
            ),
            recording_count=(
                result.recording_count
            ),
            recording_key="all-recordings",
            state=(
                "completed"
                if result.passed
                else "completed-with-errors"
            ),
            row_count=(
                result.total_row_count
            ),
        ),
        flush=True,
    )

    emit_event(
        event="silver_batch_completed",
        recording_count=(
            result.recording_count
        ),
        written_count=result.written_count,
        skipped_count=result.skipped_count,
        failed_count=result.failed_count,
        total_row_count=(
            result.total_row_count
        ),
        passed=result.passed,
    )

    return result
