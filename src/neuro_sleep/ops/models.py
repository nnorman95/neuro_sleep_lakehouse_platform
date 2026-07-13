from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class PipelineRunRecord:
    run_id: UUID
    pipeline_name: str
    task_name: str | None
    source_system: str | None
    status: str
    rows_read: int
    rows_written: int
    files_processed: int
    records_quarantined: int
    error_message: str | None
