from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True)
class RawFileRecord:
    file_id: UUID
    source_system: str
    source_url: str | None
    bucket: str
    object_key: str
    file_name: str
    file_type: str
    file_size_bytes: int | None
    checksum_sha256: str | None
    ingestion_run_id: UUID | None
    status: str
    ingested_at: datetime | None
