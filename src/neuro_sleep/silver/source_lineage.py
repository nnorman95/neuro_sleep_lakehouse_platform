from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import re
from uuid import UUID

from neuro_sleep.raw.file_registry import (
    get_raw_file_by_object_key,
)


SHA256_PATTERN = re.compile(
    r"^[0-9a-f]{64}$"
)


@dataclass(frozen=True)
class SilverSourceLineage:
    source_system: str

    psg_file_id: UUID
    hypnogram_file_id: UUID

    psg_checksum_sha256: str
    hypnogram_checksum_sha256: str

    source_pair_id: str
    input_fingerprint: str


def canonical_source_pair_text(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
) -> str:
    return "\n".join(
        (
            f"psg_bucket={psg_bucket}",
            f"psg_object_key={psg_object_key}",
            (
                "hypnogram_bucket="
                f"{hypnogram_bucket}"
            ),
            (
                "hypnogram_object_key="
                f"{hypnogram_object_key}"
            ),
        )
    )


def build_source_pair_id(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
) -> str:
    canonical_text = (
        canonical_source_pair_text(
            psg_bucket=psg_bucket,
            psg_object_key=psg_object_key,
            hypnogram_bucket=(
                hypnogram_bucket
            ),
            hypnogram_object_key=(
                hypnogram_object_key
            ),
        )
    )

    return sha256(
        canonical_text.encode("utf-8")
    ).hexdigest()


def validate_sha256(
    name: str,
    checksum_sha256: str | None,
) -> str:
    if checksum_sha256 is None:
        raise ValueError(
            f"{name} has no verified SHA-256"
        )

    normalized_checksum = (
        checksum_sha256.strip().lower()
    )

    if not SHA256_PATTERN.fullmatch(
        normalized_checksum
    ):
        raise ValueError(
            f"{name} has an invalid SHA-256"
        )

    return normalized_checksum


def canonical_input_fingerprint_text(
    psg_checksum_sha256: str,
    hypnogram_checksum_sha256: str,
) -> str:
    psg_checksum = validate_sha256(
        name="psg_checksum_sha256",
        checksum_sha256=(
            psg_checksum_sha256
        ),
    )

    hypnogram_checksum = validate_sha256(
        name="hypnogram_checksum_sha256",
        checksum_sha256=(
            hypnogram_checksum_sha256
        ),
    )

    return "\n".join(
        (
            (
                "psg_checksum_sha256="
                f"{psg_checksum}"
            ),
            (
                "hypnogram_checksum_sha256="
                f"{hypnogram_checksum}"
            ),
        )
    )


def build_input_fingerprint(
    psg_checksum_sha256: str,
    hypnogram_checksum_sha256: str,
) -> str:
    canonical_text = (
        canonical_input_fingerprint_text(
            psg_checksum_sha256=(
                psg_checksum_sha256
            ),
            hypnogram_checksum_sha256=(
                hypnogram_checksum_sha256
            ),
        )
    )

    return sha256(
        canonical_text.encode("utf-8")
    ).hexdigest()


def resolve_silver_source_lineage(
    psg_bucket: str,
    psg_object_key: str,
    hypnogram_bucket: str,
    hypnogram_object_key: str,
) -> SilverSourceLineage:
    psg_record = get_raw_file_by_object_key(
        bucket=psg_bucket,
        object_key=psg_object_key,
    )

    if psg_record is None:
        raise ValueError(
            "PSG object is missing from "
            "raw.file_registry"
        )

    hypnogram_record = (
        get_raw_file_by_object_key(
            bucket=hypnogram_bucket,
            object_key=(
                hypnogram_object_key
            ),
        )
    )

    if hypnogram_record is None:
        raise ValueError(
            "Hypnogram object is missing from "
            "raw.file_registry"
        )

    if psg_record.status != "uploaded":
        raise ValueError(
            "PSG registry row is not in "
            "uploaded status"
        )

    if hypnogram_record.status != "uploaded":
        raise ValueError(
            "Hypnogram registry row is not in "
            "uploaded status"
        )

    if (
        psg_record.source_system
        != hypnogram_record.source_system
    ):
        raise ValueError(
            "PSG and Hypnogram source systems "
            "do not match"
        )

    psg_checksum = validate_sha256(
        name="PSG registry row",
        checksum_sha256=(
            psg_record.checksum_sha256
        ),
    )

    hypnogram_checksum = validate_sha256(
        name="Hypnogram registry row",
        checksum_sha256=(
            hypnogram_record
            .checksum_sha256
        ),
    )

    source_pair_id = build_source_pair_id(
        psg_bucket=psg_bucket,
        psg_object_key=psg_object_key,
        hypnogram_bucket=hypnogram_bucket,
        hypnogram_object_key=(
            hypnogram_object_key
        ),
    )

    input_fingerprint = (
        build_input_fingerprint(
            psg_checksum_sha256=(
                psg_checksum
            ),
            hypnogram_checksum_sha256=(
                hypnogram_checksum
            ),
        )
    )

    return SilverSourceLineage(
        source_system=(
            psg_record.source_system
        ),
        psg_file_id=psg_record.file_id,
        hypnogram_file_id=(
            hypnogram_record.file_id
        ),
        psg_checksum_sha256=(
            psg_checksum
        ),
        hypnogram_checksum_sha256=(
            hypnogram_checksum
        ),
        source_pair_id=source_pair_id,
        input_fingerprint=(
            input_fingerprint
        ),
    )
