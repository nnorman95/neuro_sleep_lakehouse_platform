#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import re
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = PROJECT_ROOT / "scripts/sql/migrations_manifest.txt"
ALLOWED_ROOTS = (
    Path("scripts/sql/migrations"),
    Path("scripts/sql/seeds"),
)
FILE_NAME_PATTERN = re.compile(
    r"^(?P<number>[0-9]{3})_[a-z0-9_]+[.]sql$"
)
HISTORY_MIGRATION = (
    "scripts/sql/migrations/"
    "047_create_ops_sql_migration_history.sql"
)


def fail(message: str) -> int:
    print(
        f"SQL manifest contract: FAIL: {message}",
        file=sys.stderr,
    )
    return 1


def main() -> int:
    try:
        raw_lines = MANIFEST.read_text(
            encoding="utf-8"
        ).splitlines()
    except OSError as exc:
        return fail(str(exc))

    entries: list[str] = []
    seen_paths: set[str] = set()
    seen_numbers: dict[str, str] = {}
    migration_count = 0
    seed_count = 0

    for line_number, raw_line in enumerate(
        raw_lines,
        start=1,
    ):
        entry = raw_line.strip()
        if not entry or entry.startswith("#"):
            continue

        relative_path = Path(entry)

        if relative_path.is_absolute():
            return fail(
                f"line {line_number} is absolute: {entry}"
            )

        if ".." in relative_path.parts:
            return fail(
                f"line {line_number} contains parent traversal: "
                f"{entry}"
            )

        if relative_path.parent not in ALLOWED_ROOTS:
            return fail(
                f"line {line_number} is outside migrations/seeds: "
                f"{entry}"
            )

        if entry in seen_paths:
            return fail(
                f"duplicate path on line {line_number}: {entry}"
            )

        match = FILE_NAME_PATTERN.fullmatch(
            relative_path.name
        )
        if match is None:
            return fail(
                f"invalid numbered SQL filename on line "
                f"{line_number}: {entry}"
            )

        number = match.group("number")
        previous_path = seen_numbers.get(number)
        if previous_path is not None:
            return fail(
                f"duplicate SQL sequence {number}: "
                f"{previous_path} and {entry}"
            )

        absolute_path = PROJECT_ROOT / relative_path
        if not absolute_path.is_file():
            return fail(
                f"missing SQL file on line {line_number}: "
                f"{entry}"
            )

        seen_paths.add(entry)
        seen_numbers[number] = entry
        entries.append(entry)

        if relative_path.parent == ALLOWED_ROOTS[0]:
            migration_count += 1
        else:
            seed_count += 1

    if not entries:
        return fail("manifest contains no SQL entries")

    if HISTORY_MIGRATION not in seen_paths:
        return fail(
            "migration-history bootstrap migration is missing"
        )

    print(
        "sql_manifest_entries="
        f"{len(entries)}"
    )
    print(
        "sql_manifest_migrations="
        f"{migration_count}"
    )
    print(
        "sql_manifest_seeds="
        f"{seed_count}"
    )
    print(
        "sql_manifest_unique_sequences="
        f"{len(seen_numbers)}"
    )
    print("sql_manifest_contract_status=success")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
