#!/usr/bin/env python3
"""Validate that requirements.txt mirrors pyproject.toml runtime dependencies."""

from __future__ import annotations

from pathlib import Path
import sys
import tomllib


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = PROJECT_ROOT / "pyproject.toml"
REQUIREMENTS = PROJECT_ROOT / "requirements.txt"


def load_pyproject_dependencies() -> list[str]:
    with PYPROJECT.open("rb") as handle:
        data = tomllib.load(handle)

    dependencies = data.get("project", {}).get("dependencies")
    if not isinstance(dependencies, list) or not all(
        isinstance(item, str) for item in dependencies
    ):
        raise ValueError(
            "pyproject.toml must define [project].dependencies as a list of strings"
        )

    return [item.strip() for item in dependencies]


def load_requirements() -> list[str]:
    dependencies: list[str] = []

    for line_number, raw_line in enumerate(
        REQUIREMENTS.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("-") or "://" in line:
            raise ValueError(
                f"requirements.txt line {line_number} is not a plain runtime "
                f"dependency: {line!r}"
            )

        dependencies.append(line)

    return dependencies


def main() -> int:
    try:
        pyproject_dependencies = load_pyproject_dependencies()
        requirements_dependencies = load_requirements()
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        print(f"Dependency contract: FAIL: {exc}", file=sys.stderr)
        return 1

    if pyproject_dependencies != requirements_dependencies:
        print("Dependency contract: FAIL", file=sys.stderr)
        print(
            "requirements.txt must exactly mirror "
            "[project].dependencies in pyproject.toml.",
            file=sys.stderr,
        )

        print("\npyproject.toml:", file=sys.stderr)
        for item in pyproject_dependencies:
            print(f"  {item}", file=sys.stderr)

        print("\nrequirements.txt:", file=sys.stderr)
        for item in requirements_dependencies:
            print(f"  {item}", file=sys.stderr)

        return 1

    print(
        f"Dependency contract: OK ({len(pyproject_dependencies)} dependencies)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
