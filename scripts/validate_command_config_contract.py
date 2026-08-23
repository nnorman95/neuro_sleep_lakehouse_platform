#!/usr/bin/env python3
from __future__ import annotations

import ast
from pathlib import Path
import re
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
CONFIG_FILE = PROJECT_ROOT / "src/neuro_sleep/config.py"
MAKEFILE = PROJECT_ROOT / "Makefile"

CONFIG_ENV_HELPERS = {
    "_get_int_env",
    "_get_bool_env",
    "_get_csv_env",
    "_get_required_env",
}

CANONICAL_TARGETS = {
    "doctor",
    "env-init",
    "python-env",
    "bootstrap",
    "platform-up",
    "platform-status",
    "platform-down",
    "demo",
    "backfill",
    "migrate",
    "migration-history-check",
    "ci-check",
    "batch-check",
    "test",
    "ops-status",
}

EXPECTED_BATCH_DEPS = (
    "smoke",
    "reliability-smoke",
    "silver-smoke",
    "spark-smoke",
    "gold-reliability-smoke",
    "integrated-gold-reliability-smoke",
)


def fail(message: str) -> int:
    print(
        f"Command/config contract: FAIL: {message}",
        file=sys.stderr,
    )
    return 1


def parse_env_example() -> dict[str, str]:
    values: dict[str, str] = {}

    for line_number, raw_line in enumerate(
        ENV_EXAMPLE.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if "=" not in line:
            raise ValueError(
                f".env.example line {line_number} is not KEY=VALUE"
            )

        key, value = line.split("=", 1)
        key = key.strip()

        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise ValueError(
                f".env.example line {line_number} has invalid key: {key}"
            )

        if key in values:
            raise ValueError(
                f".env.example contains duplicate key: {key}"
            )

        values[key] = value

    return values


def config_env_names() -> set[str]:
    tree = ast.parse(
        CONFIG_FILE.read_text(encoding="utf-8"),
        filename=str(CONFIG_FILE),
    )
    names: set[str] = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue

        first_arg = node.args[0]
        if not (
            isinstance(first_arg, ast.Constant)
            and isinstance(first_arg.value, str)
        ):
            continue

        function_name: str | None = None

        if isinstance(node.func, ast.Name):
            function_name = node.func.id
        elif (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "os"
            and node.func.attr == "getenv"
        ):
            function_name = "os.getenv"

        if (
            function_name == "os.getenv"
            or function_name in CONFIG_ENV_HELPERS
        ):
            names.add(first_arg.value)

    return names


def parse_makefile() -> tuple[
    set[str],
    dict[str, tuple[str, ...]],
    set[str],
    str,
]:
    text = MAKEFILE.read_text(encoding="utf-8")
    targets: set[str] = set()
    dependencies: dict[str, tuple[str, ...]] = {}
    help_targets: set[str] = set()

    target_pattern = re.compile(
        r"^([A-Za-z0-9_.-]+):(?:[ \t]+(.*))?$"
    )
    help_pattern = re.compile(
        r'@echo "make ([A-Za-z0-9_.-]+)(?:[ \t]|")'
    )

    for raw_line in text.splitlines():
        target_match = target_pattern.match(raw_line)
        if target_match:
            target = target_match.group(1)
            raw_dependencies = (
                target_match.group(2) or ""
            ).strip()
            targets.add(target)
            dependencies[target] = tuple(
                item
                for item in raw_dependencies.split()
                if item
            )

        help_match = help_pattern.search(raw_line)
        if help_match:
            help_targets.add(help_match.group(1))

    return targets, dependencies, help_targets, text


def main() -> int:
    try:
        env_values = parse_env_example()
        config_names = config_env_names()
        (
            make_targets,
            make_dependencies,
            help_targets,
            makefile_text,
        ) = parse_makefile()
    except (OSError, SyntaxError, ValueError) as exc:
        return fail(str(exc))

    missing_env = sorted(config_names - env_values.keys())
    if missing_env:
        return fail(
            "config.py variables missing from .env.example: "
            + ", ".join(missing_env)
        )

    if env_values.get("DATA_PROFILE") != "sample":
        return fail(
            ".env.example DATA_PROFILE must explicitly default to sample"
        )

    if env_values.get("ACTIVE_SOURCE") != "sleep_edf":
        return fail(
            ".env.example ACTIVE_SOURCE must explicitly default to sleep_edf"
        )

    missing_targets = sorted(CANONICAL_TARGETS - make_targets)
    if missing_targets:
        return fail(
            "canonical Make targets are missing: "
            + ", ".join(missing_targets)
        )

    dangling_help = sorted(help_targets - make_targets)
    if dangling_help:
        return fail(
            "Make help advertises undefined targets: "
            + ", ".join(dangling_help)
        )

    batch_deps = make_dependencies.get("batch-check")
    if batch_deps != EXPECTED_BATCH_DEPS:
        return fail(
            "batch-check dependencies changed: "
            f"expected {EXPECTED_BATCH_DEPS}, got {batch_deps}"
        )

    if make_dependencies.get("test") != ("batch-check",):
        return fail(
            "make test must remain a compatibility alias for batch-check"
        )

    if 'make test               Run all test suites' in makefile_text:
        return fail(
            "Make help still overstates the scope of make test"
        )

    print(f"config_env_variables={len(config_names)}")
    print(f"env_example_variables={len(env_values)}")
    print(
        "config_env_missing_from_example="
        f"{len(missing_env)}"
    )
    print(
        "make_canonical_targets="
        f"{len(CANONICAL_TARGETS)}"
    )
    print(
        "make_help_targets="
        f"{len(help_targets)}"
    )
    print("make_test_alias=batch-check")
    print("command_config_contract_status=success")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
