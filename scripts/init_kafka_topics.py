#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = (
    PROJECT_ROOT
    / "contracts"
    / "kafka"
    / "simulated_bci_device_events_v1.topic.json"
)

TOPIC_NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True)
class TopicContract:
    contract_version: str
    topic_name: str
    event_schema: str
    key_field: str
    partitions: int
    replication_factor: int
    configs: dict[str, str]

    @classmethod
    def load(
        cls,
        path: Path,
    ) -> TopicContract:
        raw = json.loads(
            path.read_text(encoding="utf-8")
        )

        required = {
            "contract_version",
            "topic_name",
            "description",
            "event_schema",
            "key_field",
            "partitions",
            "replication_factor",
            "configs",
        }

        actual = set(raw)

        if actual != required:
            missing = sorted(required - actual)
            unexpected = sorted(actual - required)

            raise ValueError(
                "Invalid Kafka topic contract fields: "
                f"missing={missing}, "
                f"unexpected={unexpected}"
            )

        contract = cls(
            contract_version=str(
                raw["contract_version"]
            ),
            topic_name=str(raw["topic_name"]),
            event_schema=str(raw["event_schema"]),
            key_field=str(raw["key_field"]),
            partitions=raw["partitions"],
            replication_factor=raw[
                "replication_factor"
            ],
            configs=dict(raw["configs"]),
        )

        contract.validate()
        return contract

    def validate(self) -> None:
        if self.contract_version != "1.0.0":
            raise ValueError(
                "Unsupported Kafka topic "
                f"contract_version: "
                f"{self.contract_version}"
            )

        if not TOPIC_NAME_PATTERN.fullmatch(
            self.topic_name
        ):
            raise ValueError(
                "Invalid Kafka topic_name: "
                f"{self.topic_name}"
            )

        if (
            not isinstance(self.partitions, int)
            or isinstance(self.partitions, bool)
            or self.partitions <= 0
        ):
            raise ValueError(
                "partitions must be a positive integer"
            )

        if (
            not isinstance(
                self.replication_factor,
                int,
            )
            or isinstance(
                self.replication_factor,
                bool,
            )
            or self.replication_factor <= 0
        ):
            raise ValueError(
                "replication_factor must be "
                "a positive integer"
            )

        if self.replication_factor != 1:
            raise ValueError(
                "Local Phase 11 topic contract "
                "must use replication_factor=1"
            )

        if self.key_field != "device_id":
            raise ValueError(
                "Device event topic key_field "
                "must be device_id"
            )

        expected_configs = {
            "cleanup.policy",
            "retention.ms",
            "min.insync.replicas",
        }

        if set(self.configs) != expected_configs:
            raise ValueError(
                "Kafka topic config keys do not "
                "match the Phase 11 contract"
            )

        for key, value in self.configs.items():
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"Kafka topic config {key} "
                    "must be a non-empty string"
                )

        schema_path = PROJECT_ROOT / self.event_schema

        if not schema_path.is_file():
            raise ValueError(
                "Device event schema does not exist: "
                f"{self.event_schema}"
            )

        schema = json.loads(
            schema_path.read_text(encoding="utf-8")
        )

        if schema.get("$id") != (
            "urn:neurosleep:"
            "simulated-bci-device-event:1.0.0"
        ):
            raise ValueError(
                "Kafka topic contract references "
                "an unexpected event schema"
            )


def run_command(
    args: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=check,
    )


def kafka_command(
    script_name: str,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            f"/opt/kafka/bin/{script_name}",
            *args,
        ],
        check=check,
    )


def require_kafka_running() -> None:
    result = run_command(
        [
            "docker",
            "compose",
            "ps",
            "--status",
            "running",
            "--services",
            "kafka",
        ]
    )

    services = {
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    }

    if "kafka" not in services:
        raise RuntimeError(
            "Kafka is not running. "
            "Run `make kafka-up` first."
        )


def list_topics() -> set[str]:
    result = kafka_command(
        "kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--list",
    )

    return {
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    }


def create_topic(
    contract: TopicContract,
) -> None:
    command = [
        "--bootstrap-server",
        "localhost:9092",
        "--create",
        "--topic",
        contract.topic_name,
        "--partitions",
        str(contract.partitions),
        "--replication-factor",
        str(contract.replication_factor),
    ]

    for key, value in sorted(
        contract.configs.items()
    ):
        command.extend(
            [
                "--config",
                f"{key}={value}",
            ]
        )

    result = kafka_command(
        "kafka-topics.sh",
        *command,
    )

    if result.stdout.strip():
        print(result.stdout.strip())


def describe_topic(
    contract: TopicContract,
) -> str:
    result = kafka_command(
        "kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--topic",
        contract.topic_name,
    )
    return result.stdout


def describe_topic_configs(
    contract: TopicContract,
) -> str:
    result = kafka_command(
        "kafka-configs.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--entity-type",
        "topics",
        "--entity-name",
        contract.topic_name,
        "--describe",
    )
    return result.stdout


def validate_runtime_topic(
    contract: TopicContract,
) -> None:
    description = describe_topic(contract)

    partition_match = re.search(
        r"PartitionCount:\s*(\d+)",
        description,
    )
    replication_match = re.search(
        r"ReplicationFactor:\s*(\d+)",
        description,
    )

    if partition_match is None:
        raise RuntimeError(
            "Could not read Kafka partition count"
        )

    if replication_match is None:
        raise RuntimeError(
            "Could not read Kafka replication factor"
        )

    partitions = int(partition_match.group(1))
    replication_factor = int(
        replication_match.group(1)
    )

    if partitions != contract.partitions:
        raise RuntimeError(
            "Kafka topic partition drift: "
            f"expected={contract.partitions}, "
            f"actual={partitions}"
        )

    if (
        replication_factor
        != contract.replication_factor
    ):
        raise RuntimeError(
            "Kafka topic replication drift: "
            "expected="
            f"{contract.replication_factor}, "
            f"actual={replication_factor}"
        )

    config_description = describe_topic_configs(
        contract
    )

    for key, expected_value in sorted(
        contract.configs.items()
    ):
        pattern = re.compile(
            rf"(?<![A-Za-z0-9._-])"
            rf"{re.escape(key)}="
            rf"{re.escape(expected_value)}"
            rf"(?![A-Za-z0-9._-])"
        )

        if pattern.search(
            config_description
        ) is None:
            raise RuntimeError(
                "Kafka topic config drift: "
                f"{key} expected={expected_value}"
            )

    print(
        "kafka_topic_name="
        f"{contract.topic_name}"
    )
    print(
        "kafka_topic_partitions="
        f"{partitions}"
    )
    print(
        "kafka_topic_replication_factor="
        f"{replication_factor}"
    )
    print(
        "kafka_topic_key_field="
        f"{contract.key_field}"
    )

    for key, value in sorted(
        contract.configs.items()
    ):
        normalized_key = key.replace(".", "_")
        print(
            "kafka_topic_config_"
            f"{normalized_key}={value}"
        )

    print(
        "kafka_topic_contract_status=success"
    )


def main() -> None:
    contract = TopicContract.load(CONTRACT_PATH)

    require_kafka_running()

    topics_before = list_topics()

    if contract.topic_name in topics_before:
        topic_status = "existing"
    else:
        create_topic(contract)
        topic_status = "created"

    topics_after = list_topics()

    if contract.topic_name not in topics_after:
        raise RuntimeError(
            "Kafka topic was not available "
            "after initialization"
        )

    print(
        f"kafka_topic_status={topic_status}"
    )

    validate_runtime_topic(contract)


if __name__ == "__main__":
    main()
