#!/usr/bin/env python3
from __future__ import annotations

import argparse

from neuro_sleep.streaming.device_event_inbox import (
    delete_inbox_event_for_smoke_test,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-id", required=True)
    args = parser.parse_args()

    delete_inbox_event_for_smoke_test(args.event_id)

    print("kafka_warehouse_fixture_cleanup=success")


if __name__ == "__main__":
    main()
