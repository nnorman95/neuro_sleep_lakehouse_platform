from __future__ import annotations

from neuro_sleep.spark.signal_input import (
    discover_selected_signal_inputs,
)


def run_smoke_test() -> None:
    inputs = discover_selected_signal_inputs(
        verify_live_objects=True
    )

    identities = {
        (
            item.source_system,
            item.dataset_version,
            item.collection,
            item.recording_key,
        )
        for item in inputs
    }
    if len(identities) != len(inputs):
        raise RuntimeError(
            "Duplicate logical recording identity in Spark signal input"
        )

    all_object_keys = [
        object_key
        for item in inputs
        for object_key in item.signal_object_keys
    ]
    if len(all_object_keys) != len(set(all_object_keys)):
        raise RuntimeError(
            "One Silver signal object was selected more than once"
        )

    total_files = sum(
        item.signal_file_count for item in inputs
    )
    total_rows = sum(
        item.signal_row_count for item in inputs
    )
    total_bytes = sum(
        item.signal_size_bytes for item in inputs
    )

    if total_files <= 0:
        raise RuntimeError(
            "Selected signal file count must be positive"
        )
    if total_rows <= 0:
        raise RuntimeError(
            "Selected signal row count must be positive"
        )
    if total_bytes <= 0:
        raise RuntimeError(
            "Selected signal byte count must be positive"
        )

    for item in inputs:
        print(
            f"{item.recording_key}: "
            f"files={item.signal_file_count} "
            f"rows={item.signal_row_count} "
            "size_mib="
            f"{item.signal_size_bytes / (1024 ** 2):.2f}"
        )

    print()
    print(f"selected_signal_recordings={len(inputs)}")
    print(f"selected_signal_files={total_files}")
    print(f"selected_signal_rows={total_rows}")
    print(
        "selected_signal_size_gib="
        f"{total_bytes / (1024 ** 3):.3f}"
    )
    print("selected_signal_input_smoke_status=success")


if __name__ == "__main__":
    run_smoke_test()
