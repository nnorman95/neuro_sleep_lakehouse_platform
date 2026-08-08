from __future__ import annotations

from neuro_sleep.silver.idempotency import (
    build_config_id,
    canonical_transform_config_text,
)
from neuro_sleep.sources.sleep_edf_manifest import (
    parse_sleep_edf_checksum_manifest,
    select_sleep_edf_source_files,
)

SAMPLE_CHECKSUMS = """
1111111111111111111111111111111111111111111111111111111111111111 sleep-cassette/SC4001E0-PSG.edf
2222222222222222222222222222222222222222222222222222222222222222 sleep-cassette/SC4001EC-Hypnogram.edf
3333333333333333333333333333333333333333333333333333333333333333 sleep-cassette/SC4002E0-PSG.edf
4444444444444444444444444444444444444444444444444444444444444444 sleep-cassette/SC4002EC-Hypnogram.edf
5555555555555555555555555555555555555555555555555555555555555555 sleep-telemetry/ST7011J0-PSG.edf
6666666666666666666666666666666666666666666666666666666666666666 sleep-telemetry/ST7011JP-Hypnogram.edf
"""


def run_smoke_test() -> None:
    files = parse_sleep_edf_checksum_manifest(
        checksum_text=SAMPLE_CHECKSUMS,
        dataset_version="1.0.0",
    )
    selected = select_sleep_edf_source_files(
        files=files,
        max_recordings=1,
        include_cassette=True,
        include_telemetry=True,
        include_metadata=False,
        recording_keys=("SC4002E", "ST7011J"),
    )
    keys = {x.recording_key for x in selected if x.recording_key is not None}
    if keys != {"SC4002E", "ST7011J"}:
        raise RuntimeError("recording allowlist failed")
    try:
        select_sleep_edf_source_files(
            files=files,
            max_recordings=0,
            include_cassette=True,
            include_telemetry=True,
            include_metadata=False,
            recording_keys=("DOES_NOT_EXIST",),
        )
    except ValueError:
        pass
    else:
        raise RuntimeError("missing recording key was not blocked")
    full_text = canonical_transform_config_text(300.0, 0.0, None, True)
    metadata_text = canonical_transform_config_text(300.0, 0.0, None, False)
    if "include_signals=" in full_text:
        raise RuntimeError("full config compatibility changed")
    if not metadata_text.endswith("include_signals=false"):
        raise RuntimeError("metadata-only config marker missing")
    if build_config_id(300.0, 0.0, None, True) == build_config_id(300.0, 0.0, None, False):
        raise RuntimeError("config ids must differ")
    print("recording_allowlist_selection=true")
    print("recording_allowlist_missing_key_blocked=true")
    print("full_signal_config_backward_compatible=true")
    print("metadata_only_config_distinct=true")
    print("phase7_cohort_controls_smoke_status=success")


if __name__ == "__main__":
    run_smoke_test()
