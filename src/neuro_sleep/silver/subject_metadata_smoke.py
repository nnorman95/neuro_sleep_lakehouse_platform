from neuro_sleep.silver.subject_metadata import (
    NormalizedSubjectMetadata,
    RecordingSubjectContext,
    build_subject_metadata_bundle,
    parse_sc_rows,
    parse_st_rows,
)


def expect_value_error(
    function,
) -> None:
    try:
        function()
    except ValueError:
        return

    raise RuntimeError(
        "Expected ValueError was not raised"
    )


def main() -> None:
    sc_bundle = parse_sc_rows(
        (
            (
                0.0,
                1.0,
                33.0,
                1.0,
                0.02638888888888889,
            ),
            (
                0.0,
                2.0,
                33.0,
                1.0,
                0.9145833333333333,
            ),
            (
                1.0,
                1.0,
                33.0,
                1.0,
                0.9472222222222223,
            ),
            (
                1.0,
                2.0,
                33.0,
                1.0,
                0.9270833333333334,
            ),
            (
                10.0,
                1.0,
                28.0,
                2.0,
                0.9500000000000000,
            ),
        )
    )

    if len(sc_bundle.subjects) != 3:
        raise RuntimeError(
            "SC subjects were not deduplicated"
        )

    sc_contexts = (
        sc_bundle.context_by_recording_key()
    )

    required_sc_recording_keys = {
        "SC4001E",
        "SC4002E",
        "SC4011E",
        "SC4012E",
        "SC4101E",
    }

    if not required_sc_recording_keys.issubset(
        sc_contexts
    ):
        raise RuntimeError(
            "SC recording-key mapping failed"
        )

    if (
        sc_contexts[
            "SC4001E"
        ].source_subject_id
        != "SC00"
    ):
        raise RuntimeError(
            "SC subject mapping failed"
        )

    if (
        sc_contexts[
            "SC4001E"
        ].lights_off_seconds
        != 2280
    ):
        raise RuntimeError(
            "SC lights-off conversion failed"
        )

    sc_subjects = (
        sc_bundle.subject_by_source_id()
    )

    if sc_subjects["SC00"].sex != "F":
        raise RuntimeError(
            "SC female code normalization failed"
        )

    if sc_subjects["SC10"].sex != "M":
        raise RuntimeError(
            "SC male code normalization failed"
        )

    st_bundle = parse_st_rows(
        (
            (
                1.0,
                60.0,
                1.0,
                1.0,
                0.9590277777777777,
                2.0,
                0.9916666666666667,
            ),
            (
                2.0,
                35.0,
                2.0,
                2.0,
                0.9770833333333333,
                1.0,
                0.0,
            ),
        )
    )

    if len(st_bundle.subjects) != 2:
        raise RuntimeError(
            "ST subject parsing failed"
        )

    st_subjects = (
        st_bundle.subject_by_source_id()
    )

    if st_subjects["ST01"].sex != "M":
        raise RuntimeError(
            "ST male code normalization failed"
        )

    if st_subjects["ST02"].sex != "F":
        raise RuntimeError(
            "ST female code normalization failed"
        )

    st_contexts = (
        st_bundle.context_by_recording_key()
    )

    if (
        st_contexts["ST7011J"].treatment
        != "placebo"
    ):
        raise RuntimeError(
            "ST placebo-night mapping failed"
        )

    if (
        st_contexts["ST7012J"].treatment
        != "temazepam"
    ):
        raise RuntimeError(
            "ST temazepam-night mapping failed"
        )

    expect_value_error(
        lambda: build_subject_metadata_bundle(
            (
                NormalizedSubjectMetadata(
                    collection="sleep-cassette",
                    source_subject_id="SC00",
                    source_subject_number=0,
                    age_years=33,
                    sex="F",
                ),
                NormalizedSubjectMetadata(
                    collection="sleep-cassette",
                    source_subject_id="SC00",
                    source_subject_number=0,
                    age_years=34,
                    sex="F",
                ),
            ),
            (
                RecordingSubjectContext(
                    collection="sleep-cassette",
                    recording_key="SC4001E",
                    source_subject_id="SC00",
                    night_number=1,
                    lights_off_seconds=2280,
                    treatment=None,
                ),
            ),
        )
    )

    expect_value_error(
        lambda: parse_sc_rows(
            (
                (
                    0,
                    1,
                    33,
                    3,
                    0.5,
                ),
            )
        )
    )

    expect_value_error(
        lambda: build_subject_metadata_bundle(
            (
                NormalizedSubjectMetadata(
                    collection="sleep-cassette",
                    source_subject_id="SC00",
                    source_subject_number=0,
                    age_years=33,
                    sex="F",
                ),
            ),
            (
                RecordingSubjectContext(
                    collection="sleep-cassette",
                    recording_key="SC4001E",
                    source_subject_id="SC00",
                    night_number=1,
                    lights_off_seconds=2280,
                    treatment=None,
                ),
                RecordingSubjectContext(
                    collection="sleep-cassette",
                    recording_key="SC4001E",
                    source_subject_id="SC00",
                    night_number=1,
                    lights_off_seconds=2280,
                    treatment=None,
                ),
            ),
        )
    )

    print(
        "sc_subject_deduplication=true"
    )
    print(
        "sc_recording_key_mapping=true"
    )
    print(
        "sc_lights_off_conversion=true"
    )
    print(
        "sc_subject_sex_normalization=true"
    )
    print(
        "st_subject_sex_normalization=true"
    )
    print(
        "st_treatment_night_mapping=true"
    )
    print(
        "conflicting_demographics_blocked=true"
    )
    print(
        "invalid_sex_code_blocked=true"
    )
    print(
        "duplicate_recording_context_blocked=true"
    )
    print(
        "subject_metadata_parser_smoke_status="
        "success"
    )


if __name__ == "__main__":
    main()
