from neuro_sleep.ops.pipeline_lock import (
    PipelineRunLock,
)
from neuro_sleep.reliability.errors import (
    ConcurrentPipelineRunError,
)


PIPELINE_NAME = "pipeline_lock_smoke_test"


def run_smoke_test() -> None:
    first_lock = PipelineRunLock(
        pipeline_name=PIPELINE_NAME
    )

    second_lock = PipelineRunLock(
        pipeline_name=PIPELINE_NAME
    )

    first_lock.acquire()

    try:
        if not first_lock.is_acquired:
            raise RuntimeError(
                "First lock was not acquired"
            )

        print(
            "first_pipeline_lock_acquired=true"
        )

        try:
            second_lock.acquire()

        except ConcurrentPipelineRunError:
            print(
                "concurrent_pipeline_blocked=true"
            )

        else:
            raise RuntimeError(
                "Second pipeline unexpectedly "
                "acquired the same lock"
            )

    finally:
        first_lock.release()

    if first_lock.is_acquired:
        raise RuntimeError(
            "First lock remains acquired"
        )

    print(
        "first_pipeline_lock_released=true"
    )

    try:
        second_lock.acquire()

        if not second_lock.is_acquired:
            raise RuntimeError(
                "Second lock was not acquired "
                "after release"
            )

        print(
            "lock_reacquired_after_release=true"
        )

    finally:
        second_lock.release()

    if second_lock.is_acquired:
        raise RuntimeError(
            "Second lock remains acquired"
        )

    print(
        "pipeline_lock_connection_closed=true"
    )
    print(
        "pipeline_lock_smoke_status=success"
    )


if __name__ == "__main__":
    run_smoke_test()
