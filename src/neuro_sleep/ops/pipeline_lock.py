from psycopg import Connection

from neuro_sleep.config import Settings
from neuro_sleep.db.postgres import (
    open_postgres_connection,
)
from neuro_sleep.reliability.errors import (
    ConcurrentPipelineRunError,
)


LOCK_NAMESPACE = "neuro_sleep_pipeline"


class PipelineRunLock:
    def __init__(
        self,
        pipeline_name: str,
        settings: Settings | None = None,
    ) -> None:
        normalized_name = (
            pipeline_name.strip()
        )

        if not normalized_name:
            raise ValueError(
                "pipeline_name cannot be empty"
            )

        self.pipeline_name = normalized_name
        self.settings = settings

        self.lock_name = (
            f"{LOCK_NAMESPACE}:"
            f"{self.pipeline_name}"
        )

        self._connection: (
            Connection | None
        ) = None

        self._is_acquired = False

    @property
    def is_acquired(self) -> bool:
        return self._is_acquired

    def acquire(self) -> None:
        if self._is_acquired:
            raise RuntimeError(
                "Pipeline lock is already acquired: "
                f"{self.pipeline_name}"
            )

        connection = open_postgres_connection(
            settings=self.settings
        )

        connection.autocommit = True

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select pg_try_advisory_lock(
                        hashtextextended(
                            %s::text,
                            0
                        )
                    );
                    """,
                    (self.lock_name,),
                )

                row = cursor.fetchone()

            lock_acquired = (
                row is not None
                and row[0] is True
            )

            if not lock_acquired:
                raise ConcurrentPipelineRunError(
                    "Another instance of pipeline "
                    f"'{self.pipeline_name}' "
                    "is already running."
                )

        except Exception:
            connection.close()
            raise

        self._connection = connection
        self._is_acquired = True

    def release(self) -> None:
        connection = self._connection

        self._connection = None
        self._is_acquired = False

        if connection is None:
            return

        if connection.closed:
            return

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select pg_advisory_unlock(
                        hashtextextended(
                            %s::text,
                            0
                        )
                    );
                    """,
                    (self.lock_name,),
                )

                row = cursor.fetchone()

            if (
                row is None
                or row[0] is not True
            ):
                raise RuntimeError(
                    "PostgreSQL did not confirm "
                    "pipeline lock release: "
                    f"{self.pipeline_name}"
                )

        finally:
            connection.close()

    def __enter__(
        self,
    ) -> "PipelineRunLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self.release()
