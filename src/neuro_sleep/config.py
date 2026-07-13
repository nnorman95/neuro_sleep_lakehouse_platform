from dataclasses import dataclass, field
import os

from dotenv import load_dotenv

from neuro_sleep.paths import PROJECT_ROOT


load_dotenv(PROJECT_ROOT / ".env", override=False)


def _get_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)

    if raw_value is None or raw_value == "":
        return default

    value = int(raw_value)

    if value < 0:
        raise ValueError(
            f"{name} must be 0 or a positive integer"
        )

    return value


def _get_bool_env(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)

    if raw_value is None or raw_value == "":
        return default

    normalized_value = raw_value.strip().lower()

    if normalized_value in {"true", "1", "yes", "y"}:
        return True

    if normalized_value in {"false", "0", "no", "n"}:
        return False

    raise ValueError(f"{name} must be true or false")


def _get_required_env(name: str) -> str:
    raw_value = os.getenv(name)

    if raw_value is None or raw_value.strip() == "":
        raise ValueError(
            f"{name} is required. "
            "Create .env from .env.example."
        )

    return raw_value


@dataclass(frozen=True)
class Settings:
    project_name: str
    env: str

    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str = field(repr=False)

    minio_endpoint: str
    minio_access_key: str = field(repr=False)
    minio_secret_key: str = field(repr=False)

    data_profile: str
    max_rows_per_table: int

    active_source: str

    sleep_edf_version: str
    sleep_edf_max_recordings: int
    sleep_edf_include_cassette: bool
    sleep_edf_include_telemetry: bool
    sleep_edf_include_metadata: bool

    def safe_dict(self) -> dict[str, str | int | bool]:
        return {
            "project_name": self.project_name,
            "env": self.env,
            "postgres_host": self.postgres_host,
            "postgres_port": self.postgres_port,
            "postgres_db": self.postgres_db,
            "postgres_user": self.postgres_user,
            "postgres_password": "***",
            "minio_endpoint": self.minio_endpoint,
            "minio_access_key": "***",
            "minio_secret_key": "***",
            "data_profile": self.data_profile,
            "max_rows_per_table": self.max_rows_per_table,
            "active_source": self.active_source,
            "sleep_edf_version": self.sleep_edf_version,
            "sleep_edf_max_recordings": (
                self.sleep_edf_max_recordings
            ),
            "sleep_edf_include_cassette": (
                self.sleep_edf_include_cassette
            ),
            "sleep_edf_include_telemetry": (
                self.sleep_edf_include_telemetry
            ),
            "sleep_edf_include_metadata": (
                self.sleep_edf_include_metadata
            ),
        }


def get_settings() -> Settings:
    data_profile = (
        os.getenv("DATA_PROFILE", "sample")
        .strip()
        .lower()
    )

    if data_profile not in {"sample", "full"}:
        raise ValueError(
            "DATA_PROFILE must be 'sample' or 'full'"
        )

    active_source = (
        os.getenv("ACTIVE_SOURCE", "sleep_edf")
        .strip()
        .lower()
    )

    return Settings(
        project_name=os.getenv(
            "PROJECT_NAME",
            "neuro_sleep_lakehouse_platform",
        ),
        env=os.getenv("ENV", "local"),
        postgres_host=os.getenv(
            "POSTGRES_HOST",
            "localhost",
        ),
        postgres_port=_get_int_env(
            "POSTGRES_PORT",
            5432,
        ),
        postgres_db=os.getenv(
            "POSTGRES_DB",
            "neuro_sleep",
        ),
        postgres_user=os.getenv(
            "POSTGRES_USER",
            "neuro_sleep",
        ),
        postgres_password=_get_required_env(
            "POSTGRES_PASSWORD"
        ),
        minio_endpoint=os.getenv(
            "MINIO_ENDPOINT",
            "http://localhost:9000",
        ),
        minio_access_key=_get_required_env(
            "MINIO_ACCESS_KEY"
        ),
        minio_secret_key=_get_required_env(
            "MINIO_SECRET_KEY"
        ),
        data_profile=data_profile,
        max_rows_per_table=_get_int_env(
            "MAX_ROWS_PER_TABLE",
            100000,
        ),
        active_source=active_source,
        sleep_edf_version=os.getenv(
            "SLEEP_EDF_VERSION",
            "1.0.0",
        ).strip(),
        sleep_edf_max_recordings=_get_int_env(
            "SLEEP_EDF_MAX_RECORDINGS",
            4,
        ),
        sleep_edf_include_cassette=_get_bool_env(
            "SLEEP_EDF_INCLUDE_CASSETTE",
            True,
        ),
        sleep_edf_include_telemetry=_get_bool_env(
            "SLEEP_EDF_INCLUDE_TELEMETRY",
            True,
        ),
        sleep_edf_include_metadata=_get_bool_env(
            "SLEEP_EDF_INCLUDE_METADATA",
            True,
        ),
    )


if __name__ == "__main__":
    settings = get_settings()
    print(settings.safe_dict())
