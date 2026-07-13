class NeuroSleepError(Exception):
    """Base exception for project-specific failures."""


class RetryablePipelineError(NeuroSleepError):
    """A temporary failure that may succeed after retry."""


class PermanentPipelineError(NeuroSleepError):
    """A failure that should not be retried automatically."""


class SourceNetworkError(RetryablePipelineError):
    """Temporary source HTTP or network failure."""


class ObjectStorageTransientError(
    RetryablePipelineError
):
    """Temporary MinIO or S3-compatible storage failure."""


class DatabaseTransientError(
    RetryablePipelineError
):
    """Temporary PostgreSQL connection failure."""


class ChecksumMismatchError(
    PermanentPipelineError
):
    """Downloaded content does not match the source checksum."""


class InvalidConfigurationError(
    PermanentPipelineError
):
    """Project configuration is invalid."""


class ConcurrentPipelineRunError(
    PermanentPipelineError
):
    """Another copy of the same pipeline is already active."""

class SourceHttpError(
    PermanentPipelineError
):
    """Permanent HTTP failure such as 404."""


class SourceContentError(
    PermanentPipelineError
):
    """Source returned structurally invalid content."""
