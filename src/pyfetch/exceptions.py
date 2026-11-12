"""Application-specific exceptions for PyFetch."""


class PyFetchError(Exception):
    """Base exception for PyFetch application errors."""


class ValidationError(PyFetchError, ValueError):
    """Raised when download metadata or settings are invalid."""


class ConfigError(PyFetchError):
    """Raised when configuration cannot be loaded or saved."""


class DownloadError(PyFetchError):
    """Base exception for download operations."""


class DownloadHTTPError(DownloadError):
    """Raised for an unacceptable HTTP response status."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code


class DownloadNetworkError(DownloadError):
    """Raised when a download fails during network transfer."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable


class DownloadFileError(DownloadError):
    """Raised when a download file cannot be written or published."""
