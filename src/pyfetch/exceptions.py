"""Application-specific exceptions for PyFetch."""


class PyFetchError(Exception):
    """Base exception for PyFetch application errors."""


class ValidationError(PyFetchError, ValueError):
    """Raised when download metadata or settings are invalid."""


class ConfigError(PyFetchError):
    """Raised when configuration cannot be loaded or saved."""


class DownloadError(PyFetchError):
    """Base error for download operations."""


class DownloadHTTPError(DownloadError):
    """Raised when the server returns an unacceptable HTTP status."""


class DownloadNetworkError(DownloadError):
    """Raised when a download fails during network transfer."""


class DownloadFileError(DownloadError):
    """Raised when a download file cannot be written or published."""
