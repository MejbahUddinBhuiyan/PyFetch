"""Application-specific exceptions for PyFetch."""


class PyFetchError(Exception):
    """Base exception for PyFetch application errors."""


class ValidationError(PyFetchError, ValueError):
    """Raised when download metadata or settings are invalid."""


class ConfigError(PyFetchError):
    """Raised when configuration cannot be loaded or saved."""
