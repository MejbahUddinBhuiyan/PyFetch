"""Type-safe models for download jobs."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from pyfetch.exceptions import ValidationError


class DownloadStatus(StrEnum):
    """Possible states of a download job."""

    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(UTC)


def validate_url(url: str) -> str:
    """Validate a supported HTTP or HTTPS download URL."""
    if not isinstance(url, str) or not url.strip():
        raise ValidationError("Download URL cannot be empty.")

    value = url.strip()

    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise ValidationError("Download URL is malformed.") from exc

    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValidationError("Only HTTP and HTTPS download URLs are supported.")

    if not hostname or any(char.isspace() for char in value):
        raise ValidationError("Download URL must contain a valid host.")

    if parsed.username is not None or parsed.password is not None:
        raise ValidationError(
            "Credentials embedded in download URLs are not supported."
        )

    if parsed.fragment:
        raise ValidationError("Download URLs must not contain fragments.")

    if port is not None and not 1 <= port <= 65535:
        raise ValidationError("Download URL contains an invalid port.")

    return value


def validate_filename(filename: str) -> str:
    """Validate a filename for safe cross-platform file creation."""
    if not isinstance(filename, str) or not filename:
        raise ValidationError("Filename cannot be empty.")

    if filename != filename.strip():
        raise ValidationError("Filename cannot have leading or trailing whitespace.")

    if filename in {".", ".."}:
        raise ValidationError("Filename cannot be '.' or '..'.")

    if filename.endswith("."):
        raise ValidationError("Filename cannot end with a period.")

    invalid_characters = '<>:"/\\|?*'

    if any(char in invalid_characters or ord(char) < 32 for char in filename):
        raise ValidationError("Filename contains invalid or unsafe characters.")

    reserved_names = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }

    stem = filename.split(".")[0].upper()

    if stem in reserved_names:
        raise ValidationError("Filename uses a reserved Windows device name.")

    return filename


def validate_sha256(value: str) -> str:
    """Validate a hexadecimal SHA-256 checksum."""
    normalized = value.strip().lower()

    if len(normalized) != 64 or any(
        char not in "0123456789abcdef" for char in normalized
    ):
        raise ValidationError("SHA-256 must contain exactly 64 hexadecimal characters.")

    return normalized


@dataclass(slots=True, kw_only=True)
class DownloadJob:
    """Represent one download and its current metadata."""

    url: str
    destination: Path
    filename: str

    id: str = field(default_factory=lambda: str(uuid4()))
    status: DownloadStatus = DownloadStatus.QUEUED

    downloaded_bytes: int = 0
    total_bytes: int | None = None

    retry_count: int = 0

    created_at: datetime = field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None

    error_message: str | None = None

    expected_sha256: str | None = None
    actual_sha256: str | None = None

    def __post_init__(self) -> None:
        """Validate the initial download metadata."""
        self.url = validate_url(self.url)
        self.filename = validate_filename(self.filename)

        if not isinstance(self.destination, Path):
            raise ValidationError("Destination must be a pathlib.Path object.")

        if not isinstance(self.status, DownloadStatus):
            raise ValidationError("Invalid download status.")

        if self.downloaded_bytes < 0:
            raise ValidationError("Downloaded bytes cannot be negative.")

        if self.total_bytes is not None:
            if self.total_bytes < 0:
                raise ValidationError("Total bytes cannot be negative.")

            if self.downloaded_bytes > self.total_bytes:
                raise ValidationError("Downloaded bytes cannot exceed total bytes.")

        if self.retry_count < 0:
            raise ValidationError("Retry count cannot be negative.")

        if self.expected_sha256 is not None:
            self.expected_sha256 = validate_sha256(self.expected_sha256)

        if self.actual_sha256 is not None:
            self.actual_sha256 = validate_sha256(self.actual_sha256)

    @property
    def file_path(self) -> Path:
        """Return the intended final download location."""
        return self.destination / self.filename

    @property
    def progress_percentage(self) -> float | None:
        """Return percentage only when total size is known."""
        if self.total_bytes is None:
            return None

        if self.total_bytes == 0:
            return 100.0

        return (self.downloaded_bytes / self.total_bytes) * 100.0

    @property
    def is_finished(self) -> bool:
        """Check whether the job is in a terminal state."""
        return self.status in {
            DownloadStatus.COMPLETED,
            DownloadStatus.FAILED,
            DownloadStatus.CANCELLED,
        }
