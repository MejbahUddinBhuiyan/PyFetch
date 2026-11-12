"""Tests for download job models and validation."""

from pathlib import Path

import pytest

from pyfetch.exceptions import ValidationError
from pyfetch.models import (
    DownloadJob,
    DownloadStatus,
    validate_filename,
    validate_sha256,
    validate_url,
)

TEST_URL = "https://example.com/data.zip"


def make_job(**overrides: object) -> DownloadJob:
    """Create a download job with test defaults."""
    values: dict[str, object] = {
        "url": TEST_URL,
        "destination": Path("downloads"),
        "filename": "data.zip",
    }
    values.update(overrides)
    return DownloadJob(**values)  # type: ignore[arg-type]


def test_default_job() -> None:
    job = make_job()

    assert job.status == DownloadStatus.QUEUED
    assert job.downloaded_bytes == 0
    assert job.total_bytes is None
    assert job.retry_count == 0
    assert job.progress_percentage is None
    assert job.file_path == Path("downloads/data.zip")
    assert not job.is_finished


def test_unique_download_ids() -> None:
    first = make_job()
    second = make_job()

    assert first.id != second.id


def test_progress_calculation() -> None:
    job = make_job(
        total_bytes=1000,
        downloaded_bytes=250,
    )

    assert job.progress_percentage == 25.0


def test_zero_byte_file_progress() -> None:
    job = make_job(total_bytes=0)

    assert job.progress_percentage == 100.0


def test_completed_job() -> None:
    job = make_job(status=DownloadStatus.COMPLETED)

    assert job.is_finished


@pytest.mark.parametrize(
    "status",
    [DownloadStatus.FAILED, DownloadStatus.CANCELLED],
)
def test_other_terminal_states(status: DownloadStatus) -> None:
    job = make_job(status=status)

    assert job.is_finished


@pytest.mark.parametrize(
    "url",
    [
        "",
        "not-a-url",
        "ftp://example.com/file.zip",
        "https:///file.zip",
        "https://user:password@example.com/file.zip",
        "https://example.com/file.zip#fragment",
        "https://example.com:99999/file.zip",
    ],
)
def test_invalid_urls(url: str) -> None:
    with pytest.raises(ValidationError):
        validate_url(url)


def test_valid_urls() -> None:
    assert validate_url(TEST_URL) == TEST_URL
    assert validate_url("http://example.com/file.txt") == "http://example.com/file.txt"


@pytest.mark.parametrize(
    "filename",
    [
        "",
        ".",
        "..",
        "../secret.txt",
        "folder/file.zip",
        r"folder\file.zip",
        "data?.zip",
        "file:name.zip",
        "CON",
        "NUL.txt",
        "file.",
        " file.zip",
        "file.zip ",
    ],
)
def test_unsafe_filenames(filename: str) -> None:
    with pytest.raises(ValidationError):
        validate_filename(filename)


def test_safe_filename() -> None:
    assert validate_filename("dataset_2026.zip") == "dataset_2026.zip"


def test_negative_downloaded_bytes() -> None:
    with pytest.raises(ValidationError):
        make_job(downloaded_bytes=-1)


def test_negative_total_bytes() -> None:
    with pytest.raises(ValidationError):
        make_job(total_bytes=-10)


def test_downloaded_exceeds_total() -> None:
    with pytest.raises(ValidationError):
        make_job(
            downloaded_bytes=200,
            total_bytes=100,
        )


def test_negative_retry_count() -> None:
    with pytest.raises(ValidationError):
        make_job(retry_count=-1)


def test_valid_sha256() -> None:
    checksum = "A" * 64

    assert validate_sha256(checksum) == "a" * 64


@pytest.mark.parametrize(
    "checksum",
    [
        "",
        "abc123",
        "z" * 64,
        "a" * 63,
        "a" * 65,
    ],
)
def test_invalid_sha256(checksum: str) -> None:
    with pytest.raises(ValidationError):
        validate_sha256(checksum)


def test_job_created_at_is_utc() -> None:
    job = make_job()

    assert job.created_at.tzinfo is not None
    assert job.created_at.utcoffset() is not None
    assert job.created_at.utcoffset().total_seconds() == 0
