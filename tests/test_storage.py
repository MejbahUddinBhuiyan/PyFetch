"""Tests for persistent SQLite download metadata."""

from pathlib import Path

import pytest

from pyfetch.exceptions import StorageError
from pyfetch.models import DownloadJob, DownloadStatus, utc_now
from pyfetch.storage import DownloadRepository, JobSnapshot


def make_job(filename: str = "file.bin") -> DownloadJob:
    """Create a download job for storage tests."""
    return DownloadJob(
        url="https://example.com/file.bin",
        destination=Path("downloads"),
        filename=filename,
    )


def test_database_created(tmp_path: Path) -> None:
    path = tmp_path / "pyfetch.db"

    DownloadRepository(path)

    assert path.exists()


def test_add_and_load_job(tmp_path: Path) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")
    job = make_job()

    repository.add_job(job)
    restored = repository.get_job(job.id)

    assert restored is not None
    assert restored.id == job.id
    assert restored.url == job.url
    assert restored.filename == job.filename
    assert restored.status is DownloadStatus.QUEUED


def test_downloads_survive_repository_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobs.db"
    first = DownloadRepository(path)

    job = make_job()
    first.add_job(job)

    second = DownloadRepository(path)
    restored = second.get_job(job.id)

    assert restored is not None
    assert restored.id == job.id


def test_duplicate_job_id_is_rejected(
    tmp_path: Path,
) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")
    job = make_job()

    repository.add_job(job)

    with pytest.raises(StorageError):
        repository.add_job(job)

    assert len(repository.list_jobs()) == 1


def test_list_jobs_by_status(tmp_path: Path) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")

    first = make_job("first.bin")
    second = make_job("second.bin")

    repository.add_job(first)
    repository.add_job(second)

    second.status = DownloadStatus.COMPLETED
    second.completed_at = utc_now()

    repository.save_snapshot(JobSnapshot.from_job(second))

    queued = repository.list_jobs((DownloadStatus.QUEUED,))
    completed = repository.list_jobs((DownloadStatus.COMPLETED,))

    assert len(queued) == 1
    assert queued[0].id == first.id

    assert len(completed) == 1
    assert completed[0].id == second.id


def test_progress_snapshot_persists(tmp_path: Path) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")
    job = make_job()

    repository.add_job(job)

    job.status = DownloadStatus.RUNNING
    job.started_at = utc_now()
    job.downloaded_bytes = 250
    job.total_bytes = 1000
    job.retry_count = 1

    repository.save_snapshot(JobSnapshot.from_job(job))

    restored = repository.get_job(job.id)

    assert restored is not None
    assert restored.status is DownloadStatus.RUNNING
    assert restored.downloaded_bytes == 250
    assert restored.total_bytes == 1000
    assert restored.retry_count == 1
    assert restored.started_at is not None


def test_recovery_resets_interrupted_jobs(
    tmp_path: Path,
) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")
    job = make_job()

    repository.add_job(job)

    job.status = DownloadStatus.RUNNING
    job.started_at = utc_now()
    job.downloaded_bytes = 500
    job.total_bytes = 1000

    repository.save_snapshot(JobSnapshot.from_job(job))

    recovered = repository.recover_interrupted()

    restored = repository.get_job(job.id)

    assert recovered == 1
    assert restored is not None
    assert restored.status is DownloadStatus.QUEUED
    assert restored.downloaded_bytes == 0
    assert restored.total_bytes is None
    assert restored.started_at is None
    assert restored.error_message == "Recovered after interruption"


def test_completed_job_is_not_recovered(
    tmp_path: Path,
) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")
    job = make_job()

    repository.add_job(job)

    job.status = DownloadStatus.COMPLETED
    job.completed_at = utc_now()

    repository.save_snapshot(JobSnapshot.from_job(job))

    assert repository.recover_interrupted() == 0

    restored = repository.get_job(job.id)

    assert restored is not None
    assert restored.status is DownloadStatus.COMPLETED


def test_unknown_job_returns_none(tmp_path: Path) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")

    assert repository.get_job("does-not-exist") is None


def test_unknown_snapshot_is_rejected(
    tmp_path: Path,
) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")
    job = make_job()

    with pytest.raises(StorageError):
        repository.save_snapshot(JobSnapshot.from_job(job))


def test_empty_status_filter(tmp_path: Path) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")

    assert repository.list_jobs(()) == []
