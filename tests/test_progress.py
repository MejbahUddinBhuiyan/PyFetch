"""Tests for the M5 Rich progress reporter."""

from io import StringIO
from pathlib import Path

import pytest
from rich.console import Console

from pyfetch.models import DownloadJob, DownloadStatus
from pyfetch.progress import (
    DownloadProgress,
    KnownEtaColumn,
    KnownPercentageColumn,
)


def make_job(
    filename: str,
    job_id: str,
) -> DownloadJob:
    return DownloadJob(
        id=job_id,
        url="https://example.com/file",
        filename=filename,
        destination=Path("downloads"),
    )


def test_unknown_size_has_no_fake_percentage_or_eta() -> None:
    job = make_job("unknown.bin", "j1")
    output = StringIO()

    with DownloadProgress(
        [job],
        console=Console(file=output, width=110),
    ) as reporter:
        job.status = DownloadStatus.RUNNING
        job.downloaded_bytes = 128
        reporter.update(job)

        task = reporter.progress.tasks[0]

        assert task.total is None
        assert task.completed == 128

        assert KnownPercentageColumn().render(task).plain == "--"
        assert KnownEtaColumn().render(task).plain == "ETA --"


def test_known_size_and_terminal_state() -> None:
    job = make_job("known.bin", "j2")

    with DownloadProgress(
        [job],
        console=Console(file=StringIO()),
    ) as reporter:
        job.status = DownloadStatus.RUNNING
        job.total_bytes = 100
        job.downloaded_bytes = 50
        reporter.update(job)

        task = reporter.progress.tasks[0]

        assert task.total == 100
        assert task.completed == 50
        assert KnownPercentageColumn().render(task).plain == "50%"

        job.downloaded_bytes = 100
        job.status = DownloadStatus.COMPLETED
        reporter.update(job)

        assert task.completed == 100
        assert task.fields["status"] == "completed"
        assert task.finished


def test_throttling_does_not_hide_final_status() -> None:
    job = make_job("small.bin", "j3")

    with DownloadProgress(
        [job],
        console=Console(file=StringIO()),
        update_interval=3600,
    ) as reporter:
        job.status = DownloadStatus.RUNNING
        reporter.update(job)

        job.downloaded_bytes = 12
        reporter.update(job)

        assert reporter.progress.tasks[0].completed == 0

        job.status = DownloadStatus.CANCELLED
        reporter.update(job)

        task = reporter.progress.tasks[0]
        assert task.completed == 12
        assert task.fields["status"] == "cancelled"


def test_parallel_jobs_have_independent_progress() -> None:
    jobs = [
        make_job("a.bin", "a"),
        make_job("b.bin", "b"),
    ]

    with DownloadProgress(
        jobs,
        console=Console(file=StringIO()),
    ) as reporter:
        jobs[0].status = DownloadStatus.RUNNING
        jobs[0].downloaded_bytes = 10
        reporter.update(jobs[0])

        jobs[1].status = DownloadStatus.RUNNING
        jobs[1].downloaded_bytes = 20
        reporter.update(jobs[1])

        assert [task.completed for task in reporter.progress.tasks] == [10, 20]


def test_duplicate_job_ids_are_rejected() -> None:
    jobs = [
        make_job("a.bin", "same"),
        make_job("b.bin", "same"),
    ]

    with pytest.raises(
        ValueError,
        match="Duplicate download job ID",
    ):
        DownloadProgress(jobs)


def test_negative_refresh_interval_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="update_interval",
    ):
        DownloadProgress(
            [],
            update_interval=-0.5,
        )


def test_eta_uses_measured_speed() -> None:
    """ETA should use Rich's actual byte-rate estimate."""
    from rich.progress import Progress

    clock = [0.0]

    progress = Progress(
        get_time=lambda: clock[0],
        auto_refresh=False,
    )

    task_id = progress.add_task(
        "test",
        total=100,
        completed=0,
    )

    clock[0] = 1.0
    progress.update(task_id, advance=10)

    clock[0] = 2.0
    progress.update(task_id, advance=10)

    task = progress.tasks[0]

    assert task.completed == 20
    assert task.speed == pytest.approx(10.0)
    assert KnownEtaColumn().render(task).plain == "ETA 0:08"
