"""Integration tests for the persistent download queue."""

import asyncio
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from typer.testing import CliRunner

from pyfetch.cli import app as cli_app
from pyfetch.config import DownloadConfig
from pyfetch.exceptions import QueueLockedError
from pyfetch.models import DownloadJob, DownloadStatus
from pyfetch.queue_runner import (
    execute_persistent_jobs,
    prepare_queue,
    queue_lock,
)
from pyfetch.storage import DownloadRepository, JobSnapshot


def make_job(
    url: str,
    destination: Path,
    filename: str,
) -> DownloadJob:
    return DownloadJob(
        url=url,
        destination=destination,
        filename=filename,
    )


def test_new_queue_commands_in_help() -> None:
    result = CliRunner().invoke(cli_app, ["--help"])

    assert result.exit_code == 0

    for command in ("add", "list", "start", "history"):
        assert command in result.stdout


def test_queue_lock_prevents_second_runner(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobs.db"

    with queue_lock(path):
        with pytest.raises(QueueLockedError):
            with queue_lock(path):
                pass

    # The lock must be released after leaving the context.
    with queue_lock(path):
        pass


def test_prepare_queue_recovers_running_jobs(
    tmp_path: Path,
) -> None:
    repository = DownloadRepository(tmp_path / "jobs.db")

    job = make_job(
        "https://example.com/file",
        tmp_path,
        "file.bin",
    )

    repository.add_job(job)

    job.status = DownloadStatus.RUNNING
    repository.save_snapshot(JobSnapshot.from_job(job))

    recovered, queued = prepare_queue(repository)

    assert recovered == 1
    assert len(queued) == 1
    assert queued[0].status is DownloadStatus.QUEUED


@pytest.mark.asyncio
async def test_successful_queue_execution_persists(
    tmp_path: Path,
) -> None:
    data = b"persistent-download" * 100

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=data)

    http_app = web.Application()
    http_app.router.add_get("/file", handler)

    async with TestServer(
        http_app,
        host="127.0.0.1",
    ) as server:
        repository = DownloadRepository(tmp_path / "jobs.db")

        job = make_job(
            str(server.make_url("/file")),
            tmp_path / "downloads",
            "file.bin",
        )

        repository.add_job(job)
        recovered, queued = prepare_queue(repository)

        assert recovered == 0

        result = await execute_persistent_jobs(
            queued,
            repository,
            DownloadConfig(chunk_size=128),
        )

    restored = repository.get_job(job.id)

    assert result.completed == 1
    assert restored is not None
    assert restored.status is DownloadStatus.COMPLETED
    assert restored.downloaded_bytes == len(data)
    assert restored.total_bytes == len(data)
    assert restored.started_at is not None
    assert restored.completed_at is not None

    assert await asyncio.to_thread(job.file_path.read_bytes) == data


@pytest.mark.asyncio
async def test_failed_job_is_saved_in_history(
    tmp_path: Path,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(status=404)

    http_app = web.Application()
    http_app.router.add_get("/missing", handler)

    async with TestServer(
        http_app,
        host="127.0.0.1",
    ) as server:
        repository = DownloadRepository(tmp_path / "jobs.db")

        job = make_job(
            str(server.make_url("/missing")),
            tmp_path / "downloads",
            "missing.bin",
        )

        repository.add_job(job)

        _, queued = prepare_queue(repository)

        result = await execute_persistent_jobs(
            queued,
            repository,
            DownloadConfig(max_retries=0),
        )

    restored = repository.get_job(job.id)

    assert result.failed == 1
    assert restored is not None
    assert restored.status is DownloadStatus.FAILED
    assert restored.error_message is not None
    assert "404" in restored.error_message

    history = repository.list_jobs((DownloadStatus.FAILED,))
    assert len(history) == 1


@pytest.mark.asyncio
async def test_cancelled_queue_persists_status(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()

    async def handler(
        request: web.Request,
    ) -> web.Response:
        started.set()
        await asyncio.sleep(10)
        return web.Response(body=b"late")

    http_app = web.Application()
    http_app.router.add_get("/slow", handler)

    async with TestServer(
        http_app,
        host="127.0.0.1",
    ) as server:
        repository = DownloadRepository(tmp_path / "jobs.db")

        job = make_job(
            str(server.make_url("/slow")),
            tmp_path / "downloads",
            "slow.bin",
        )

        repository.add_job(job)
        _, queued = prepare_queue(repository)

        task = asyncio.create_task(
            execute_persistent_jobs(
                queued,
                repository,
                DownloadConfig(),
            )
        )

        await asyncio.wait_for(
            started.wait(),
            timeout=3,
        )

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    restored = repository.get_job(job.id)

    assert restored is not None
    assert restored.status is DownloadStatus.CANCELLED
    assert not await asyncio.to_thread(job.file_path.exists)
