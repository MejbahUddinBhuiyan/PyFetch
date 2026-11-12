"""Tests for M4 concurrency and batch isolation."""

import asyncio
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from pyfetch.config import DownloadConfig
from pyfetch.engine import run_downloads
from pyfetch.exceptions import ValidationError
from pyfetch.models import DownloadJob, DownloadStatus


def make_job(
    url: str,
    destination: Path,
    filename: str,
) -> DownloadJob:
    """Create a download job for testing."""
    return DownloadJob(
        url=url,
        destination=destination,
        filename=filename,
    )


@pytest.mark.asyncio
async def test_downloads_overlap_with_limit(
    tmp_path: Path,
) -> None:
    active = 0
    maximum = 0
    arrivals = asyncio.Event()
    release = asyncio.Event()

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal active, maximum

        active += 1
        maximum = max(maximum, active)

        if active == 2:
            arrivals.set()

        try:
            await release.wait()
            return web.Response(body=b"data")
        finally:
            active -= 1

    app = web.Application()
    app.router.add_get("/{name}", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        jobs = [
            make_job(
                str(server.make_url(f"/file{i}")),
                tmp_path,
                f"file{i}.bin",
            )
            for i in range(3)
        ]

        task = asyncio.create_task(
            run_downloads(
                jobs,
                DownloadConfig(max_concurrency=2),
            )
        )

        await asyncio.wait_for(
            arrivals.wait(),
            timeout=3,
        )

        assert maximum == 2

        release.set()

        result = await asyncio.wait_for(
            task,
            timeout=3,
        )

    assert result.completed == 3
    assert result.failed == 0
    assert maximum == 2

    assert all(job.status is DownloadStatus.COMPLETED for job in jobs)

    assert all(job.file_path.read_bytes() == b"data" for job in jobs)


@pytest.mark.asyncio
async def test_one_failure_does_not_stop_others(
    tmp_path: Path,
) -> None:
    async def handler(
        request: web.Request,
    ) -> web.Response:
        if request.match_info["name"] == "bad":
            return web.Response(status=404)
        return web.Response(body=b"ok")

    app = web.Application()
    app.router.add_get("/{name}", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        jobs = [
            make_job(
                str(server.make_url("/ok1")),
                tmp_path,
                "ok1.txt",
            ),
            make_job(
                str(server.make_url("/bad")),
                tmp_path,
                "bad.txt",
            ),
            make_job(
                str(server.make_url("/ok2")),
                tmp_path,
                "ok2.txt",
            ),
        ]

        result = await run_downloads(
            jobs,
            DownloadConfig(max_concurrency=2),
        )

    assert result.completed == 2
    assert result.failed == 1

    assert jobs[1].status is DownloadStatus.FAILED
    assert not jobs[1].file_path.exists()

    assert jobs[0].file_path.read_bytes() == b"ok"
    assert jobs[2].file_path.read_bytes() == b"ok"


@pytest.mark.asyncio
async def test_duplicate_destinations_rejected_before_network(
    tmp_path: Path,
) -> None:
    jobs = [
        make_job(
            "https://example.com/a",
            tmp_path,
            "same.bin",
        ),
        make_job(
            "https://example.com/b",
            tmp_path,
            "same.bin",
        ),
    ]

    with pytest.raises(
        ValidationError,
        match="same destination",
    ):
        await run_downloads(
            jobs,
            DownloadConfig(),
        )

    assert all(job.status is DownloadStatus.QUEUED for job in jobs)

    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
async def test_empty_batch() -> None:
    result = await run_downloads(
        [],
        DownloadConfig(),
    )

    assert result.completed == 0
    assert result.failed == 0
    assert result.jobs == ()


@pytest.mark.asyncio
async def test_cancel_batch_cancels_active_and_waiting(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()

    async def handler(
        request: web.Request,
    ) -> web.Response:
        started.set()
        await asyncio.sleep(10)
        return web.Response(body=b"late")

    app = web.Application()
    app.router.add_get("/{name}", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        jobs = [
            make_job(
                str(server.make_url(f"/{i}")),
                tmp_path,
                f"{i}.bin",
            )
            for i in range(3)
        ]

        batch_task = asyncio.create_task(
            run_downloads(
                jobs,
                DownloadConfig(max_concurrency=1),
            )
        )

        await asyncio.wait_for(
            started.wait(),
            timeout=3,
        )

        batch_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await batch_task

    assert all(job.status is DownloadStatus.CANCELLED for job in jobs)

    assert await asyncio.to_thread(lambda: list(tmp_path.glob(".pyfetch-*.part"))) == []

    assert all(not job.file_path.exists() for job in jobs)
