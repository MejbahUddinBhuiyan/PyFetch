"""Integration tests for M5 progress callbacks."""

import asyncio
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from pyfetch.config import DownloadConfig
from pyfetch.downloader import download_file
from pyfetch.engine import run_downloads
from pyfetch.models import DownloadJob, DownloadStatus


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


@pytest.mark.asyncio
async def test_download_emits_progress_updates(
    tmp_path: Path,
) -> None:
    data = b"abc123" * 3000
    events: list[tuple[DownloadStatus, int, int | None]] = []

    async def handler(
        request: web.Request,
    ) -> web.Response:
        return web.Response(body=data)

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
            "file.bin",
        )

        await download_file(
            job,
            DownloadConfig(chunk_size=256),
            on_progress=lambda current: events.append(
                (
                    current.status,
                    current.downloaded_bytes,
                    current.total_bytes,
                )
            ),
        )

    assert events
    assert events[0][0] is DownloadStatus.RUNNING
    assert events[-1] == (
        DownloadStatus.COMPLETED,
        len(data),
        len(data),
    )

    assert any(
        status is DownloadStatus.RUNNING and 0 < count < len(data)
        for status, count, _ in events
    )


@pytest.mark.asyncio
async def test_unknown_length_progress(
    tmp_path: Path,
) -> None:
    events: list[tuple[int, int | None]] = []

    async def handler(
        request: web.Request,
    ) -> web.StreamResponse:
        response = web.StreamResponse(status=200)
        await response.prepare(request)
        await response.write(b"unknown-size-data")
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
            "unknown.bin",
        )

        await download_file(
            job,
            DownloadConfig(),
            on_progress=lambda current: events.append(
                (
                    current.downloaded_bytes,
                    current.total_bytes,
                )
            ),
        )

    assert job.status is DownloadStatus.COMPLETED
    assert events[-1] == (len(b"unknown-size-data"), None)


@pytest.mark.asyncio
async def test_concurrent_jobs_emit_separate_progress(
    tmp_path: Path,
) -> None:
    events: dict[str, list[DownloadStatus]] = {}

    async def handler(
        request: web.Request,
    ) -> web.Response:
        return web.Response(body=b"data" * 2000)

    app = web.Application()
    app.router.add_get("/{name}", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        jobs = [
            make_job(
                str(server.make_url(f"/{index}")),
                tmp_path,
                f"file{index}.bin",
            )
            for index in range(3)
        ]

        def collect(job: DownloadJob) -> None:
            events.setdefault(job.id, []).append(job.status)

        result = await run_downloads(
            jobs,
            DownloadConfig(max_concurrency=2),
            on_progress=collect,
        )

    assert result.completed == 3
    assert len(events) == 3

    for job in jobs:
        assert DownloadStatus.RUNNING in events[job.id]
        assert events[job.id][-1] is DownloadStatus.COMPLETED


@pytest.mark.asyncio
async def test_cancellation_reports_cancelled_status(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    events: list[DownloadStatus] = []

    async def handler(
        request: web.Request,
    ) -> web.Response:
        started.set()
        await asyncio.sleep(2)
        return web.Response(body=b"late")

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
            "cancelled.bin",
        )

        task = asyncio.create_task(
            download_file(
                job,
                DownloadConfig(),
                on_progress=lambda current: events.append(current.status),
            )
        )

        await asyncio.wait_for(
            started.wait(),
            timeout=3,
        )

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert job.status is DownloadStatus.CANCELLED
    assert events[-1] is DownloadStatus.CANCELLED
    assert not job.file_path.exists()


@pytest.mark.asyncio
async def test_http_failure_reports_failed_status(
    tmp_path: Path,
) -> None:
    from pyfetch.exceptions import DownloadHTTPError

    events: list[DownloadStatus] = []

    async def handler(
        request: web.Request,
    ) -> web.Response:
        return web.Response(status=404)

    app = web.Application()
    app.router.add_get("/missing", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/missing")),
            tmp_path,
            "missing.bin",
        )

        with pytest.raises(DownloadHTTPError):
            await download_file(
                job,
                DownloadConfig(),
                on_progress=lambda current: events.append(current.status),
            )

    assert events[-1] is DownloadStatus.FAILED
    assert not job.file_path.exists()
