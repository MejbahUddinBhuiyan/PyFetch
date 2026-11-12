"""Local HTTP tests for streamed file downloads."""

import asyncio
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from pyfetch.config import DownloadConfig
from pyfetch.downloader import download_file
from pyfetch.exceptions import (
    DownloadError,
    DownloadFileError,
    DownloadHTTPError,
    DownloadNetworkError,
)
from pyfetch.models import DownloadJob, DownloadStatus


def make_job(base_url: str, destination: Path) -> DownloadJob:
    """Create a test download job."""
    return DownloadJob(
        url=base_url,
        destination=destination,
        filename="file.bin",
    )


def assert_no_partials(destination: Path) -> None:
    """Verify that no PyFetch partial files remain."""
    assert list(destination.glob(".pyfetch-*.part")) == []


@pytest.mark.asyncio
async def test_successful_download(tmp_path: Path) -> None:
    data = b"abc123" * 10000

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=data)

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path / "downloads",
        )

        result = await download_file(
            job,
            DownloadConfig(chunk_size=1024),
        )

    assert result is job
    assert job.status == DownloadStatus.COMPLETED
    assert job.downloaded_bytes == len(data)
    assert job.total_bytes == len(data)
    assert job.started_at is not None
    assert job.completed_at is not None
    assert job.file_path.read_bytes() == data
    assert_no_partials(job.destination)


@pytest.mark.asyncio
async def test_missing_content_length(tmp_path: Path) -> None:
    data = b"streamed-without-length"

    async def handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(status=200)
        await response.prepare(request)
        await response.write(data)
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        await download_file(job, DownloadConfig())

    assert job.status == DownloadStatus.COMPLETED
    assert job.total_bytes is None
    assert job.downloaded_bytes == len(data)
    assert job.file_path.read_bytes() == data


@pytest.mark.asyncio
async def test_http_404(tmp_path: Path) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(status=404)

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        with pytest.raises(DownloadHTTPError, match="404"):
            await download_file(job, DownloadConfig())

    assert job.status == DownloadStatus.FAILED
    assert not job.file_path.exists()
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_existing_file_remains_unchanged(
    tmp_path: Path,
) -> None:
    target = tmp_path / "file.bin"
    target.write_bytes(b"original")

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"replacement")

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        with pytest.raises(
            DownloadFileError,
            match="already exists",
        ):
            await download_file(job, DownloadConfig())

    assert job.status == DownloadStatus.FAILED
    assert target.read_bytes() == b"original"
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_timeout(tmp_path: Path) -> None:
    async def handler(request: web.Request) -> web.Response:
        await asyncio.sleep(0.2)
        return web.Response(body=b"late")

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        config = DownloadConfig(
            timeout_seconds=0.05,
            connect_timeout_seconds=0.05,
        )

        with pytest.raises(DownloadNetworkError):
            await download_file(job, config)

    assert job.status == DownloadStatus.FAILED
    assert not job.file_path.exists()
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_cancellation_cleans_partial(
    tmp_path: Path,
) -> None:
    first_chunk_sent = asyncio.Event()

    async def handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(status=200)
        await response.prepare(request)
        await response.write(b"initial")
        first_chunk_sent.set()
        await asyncio.sleep(0.5)
        return response

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        task = asyncio.create_task(download_file(job, DownloadConfig()))

        await asyncio.wait_for(
            first_chunk_sent.wait(),
            timeout=2,
        )
        await asyncio.sleep(0.02)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert job.status == DownloadStatus.CANCELLED
    assert not job.file_path.exists()
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_206_partial_response_rejected(
    tmp_path: Path,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(
            status=206,
            body=b"part",
        )

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(app, host="127.0.0.1") as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        with pytest.raises(DownloadHTTPError, match="206"):
            await download_file(job, DownloadConfig())

    assert not job.file_path.exists()
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_invalid_destination_file(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "not-a-directory"
    destination.write_text("occupied")

    job = make_job(
        "https://example.com/file",
        destination,
    )

    with pytest.raises(DownloadFileError):
        await download_file(job, DownloadConfig())

    assert job.status == DownloadStatus.FAILED
    assert destination.read_text() == "occupied"


@pytest.mark.asyncio
async def test_nonqueued_job_rejected(
    tmp_path: Path,
) -> None:
    job = make_job(
        "https://example.com/file",
        tmp_path,
    )
    job.status = DownloadStatus.COMPLETED

    with pytest.raises(
        DownloadError,
        match="Only queued",
    ):
        await download_file(job, DownloadConfig())

    assert job.status == DownloadStatus.COMPLETED
