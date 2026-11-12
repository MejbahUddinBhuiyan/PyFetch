"""M6 tests for safe retries and exponential backoff."""

import asyncio
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from pyfetch.config import DownloadConfig
from pyfetch.engine import run_downloads
from pyfetch.exceptions import (
    DownloadFileError,
    DownloadHTTPError,
    DownloadNetworkError,
)
from pyfetch.models import DownloadJob, DownloadStatus
from pyfetch.retry import (
    calculate_backoff,
    download_with_retries,
    is_retryable,
)


def make_job(
    url: str,
    destination: Path,
    filename: str = "result.bin",
) -> DownloadJob:
    """Create a valid download job."""
    return DownloadJob(
        url=url,
        destination=destination,
        filename=filename,
    )


def fast_config(
    *,
    retries: int = 3,
    timeout: float = 2.0,
) -> DownloadConfig:
    """Use short retry delays for predictable local tests."""
    return DownloadConfig(
        max_retries=retries,
        retry_base_delay=0.01,
        retry_max_delay=0.04,
        timeout_seconds=timeout,
        connect_timeout_seconds=min(timeout, 1.0),
        chunk_size=256,
    )


def assert_no_partials(destination: Path) -> None:
    """Check that temporary download files were cleaned up."""
    assert list(destination.glob(".pyfetch-*.part")) == []


@pytest.mark.parametrize(
    ("retry_number", "expected_delay"),
    [
        (1, 1.0),
        (2, 2.0),
        (3, 4.0),
        (4, 5.0),
    ],
)
def test_exponential_backoff(
    retry_number: int,
    expected_delay: float,
) -> None:
    config = DownloadConfig(
        retry_base_delay=1.0,
        retry_max_delay=5.0,
    )

    assert (
        calculate_backoff(
            retry_number,
            config,
        )
        == expected_delay
    )


def test_invalid_retry_number() -> None:
    with pytest.raises(
        ValueError,
        match="retry_number",
    ):
        calculate_backoff(
            0,
            DownloadConfig(),
        )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (408, True),
        (425, True),
        (429, True),
        (500, True),
        (503, True),
        (599, True),
        (400, False),
        (403, False),
        (404, False),
    ],
)
def test_http_error_classification(
    status: int,
    expected: bool,
) -> None:
    error = DownloadHTTPError(
        f"HTTP {status}",
        status_code=status,
    )

    assert is_retryable(error) is expected


@pytest.mark.parametrize(
    ("retryable", "expected"),
    [
        (True, True),
        (False, False),
    ],
)
def test_network_error_classification(
    retryable: bool,
    expected: bool,
) -> None:
    error = DownloadNetworkError(
        "Network error",
        retryable=retryable,
    )

    assert is_retryable(error) is expected


def test_file_error_is_permanent() -> None:
    assert not is_retryable(DownloadFileError("Disk write failed."))


@pytest.mark.asyncio
async def test_transient_503_eventually_succeeds(
    tmp_path: Path,
) -> None:
    requests = 0

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal requests
        requests += 1

        if requests <= 2:
            return web.Response(status=503)

        return web.Response(body=b"final-success")

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        result = await download_with_retries(
            job,
            fast_config(),
        )

    assert result is job
    assert requests == 3
    assert job.retry_count == 2
    assert job.status is DownloadStatus.COMPLETED
    assert job.file_path.read_bytes() == b"final-success"
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_http_404_is_not_retried(
    tmp_path: Path,
) -> None:
    requests = 0

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal requests
        requests += 1
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
        )

        with pytest.raises(DownloadHTTPError):
            await download_with_retries(
                job,
                fast_config(),
            )

    assert requests == 1
    assert job.retry_count == 0
    assert job.status is DownloadStatus.FAILED
    assert not job.file_path.exists()
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_retry_limit_is_respected(
    tmp_path: Path,
) -> None:
    requests = 0

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal requests
        requests += 1
        return web.Response(status=503)

    app = web.Application()
    app.router.add_get("/unavailable", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/unavailable")),
            tmp_path,
        )

        with pytest.raises(
            DownloadHTTPError,
            match="503",
        ):
            await download_with_retries(
                job,
                fast_config(retries=2),
            )

    assert requests == 3
    assert job.retry_count == 2
    assert job.status is DownloadStatus.FAILED
    assert not job.file_path.exists()
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_http_429_is_retried(
    tmp_path: Path,
) -> None:
    requests = 0

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal requests
        requests += 1

        if requests == 1:
            return web.Response(status=429)

        return web.Response(body=b"accepted")

    app = web.Application()
    app.router.add_get("/limited", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/limited")),
            tmp_path,
        )

        await download_with_retries(
            job,
            fast_config(),
        )

    assert requests == 2
    assert job.retry_count == 1
    assert job.status is DownloadStatus.COMPLETED
    assert job.file_path.read_bytes() == b"accepted"


@pytest.mark.asyncio
async def test_zero_retries_means_one_attempt(
    tmp_path: Path,
) -> None:
    requests = 0

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal requests
        requests += 1
        return web.Response(status=503)

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        with pytest.raises(DownloadHTTPError):
            await download_with_retries(
                job,
                fast_config(retries=0),
            )

    assert requests == 1
    assert job.retry_count == 0


@pytest.mark.asyncio
async def test_timeout_then_success(
    tmp_path: Path,
) -> None:
    requests = 0

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal requests
        requests += 1

        if requests == 1:
            await asyncio.sleep(0.15)

        return web.Response(body=b"recovered")

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        await download_with_retries(
            job,
            fast_config(
                retries=2,
                timeout=0.05,
            ),
        )

    assert requests == 2
    assert job.retry_count == 1
    assert job.status is DownloadStatus.COMPLETED
    assert job.file_path.read_bytes() == b"recovered"
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_existing_destination_is_not_retried(
    tmp_path: Path,
) -> None:
    target = tmp_path / "result.bin"
    target.write_bytes(b"original")

    requests = 0

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal requests
        requests += 1
        return web.Response(body=b"replacement")

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        with pytest.raises(DownloadFileError):
            await download_with_retries(
                job,
                fast_config(),
            )

    assert requests == 1
    assert job.retry_count == 0
    assert job.status is DownloadStatus.FAILED
    assert target.read_bytes() == b"original"
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_cancellation_during_backoff(
    tmp_path: Path,
) -> None:
    retry_scheduled = asyncio.Event()
    events: list[DownloadStatus] = []
    requests = 0

    async def handler(
        request: web.Request,
    ) -> web.Response:
        nonlocal requests
        requests += 1
        return web.Response(status=503)

    app = web.Application()
    app.router.add_get("/file", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        job = make_job(
            str(server.make_url("/file")),
            tmp_path,
        )

        def collect(current: DownloadJob) -> None:
            events.append(current.status)

            if current.status is DownloadStatus.QUEUED and current.retry_count == 1:
                retry_scheduled.set()

        config = DownloadConfig(
            max_retries=3,
            retry_base_delay=10.0,
            retry_max_delay=10.0,
        )

        task = asyncio.create_task(
            download_with_retries(
                job,
                config,
                on_progress=collect,
            )
        )

        await asyncio.wait_for(
            retry_scheduled.wait(),
            timeout=3,
        )

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert requests == 1
    assert job.retry_count == 1
    assert job.status is DownloadStatus.CANCELLED
    assert events[-1] is DownloadStatus.CANCELLED
    assert not job.file_path.exists()
    assert_no_partials(tmp_path)


@pytest.mark.asyncio
async def test_batch_retries_are_independent(
    tmp_path: Path,
) -> None:
    requests: dict[str, int] = {}

    async def handler(
        request: web.Request,
    ) -> web.Response:
        name = request.match_info["name"]
        requests[name] = requests.get(name, 0) + 1

        if name == "flaky" and requests[name] == 1:
            return web.Response(status=503)

        if name == "missing":
            return web.Response(status=404)

        return web.Response(body=name.encode("utf-8"))

    app = web.Application()
    app.router.add_get("/{name}", handler)

    async with TestServer(
        app,
        host="127.0.0.1",
    ) as server:
        jobs = [
            make_job(
                str(server.make_url("/flaky")),
                tmp_path,
                "flaky.bin",
            ),
            make_job(
                str(server.make_url("/normal")),
                tmp_path,
                "normal.bin",
            ),
            make_job(
                str(server.make_url("/missing")),
                tmp_path,
                "missing.bin",
            ),
        ]

        result = await run_downloads(
            jobs,
            fast_config(),
        )

    assert result.completed == 2
    assert result.failed == 1
    assert result.cancelled == 0

    assert requests == {
        "flaky": 2,
        "normal": 1,
        "missing": 1,
    }

    assert jobs[0].retry_count == 1
    assert jobs[1].retry_count == 0
    assert jobs[2].retry_count == 0

    assert jobs[0].file_path.read_bytes() == b"flaky"
    assert jobs[1].file_path.read_bytes() == b"normal"
    assert not jobs[2].file_path.exists()
    assert_no_partials(tmp_path)
