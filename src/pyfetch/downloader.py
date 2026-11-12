"""Single-file asynchronous streaming download implementation."""

import asyncio
import os
import tempfile
from collections.abc import Callable
from pathlib import Path

import aiofiles
import aiohttp

from pyfetch.config import DownloadConfig
from pyfetch.exceptions import (
    DownloadError,
    DownloadFileError,
    DownloadHTTPError,
    DownloadNetworkError,
)
from pyfetch.models import DownloadJob, DownloadStatus, utc_now

ProgressCallback = Callable[[DownloadJob], None]


async def _create_partial_file(destination: Path) -> Path:
    """Create a unique temporary file beside the target file."""
    await asyncio.to_thread(
        destination.mkdir,
        parents=True,
        exist_ok=True,
    )

    # This short synchronous operation avoids a cancellation
    # point between file creation and recording its path.
    fd, name = tempfile.mkstemp(
        prefix=".pyfetch-",
        suffix=".part",
        dir=destination,
    )

    os.close(fd)
    return Path(name)


async def download_file(
    job: DownloadJob,
    config: DownloadConfig,
    *,
    on_progress: ProgressCallback | None = None,
) -> DownloadJob:
    """Download one HTTP(S) file without overwriting existing files."""
    if job.status is not DownloadStatus.QUEUED:
        raise DownloadError("Only queued download jobs can be started.")

    partial_path: Path | None = None

    job.status = DownloadStatus.RUNNING
    job.started_at = utc_now()
    job.completed_at = None
    job.error_message = None
    job.downloaded_bytes = 0
    job.total_bytes = None

    try:
        if on_progress is not None:
            on_progress(job)

        partial_path = await _create_partial_file(job.destination)

        timeout = aiohttp.ClientTimeout(
            total=config.timeout_seconds,
            connect=config.connect_timeout_seconds,
        )

        async with aiohttp.ClientSession(
            timeout=timeout,
            auto_decompress=False,
        ) as session:
            async with session.get(job.url) as response:
                if response.status != 200:
                    raise DownloadHTTPError(
                        f"HTTP {response.status}: expected HTTP 200."
                    )

                job.total_bytes = response.content_length

                if on_progress is not None:
                    on_progress(job)

                async with aiofiles.open(partial_path, "wb") as output:
                    async for chunk in response.content.iter_chunked(config.chunk_size):
                        await output.write(chunk)
                        job.downloaded_bytes += len(chunk)

                        if on_progress is not None:
                            on_progress(job)

        if job.total_bytes is not None and job.downloaded_bytes != job.total_bytes:
            raise DownloadNetworkError(
                "Received byte count differs from Content-Length."
            )

        # Publish only after the transfer has completed.
        # os.link refuses to replace an existing final file.
        # This requires a filesystem that supports hard links.
        os.link(partial_path, job.file_path)

        job.status = DownloadStatus.COMPLETED
        job.completed_at = utc_now()

        return job

    except asyncio.CancelledError:
        job.status = DownloadStatus.CANCELLED
        job.error_message = "Download cancelled."
        raise

    except DownloadError as exc:
        job.status = DownloadStatus.FAILED
        job.error_message = str(exc)
        raise

    except FileExistsError as exc:
        job.status = DownloadStatus.FAILED
        job.error_message = "Destination file already exists; overwrite refused."
        raise DownloadFileError(job.error_message) from exc

    except (aiohttp.ClientError, TimeoutError) as exc:
        job.status = DownloadStatus.FAILED
        job.error_message = "Network connection or HTTP transfer failed."
        raise DownloadNetworkError(job.error_message) from exc

    except OSError as exc:
        job.status = DownloadStatus.FAILED
        job.error_message = "Could not write or publish the download file."
        raise DownloadFileError(job.error_message) from exc

    finally:
        try:
            if partial_path is not None:
                partial_path.unlink(missing_ok=True)
        finally:
            if on_progress is not None:
                on_progress(job)
