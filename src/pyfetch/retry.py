"""Retry orchestration with bounded exponential backoff."""

import asyncio

from pyfetch.config import DownloadConfig
from pyfetch.downloader import ProgressCallback, download_file
from pyfetch.exceptions import (
    DownloadError,
    DownloadHTTPError,
    DownloadNetworkError,
)
from pyfetch.models import DownloadJob, DownloadStatus

TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429})


def is_retryable(error: DownloadError) -> bool:
    """Classify a download error as retryable or permanent."""
    if isinstance(error, DownloadHTTPError):
        status = error.status_code

        if status is None:
            return False

        return status in TRANSIENT_HTTP_STATUSES or 500 <= status <= 599

    if isinstance(error, DownloadNetworkError):
        return error.retryable

    return False


def calculate_backoff(
    retry_number: int,
    config: DownloadConfig,
) -> float:
    """Calculate capped exponential backoff.

    Retry 1 uses the base delay.
    Retry 2 doubles it.
    Later delays never exceed retry_max_delay.
    """
    if retry_number < 1:
        raise ValueError("retry_number must be at least 1.")

    delay = min(
        config.retry_base_delay,
        config.retry_max_delay,
    )

    for _ in range(retry_number - 1):
        delay = min(
            delay * 2,
            config.retry_max_delay,
        )

        if delay >= config.retry_max_delay:
            break

    return delay


async def download_with_retries(
    job: DownloadJob,
    config: DownloadConfig,
    *,
    on_progress: ProgressCallback | None = None,
) -> DownloadJob:
    """Download one job, retrying eligible transient failures.

    Each attempt uses the existing single-attempt downloader.
    A failed attempt cleans up its partial file before retrying.
    """
    if job.status is not DownloadStatus.QUEUED:
        raise DownloadError("Only queued download jobs can be started.")

    def report(job_update: DownloadJob) -> None:
        """Forward progress, except intermediate failed states."""
        if on_progress is None:
            return

        # The outer retry controller decides whether FAILED
        # is final. Do not stop Rich progress prematurely.
        if job_update.status is not DownloadStatus.FAILED:
            on_progress(job_update)

    try:
        while True:
            try:
                return await download_file(
                    job,
                    config,
                    on_progress=report,
                )

            except DownloadError as exc:
                retry_allowed = (
                    is_retryable(exc) and job.retry_count < config.max_retries
                )

                if not retry_allowed:
                    # The downloader normally records FAILED.
                    # Publish the final failure to the UI.
                    if on_progress is not None:
                        on_progress(job)

                    raise

                # The failed download_file() call has already
                # removed its own temporary partial file.
                job.retry_count += 1

                delay = calculate_backoff(
                    job.retry_count,
                    config,
                )

                # A retry always starts from byte zero.
                job.status = DownloadStatus.QUEUED
                job.downloaded_bytes = 0
                job.total_bytes = None

                job.error_message = (
                    f"Retry {job.retry_count}/"
                    f"{config.max_retries} after "
                    f"{type(exc).__name__}; "
                    f"waiting {delay:g} seconds."
                )

                if on_progress is not None:
                    on_progress(job)

                # Asynchronous waiting allows other downloads
                # to continue and supports cancellation.
                await asyncio.sleep(delay)

    except asyncio.CancelledError:
        job.status = DownloadStatus.CANCELLED
        job.error_message = "Download cancelled."

        if on_progress is not None:
            on_progress(job)

        raise
