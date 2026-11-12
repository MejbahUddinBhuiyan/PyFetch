"""Concurrent download scheduling on top of the single-file downloader."""

import asyncio
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from pyfetch.config import DownloadConfig
from pyfetch.downloader import download_file
from pyfetch.exceptions import DownloadError, ValidationError
from pyfetch.models import DownloadJob, DownloadStatus


@dataclass(frozen=True, slots=True)
class BatchResult:
    """Final job outcomes, preserving input order."""

    jobs: tuple[DownloadJob, ...]

    @property
    def completed(self) -> int:
        return sum(job.status is DownloadStatus.COMPLETED for job in self.jobs)

    @property
    def failed(self) -> int:
        return sum(job.status is DownloadStatus.FAILED for job in self.jobs)

    @property
    def cancelled(self) -> int:
        return sum(job.status is DownloadStatus.CANCELLED for job in self.jobs)


def _destination_key(path: Path) -> str:
    """Normalize a target path for collision detection on this OS."""
    return os.path.normcase(os.path.realpath(path))


def _validate_batch(jobs: Sequence[DownloadJob]) -> None:
    """Reject conflicts before any downloads begin."""
    seen_paths: set[str] = set()
    seen_ids: set[str] = set()

    for job in jobs:
        if job.status is not DownloadStatus.QUEUED:
            raise ValidationError("Every batch job must be queued.")

        if job.id in seen_ids:
            raise ValidationError("Duplicate download job ID in batch.")

        seen_ids.add(job.id)

        target = _destination_key(job.file_path)

        if target in seen_paths:
            raise ValidationError(
                "Two download jobs cannot use the same destination filename."
            )

        seen_paths.add(target)


async def run_downloads(
    jobs: Sequence[DownloadJob],
    config: DownloadConfig,
) -> BatchResult:
    """Download multiple files within the configured concurrency limit.

    Individual known download failures do not stop unrelated jobs.
    Cancelling the batch cancels all unfinished child tasks.
    """
    snapshot = tuple(jobs)

    _validate_batch(snapshot)

    if not snapshot:
        return BatchResult(jobs=())

    semaphore = asyncio.Semaphore(config.max_concurrency)

    async def worker(job: DownloadJob) -> None:
        """Run one download when a concurrency slot is available."""
        try:
            async with semaphore:
                try:
                    await download_file(job, config)
                except DownloadError:
                    # The M3 downloader records FAILED status
                    # and a useful error message on this job.
                    pass

        except asyncio.CancelledError:
            # A job waiting for the semaphore has not entered
            # the downloader, so we update its status here.
            if job.status is DownloadStatus.QUEUED:
                job.status = DownloadStatus.CANCELLED
                job.error_message = "Batch cancelled before download started."

            raise

    tasks = [asyncio.create_task(worker(job)) for job in snapshot]

    try:
        await asyncio.gather(*tasks)

    finally:
        # Also clean up child tasks if an unexpected exception occurs.
        unfinished = [task for task in tasks if not task.done()]

        for task in unfinished:
            task.cancel()

        if unfinished:
            await asyncio.gather(
                *unfinished,
                return_exceptions=True,
            )

    return BatchResult(jobs=snapshot)
