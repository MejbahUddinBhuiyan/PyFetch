"""Persistent queue execution and Windows process locking."""

import asyncio
import msvcrt
import os
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from pyfetch.config import DownloadConfig
from pyfetch.engine import BatchResult, run_downloads
from pyfetch.exceptions import QueueLockedError
from pyfetch.models import DownloadJob, DownloadStatus
from pyfetch.storage import DownloadRepository, JobSnapshot

ProgressCallback = Callable[[DownloadJob], None]


@contextmanager
def queue_lock(
    database_path: Path,
) -> Iterator[None]:
    """Allow only one queue runner per database on Windows."""
    lock_path = database_path.with_name(database_path.name + ".lock")

    lock_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with lock_path.open("a+b") as lock_file:
        # Check the file size without reading a locked byte.
        # Windows prevents other handles from reading bytes
        # that are currently locked by another process.
        if os.fstat(lock_file.fileno()).st_size == 0:
            lock_file.write(b"\0")
            lock_file.flush()

        lock_file.seek(0)

        try:
            msvcrt.locking(
                lock_file.fileno(),
                msvcrt.LK_NBLCK,
                1,
            )

        except OSError as exc:
            raise QueueLockedError(
                "Another PyFetch queue runner is already active."
            ) from exc

        try:
            yield

        finally:
            lock_file.seek(0)
            msvcrt.locking(
                lock_file.fileno(),
                msvcrt.LK_UNLCK,
                1,
            )


async def execute_persistent_jobs(
    jobs: list[DownloadJob],
    repository: DownloadRepository,
    config: DownloadConfig,
    *,
    on_progress: ProgressCallback | None = None,
) -> BatchResult:
    """Execute jobs and persist progress without blocking the event loop."""
    events: asyncio.Queue[JobSnapshot | None] = asyncio.Queue()

    last_written_event: dict[str, float] = {}
    last_state: dict[str, tuple[str, int | None]] = {}

    def record_progress(job: DownloadJob) -> None:
        """Queue immutable metadata snapshots for SQLite writing."""
        if on_progress is not None:
            on_progress(job)

        now = time.monotonic()
        state = (job.status.value, job.total_bytes)

        final_state = job.is_finished

        if (
            not final_state
            and state == last_state.get(job.id)
            and now
            - last_written_event.get(
                job.id,
                float("-inf"),
            )
            < 0.5
        ):
            return

        last_written_event[job.id] = now
        last_state[job.id] = state

        events.put_nowait(JobSnapshot.from_job(job))

    async def database_writer() -> None:
        """Serialize SQLite writes from the event queue."""
        while True:
            snapshot = await events.get()

            try:
                if snapshot is None:
                    return

                await asyncio.to_thread(
                    repository.save_snapshot,
                    snapshot,
                )

            finally:
                events.task_done()

    writer_task = asyncio.create_task(database_writer())

    try:
        return await run_downloads(
            jobs,
            config,
            on_progress=record_progress,
        )

    finally:
        # Persist final in-memory states even if
        # the batch was cancelled during execution.
        for job in jobs:
            events.put_nowait(JobSnapshot.from_job(job))

        events.put_nowait(None)

        await writer_task


def prepare_queue(
    repository: DownloadRepository,
) -> tuple[int, list[DownloadJob]]:
    """Recover interrupted jobs and load queued work."""
    recovered = repository.recover_interrupted()

    jobs = repository.list_jobs((DownloadStatus.QUEUED,))

    return recovered, jobs
