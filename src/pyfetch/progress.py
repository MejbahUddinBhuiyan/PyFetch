"""Nonblocking live terminal progress for PyFetch jobs."""

from collections.abc import Sequence
from time import monotonic
from types import TracebackType

from rich.console import Console, RenderableType
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TextColumn,
    TransferSpeedColumn,
)
from rich.text import Text

from pyfetch.models import DownloadJob, DownloadStatus


class KnownPercentageColumn(ProgressColumn):
    """Display percentage only when a reliable total is available."""

    def render(self, task: Task) -> RenderableType:
        if task.total is None:
            return Text("--")

        return Text(f"{task.percentage:.0f}%")


class KnownEtaColumn(ProgressColumn):
    """Display ETA only with a known total and measured speed."""

    def render(self, task: Task) -> RenderableType:
        if task.total is None or not task.speed or task.speed <= 0:
            return Text("ETA --")

        remaining = max(0.0, task.total - task.completed)
        seconds = int(remaining / task.speed + 0.999)

        minutes, secs = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)

        if hours:
            return Text(f"ETA {hours:d}:{minutes:02d}:{secs:02d}")

        return Text(f"ETA {minutes:d}:{secs:02d}")


class DownloadProgress:
    """Render multiple DownloadJob objects with Rich Progress."""

    def __init__(
        self,
        jobs: Sequence[DownloadJob],
        *,
        console: Console | None = None,
        update_interval: float = 0.1,
    ) -> None:
        if update_interval < 0:
            raise ValueError("update_interval must not be negative")

        self._update_interval = update_interval

        self._last_update: dict[str, float] = {}
        self._last_state: dict[str, tuple[DownloadStatus, int | None]] = {}

        self._task_ids: dict[str, TaskID] = {}

        self.progress = Progress(
            SpinnerColumn(),
            TextColumn(
                "{task.fields[filename]}",
                justify="left",
            ),
            TextColumn("[{task.fields[status]}]"),
            BarColumn(),
            KnownPercentageColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            KnownEtaColumn(),
            console=console,
            refresh_per_second=8,
            transient=False,
        )

        for job in jobs:
            if job.id in self._task_ids:
                raise ValueError("Duplicate download job ID")

            task_id = self.progress.add_task(
                "download",
                filename=job.filename,
                status=job.status.value,
                total=job.total_bytes,
                completed=job.downloaded_bytes,
            )

            self._task_ids[job.id] = task_id

    def __enter__(self) -> "DownloadProgress":
        """Start the Rich display."""
        self.progress.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop the display even when a download fails."""
        self.progress.stop()

    def update(self, job: DownloadJob) -> None:
        """Update one task using its current download metadata."""
        task_id = self._task_ids[job.id]

        now = monotonic()
        state = (job.status, job.total_bytes)

        terminal = job.status in {
            DownloadStatus.COMPLETED,
            DownloadStatus.FAILED,
            DownloadStatus.CANCELLED,
        }

        if (
            not terminal
            and state == self._last_state.get(job.id)
            and now - self._last_update.get(job.id, float("-inf"))
            < self._update_interval
        ):
            return

        self._last_update[job.id] = now
        self._last_state[job.id] = state

        self.progress.update(
            task_id,
            completed=job.downloaded_bytes,
            total=job.total_bytes,
            status=job.status.value,
            refresh=False,
        )

        if terminal:
            self.progress.stop_task(task_id)
