"""SQLite persistence for PyFetch download jobs."""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyfetch.exceptions import StorageError
from pyfetch.models import DownloadJob, DownloadStatus

DEFAULT_DATABASE_PATH = Path("pyfetch.db")


@dataclass(frozen=True, slots=True)
class JobSnapshot:
    """Immutable state saved during download execution."""

    id: str
    status: DownloadStatus
    downloaded_bytes: int
    total_bytes: int | None
    retry_count: int
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None

    @classmethod
    def from_job(cls, job: DownloadJob) -> "JobSnapshot":
        """Copy mutable job metadata for safe persistence."""
        return cls(
            id=job.id,
            status=job.status,
            downloaded_bytes=job.downloaded_bytes,
            total_bytes=job.total_bytes,
            retry_count=job.retry_count,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error_message=job.error_message,
        )


class DownloadRepository:
    """Manage durable download records using SQLite."""

    def __init__(
        self,
        path: Path = DEFAULT_DATABASE_PATH,
    ) -> None:
        self.path = path
        self._initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Open, commit, and close an SQLite connection."""
        connection: sqlite3.Connection | None = None

        try:
            connection = sqlite3.connect(
                self.path,
                timeout=5.0,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 5000")

            yield connection
            connection.commit()

        except sqlite3.Error as exc:
            if connection is not None:
                connection.rollback()

            raise StorageError("SQLite database operation failed.") from exc

        finally:
            if connection is not None:
                connection.close()

    def _initialize(self) -> None:
        """Create the database schema if necessary."""
        try:
            self.path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            with self._connection() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS downloads (
                        id TEXT PRIMARY KEY,
                        url TEXT NOT NULL,
                        destination TEXT NOT NULL,
                        filename TEXT NOT NULL,
                        status TEXT NOT NULL,
                        downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                        total_bytes INTEGER,
                        retry_count INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        completed_at TEXT,
                        error_message TEXT,
                        expected_sha256 TEXT,
                        actual_sha256 TEXT
                    )
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_downloads_status_created
                    ON downloads(status, created_at)
                    """
                )

        except OSError as exc:
            raise StorageError("Cannot create the database directory.") from exc

    def add_job(self, job: DownloadJob) -> None:
        """Store a new queued job without replacing existing records."""
        if job.status is not DownloadStatus.QUEUED:
            raise StorageError("Only queued jobs can be added.")

        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO downloads (
                    id, url, destination, filename, status,
                    downloaded_bytes, total_bytes, retry_count,
                    created_at, started_at, completed_at,
                    error_message, expected_sha256, actual_sha256
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id,
                    job.url,
                    str(job.destination),
                    job.filename,
                    job.status.value,
                    job.downloaded_bytes,
                    job.total_bytes,
                    job.retry_count,
                    job.created_at.isoformat(),
                    self._datetime_text(job.started_at),
                    self._datetime_text(job.completed_at),
                    job.error_message,
                    job.expected_sha256,
                    job.actual_sha256,
                ),
            )

    @staticmethod
    def _datetime_text(
        value: datetime | None,
    ) -> str | None:
        """Serialize an optional datetime."""
        return value.isoformat() if value is not None else None

    @staticmethod
    def _parse_datetime(
        value: str | None,
    ) -> datetime | None:
        """Deserialize an optional datetime."""
        return datetime.fromisoformat(value) if value is not None else None

    @classmethod
    def _row_to_job(
        cls,
        row: sqlite3.Row,
    ) -> DownloadJob:
        """Reconstruct a validated download job."""
        created_at = cls._parse_datetime(row["created_at"])

        if created_at is None:
            raise StorageError("Stored job is missing its creation time.")

        return DownloadJob(
            id=row["id"],
            url=row["url"],
            destination=Path(row["destination"]),
            filename=row["filename"],
            status=DownloadStatus(row["status"]),
            downloaded_bytes=row["downloaded_bytes"],
            total_bytes=row["total_bytes"],
            retry_count=row["retry_count"],
            created_at=created_at,
            started_at=cls._parse_datetime(row["started_at"]),
            completed_at=cls._parse_datetime(row["completed_at"]),
            error_message=row["error_message"],
            expected_sha256=row["expected_sha256"],
            actual_sha256=row["actual_sha256"],
        )

    def list_jobs(
        self,
        statuses: tuple[DownloadStatus, ...] | None = None,
    ) -> list[DownloadJob]:
        """Return jobs ordered by creation time."""
        query = "SELECT * FROM downloads"
        parameters: tuple[str, ...] = ()

        if statuses is not None:
            if not statuses:
                return []

            placeholders = ", ".join("?" for _ in statuses)
            query += f" WHERE status IN ({placeholders})"
            parameters = tuple(status.value for status in statuses)

        query += " ORDER BY created_at, id"

        with self._connection() as connection:
            rows = connection.execute(
                query,
                parameters,
            ).fetchall()

        try:
            return [self._row_to_job(row) for row in rows]
        except (ValueError, TypeError, KeyError) as exc:
            raise StorageError("Stored download metadata is invalid.") from exc

    def get_job(self, job_id: str) -> DownloadJob | None:
        """Find one download by its UUID."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM downloads WHERE id = ?",
                (job_id,),
            ).fetchone()

        if row is None:
            return None

        try:
            return self._row_to_job(row)
        except (ValueError, TypeError, KeyError) as exc:
            raise StorageError("Stored download metadata is invalid.") from exc

    def save_snapshot(self, snapshot: JobSnapshot) -> None:
        """Persist the latest state of an existing job."""
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE downloads
                SET
                    status = ?,
                    downloaded_bytes = ?,
                    total_bytes = ?,
                    retry_count = ?,
                    started_at = ?,
                    completed_at = ?,
                    error_message = ?
                WHERE id = ?
                """,
                (
                    snapshot.status.value,
                    snapshot.downloaded_bytes,
                    snapshot.total_bytes,
                    snapshot.retry_count,
                    self._datetime_text(snapshot.started_at),
                    self._datetime_text(snapshot.completed_at),
                    snapshot.error_message,
                    snapshot.id,
                ),
            )

            if cursor.rowcount != 1:
                raise StorageError("Download record was not found.")

    def recover_interrupted(self) -> int:
        """Reset previously running jobs to queued.

        M7 restarts from byte zero, not from partial file content.
        Call only while holding the exclusive queue-runner lock.
        """
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE downloads
                SET
                    status = 'queued',
                    downloaded_bytes = 0,
                    total_bytes = NULL,
                    started_at = NULL,
                    completed_at = NULL,
                    error_message = 'Recovered after interruption'
                WHERE status = 'running'
                """
            )

            return cursor.rowcount
