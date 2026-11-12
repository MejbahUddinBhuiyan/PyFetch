"""Command-line interface for PyFetch."""

import asyncio
import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from pyfetch import __version__
from pyfetch.config import load_config
from pyfetch.engine import run_downloads
from pyfetch.exceptions import PyFetchError, ValidationError
from pyfetch.models import DownloadJob, DownloadStatus
from pyfetch.progress import DownloadProgress
from pyfetch.queue_runner import (
    execute_persistent_jobs,
    prepare_queue,
    queue_lock,
)
from pyfetch.retry import download_with_retries
from pyfetch.storage import DownloadRepository

app = typer.Typer(
    name="pyfetch",
    help="High-performance asynchronous download manager.",
    no_args_is_help=True,
    add_completion=False,
)

console = Console()


def version_callback(value: bool) -> None:
    """Display the installed application version."""
    if value:
        typer.echo(f"PyFetch {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            help="Show the installed PyFetch version.",
            callback=version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """PyFetch command-line application."""


@app.command()
def doctor() -> None:
    """Show installation and feature information."""
    console.print(
        Panel(
            "[bold green]PyFetch is installed successfully![/bold green]\n"
            f"Version: {__version__}\n"
            "Async streaming: Available (M3)\n"
            "Concurrent downloads: Available (M4)\n"
            "Live progress: Available (M5)\n"
            "Retries: Available (M6)\n"
            "Persistent SQLite queue: Available (M7)\n"
            "HTTP Range resume: Not implemented yet",
            title="PyFetch Doctor",
            border_style="cyan",
        )
    )


@app.command()
def fetch(
    url: Annotated[
        str,
        typer.Argument(help="HTTP or HTTPS download URL."),
    ],
    filename: Annotated[
        str,
        typer.Argument(help="Output filename."),
    ],
    destination: Annotated[
        Path | None,
        typer.Option(
            "--destination",
            "-d",
            help="Destination directory.",
        ),
    ] = None,
) -> None:
    """Download one file immediately, without persistent queueing."""
    try:
        config = load_config()

        job = DownloadJob(
            url=url,
            filename=filename,
            destination=(
                destination if destination is not None else config.download_dir
            ),
        )

        console.print(f"[cyan]Starting download:[/cyan] {job.filename}")

        with DownloadProgress(
            [job],
            console=console,
        ) as progress:
            asyncio.run(
                download_with_retries(
                    job,
                    config,
                    on_progress=progress.update,
                )
            )

        console.print("[bold green]Download completed![/bold green]")
        console.print(f"File: {job.file_path}")
        console.print(f"Downloaded bytes: {job.downloaded_bytes:,}")
        console.print(f"Retries used: {job.retry_count}")

    except KeyboardInterrupt:
        console.print("\n[yellow]Download interrupted by user.[/yellow]")
        raise typer.Exit(code=130) from None

    except PyFetchError as exc:
        console.print(f"[red]Download failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc


def _load_batch_manifest(
    path: Path,
    default_destination: Path,
) -> list[DownloadJob]:
    """Read and validate a JSON batch manifest."""
    try:
        data: object = json.loads(path.read_text(encoding="utf-8"))
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
    ) as exc:
        raise ValidationError("Cannot read a valid JSON batch manifest.") from exc

    if not isinstance(data, list) or not data:
        raise ValidationError("Batch manifest must be a non-empty JSON array.")

    jobs: list[DownloadJob] = []

    for index, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValidationError(f"Batch entry {index} must be an object.")

        if set(item) - {"url", "filename", "destination"}:
            raise ValidationError(f"Batch entry {index} has unknown fields.")

        url = item.get("url")
        filename = item.get("filename")
        directory = item.get(
            "destination",
            str(default_destination),
        )

        if not isinstance(url, str) or not isinstance(filename, str):
            raise ValidationError(
                f"Batch entry {index} needs URL and filename strings."
            )

        if not isinstance(directory, str) or not directory.strip():
            raise ValidationError(f"Batch entry {index} has an invalid destination.")

        jobs.append(
            DownloadJob(
                url=url,
                filename=filename,
                destination=Path(directory),
            )
        )

    return jobs


@app.command()
def batch(
    manifest: Annotated[
        Path,
        typer.Argument(help="JSON batch manifest."),
    ],
) -> None:
    """Download files from a manifest without storing queue history."""
    try:
        config = load_config()
        jobs = _load_batch_manifest(
            manifest,
            config.download_dir,
        )

        console.print(
            f"[cyan]Starting {len(jobs)} downloads "
            f"(maximum concurrent: {config.max_concurrency}, "
            f"maximum retries: {config.max_retries})[/cyan]"
        )

        with DownloadProgress(
            jobs,
            console=console,
        ) as progress:
            result = asyncio.run(
                run_downloads(
                    jobs,
                    config,
                    on_progress=progress.update,
                )
            )

    except KeyboardInterrupt:
        console.print("\n[yellow]Batch interrupted by user.[/yellow]")
        raise typer.Exit(code=130) from None

    except PyFetchError as exc:
        console.print(f"[red]Batch failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    for job in result.jobs:
        line = f"{job.filename}: {job.status.value} | retries: {job.retry_count}"

        if job.error_message:
            line += f" | {job.error_message}"

        console.print(line)

    console.print(
        f"Completed: {result.completed} | "
        f"Failed: {result.failed} | "
        f"Cancelled: {result.cancelled}"
    )

    if result.failed or result.cancelled:
        raise typer.Exit(code=1)


def _print_jobs(
    jobs: list[DownloadJob],
    *,
    title: str,
) -> None:
    """Display saved jobs without exposing URL credentials or tokens."""
    table = Table(title=title)

    table.add_column("ID", overflow="fold")
    table.add_column("Filename")
    table.add_column("Status")
    table.add_column("Bytes", justify="right")
    table.add_column("Retries", justify="right")

    for job in jobs:
        total = str(job.total_bytes) if job.total_bytes is not None else "unknown"

        table.add_row(
            job.id[:8],
            job.filename,
            job.status.value,
            f"{job.downloaded_bytes:,} / {total}",
            str(job.retry_count),
        )

    console.print(table)


@app.command("add")
def add_job(
    url: Annotated[
        str,
        typer.Argument(help="HTTP or HTTPS download URL."),
    ],
    filename: Annotated[
        str,
        typer.Argument(help="Output filename."),
    ],
    destination: Annotated[
        Path | None,
        typer.Option(
            "--destination",
            "-d",
            help="Destination directory.",
        ),
    ] = None,
) -> None:
    """Add a download to the SQLite queue without starting it."""
    try:
        config = load_config()
        repository = DownloadRepository()

        job = DownloadJob(
            url=url,
            filename=filename,
            destination=(
                destination if destination is not None else config.download_dir
            ),
        )

        repository.add_job(job)

    except (PyFetchError, OSError) as exc:
        console.print(f"[red]Cannot add download:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    console.print(f"[green]Added to queue:[/green] {job.filename}")
    console.print(f"Job ID: {job.id}")


@app.command("list")
def list_jobs() -> None:
    """List all stored download jobs."""
    try:
        repository = DownloadRepository()
        jobs = repository.list_jobs()

    except PyFetchError as exc:
        console.print(f"[red]Cannot list jobs:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    if not jobs:
        console.print("No saved download jobs.")
        return

    _print_jobs(jobs, title="PyFetch Download Queue")


@app.command("history")
def history() -> None:
    """Show completed, failed, and cancelled download jobs."""
    try:
        repository = DownloadRepository()
        jobs = repository.list_jobs(
            (
                DownloadStatus.COMPLETED,
                DownloadStatus.FAILED,
                DownloadStatus.CANCELLED,
            )
        )

    except PyFetchError as exc:
        console.print(f"[red]Cannot load history:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    if not jobs:
        console.print("No download history yet.")
        return

    _print_jobs(jobs, title="PyFetch Download History")


@app.command("start")
def start_queue() -> None:
    """Recover and execute saved queued downloads."""
    try:
        config = load_config()
        repository = DownloadRepository()

        with queue_lock(repository.path):
            recovered, jobs = prepare_queue(repository)

            if recovered:
                console.print(
                    f"[yellow]Recovered {recovered} interrupted download(s).[/yellow]"
                )

            if not jobs:
                console.print("No queued downloads.")
                return

            console.print(
                f"[cyan]Starting {len(jobs)} queued downloads "
                f"(maximum concurrent: {config.max_concurrency})[/cyan]"
            )

            with DownloadProgress(
                jobs,
                console=console,
            ) as progress:
                result = asyncio.run(
                    execute_persistent_jobs(
                        jobs,
                        repository,
                        config,
                        on_progress=progress.update,
                    )
                )

    except KeyboardInterrupt:
        console.print("\n[yellow]Queue interrupted by user.[/yellow]")
        raise typer.Exit(code=130) from None

    except (PyFetchError, OSError) as exc:
        console.print(f"[red]Queue error:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    for job in result.jobs:
        console.print(
            f"{job.filename}: {job.status.value} | retries: {job.retry_count}"
        )

    console.print(
        f"Completed: {result.completed} | "
        f"Failed: {result.failed} | "
        f"Cancelled: {result.cancelled}"
    )

    if result.failed or result.cancelled:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
