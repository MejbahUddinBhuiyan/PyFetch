"""Command-line interface for PyFetch."""

import asyncio
import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from pyfetch import __version__
from pyfetch.config import load_config
from pyfetch.downloader import download_file
from pyfetch.engine import run_downloads
from pyfetch.exceptions import PyFetchError, ValidationError
from pyfetch.models import DownloadJob

app = typer.Typer(
    name="pyfetch",
    help="High-performance asynchronous download manager.",
    no_args_is_help=True,
    add_completion=False,
)

console = Console()


def version_callback(value: bool) -> None:
    """Display the application version and exit."""
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
    """Check basic application installation."""
    console.print(
        Panel(
            "[bold green]PyFetch is installed successfully![/bold green]\n"
            f"Version: {__version__}\n"
            "Environment: Python CLI\n"
            "Download engine: Basic streaming available (M3)\n"
            "Concurrent batch downloads: Available (M4)\n"
            "Pause/resume: Not implemented yet",
            title="PyFetch Doctor",
            border_style="cyan",
        )
    )


@app.command()
def fetch(
    url: Annotated[
        str,
        typer.Argument(
            help="HTTP or HTTPS URL to download.",
        ),
    ],
    filename: Annotated[
        str,
        typer.Argument(
            help="Filename to save.",
        ),
    ],
    destination: Annotated[
        Path | None,
        typer.Option(
            "--destination",
            "-d",
            help="Override the configured download folder.",
        ),
    ] = None,
) -> None:
    """Download one file safely using asynchronous streaming."""
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

        asyncio.run(download_file(job, config))

        console.print("[bold green]Download completed![/bold green]")
        console.print(f"File: {job.file_path}")
        console.print(f"Downloaded bytes: {job.downloaded_bytes:,}")

    except KeyboardInterrupt:
        console.print("\n[yellow]Download interrupted by user.[/yellow]")
        raise typer.Exit(code=130) from None

    except PyFetchError as exc:
        console.print(f"[bold red]Download failed:[/bold red] {exc}")
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
        typer.Argument(
            help="Path to a JSON batch manifest.",
        ),
    ],
) -> None:
    """Download multiple files with a concurrency limit."""
    try:
        config = load_config()

        jobs = _load_batch_manifest(
            manifest,
            config.download_dir,
        )

        console.print(
            f"[cyan]Starting {len(jobs)} downloads "
            f"(maximum concurrent: {config.max_concurrency})[/cyan]"
        )

        result = asyncio.run(run_downloads(jobs, config))

    except KeyboardInterrupt:
        console.print("\n[yellow]Batch interrupted by user.[/yellow]")
        raise typer.Exit(code=130) from None

    except PyFetchError as exc:
        console.print(f"[red]Batch failed:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    for job in result.jobs:
        if job.error_message:
            console.print(f"{job.filename}: {job.status.value} — {job.error_message}")
        else:
            console.print(f"{job.filename}: {job.status.value}")

    console.print(
        f"Completed: {result.completed} | "
        f"Failed: {result.failed} | "
        f"Cancelled: {result.cancelled}"
    )

    if result.failed or result.cancelled:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
