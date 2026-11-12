"""Command-line interface for PyFetch."""

import asyncio
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from pyfetch import __version__
from pyfetch.config import load_config
from pyfetch.downloader import download_file
from pyfetch.exceptions import PyFetchError
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
            "Concurrent downloads: Not implemented yet\n"
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


if __name__ == "__main__":
    app()
