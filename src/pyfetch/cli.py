"""Command-line interface for PyFetch."""

from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from pyfetch import __version__

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
            "Download engine: Not implemented yet",
            title="PyFetch Doctor",
            border_style="cyan",
        )
    )


if __name__ == "__main__":
    app()
