"""Tests for the PyFetch command-line interface."""

from typer.testing import CliRunner

from pyfetch import __version__
from pyfetch.cli import app

runner = CliRunner()


def test_version_command() -> None:
    """The CLI should display the correct version."""
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert f"PyFetch {__version__}" in result.stdout


def test_doctor_command() -> None:
    """The installation diagnostic should run successfully."""
    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0
    assert "PyFetch is installed successfully!" in result.stdout
    assert "Not implemented yet" in result.stdout


def test_help_command() -> None:
    """The CLI should display its available commands."""
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "doctor" in result.stdout
    assert "High-performance asynchronous download manager" in result.stdout


def test_unknown_command() -> None:
    """Unknown commands should fail without crashing unexpectedly."""
    result = runner.invoke(app, ["unknown-command"])

    assert result.exit_code != 0
