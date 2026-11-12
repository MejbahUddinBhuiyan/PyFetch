"""Tests for M4 JSON manifest parsing and CLI registration."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pyfetch.cli import _load_batch_manifest, app
from pyfetch.exceptions import ValidationError


def test_batch_command_in_help() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "batch" in result.stdout
    assert "fetch" in result.stdout
    assert "doctor" in result.stdout


def test_manifest_loads_jobs_and_defaults(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobs.json"

    path.write_text(
        json.dumps(
            [
                {
                    "url": "https://example.com/a",
                    "filename": "a.bin",
                },
                {
                    "url": "https://example.com/b",
                    "filename": "b.bin",
                    "destination": "other",
                },
            ]
        ),
        encoding="utf-8",
    )

    jobs = _load_batch_manifest(
        path,
        Path("downloads"),
    )

    assert len(jobs) == 2
    assert jobs[0].destination == Path("downloads")
    assert jobs[1].destination == Path("other")


@pytest.mark.parametrize(
    "data",
    ["{}", "[]", "null", "not JSON"],
)
def test_invalid_manifest_rejected(
    tmp_path: Path,
    data: str,
) -> None:
    path = tmp_path / "jobs.json"
    path.write_text(data, encoding="utf-8")

    with pytest.raises(ValidationError):
        _load_batch_manifest(
            path,
            Path("downloads"),
        )


def test_unsafe_manifest_filename_rejected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "jobs.json"

    path.write_text(
        json.dumps(
            [
                {
                    "url": "https://example.com/a",
                    "filename": "../unsafe",
                }
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        _load_batch_manifest(
            path,
            Path("downloads"),
        )
