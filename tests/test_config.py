"""Tests for validated JSON configuration."""

import json
from pathlib import Path

import pytest

from pyfetch.config import (
    DownloadConfig,
    load_config,
    save_config,
)
from pyfetch.exceptions import ConfigError, ValidationError


def test_default_configuration() -> None:
    config = DownloadConfig()

    assert config.max_concurrency == 4
    assert config.timeout_seconds == 60.0
    assert config.chunk_size == 65_536
    assert config.max_retries == 3
    assert config.download_dir == Path("downloads")


def test_custom_configuration() -> None:
    config = DownloadConfig(
        max_concurrency=8,
        timeout_seconds=90.0,
        connect_timeout_seconds=20.0,
        chunk_size=131_072,
    )

    assert config.max_concurrency == 8
    assert config.timeout_seconds == 90.0
    assert config.chunk_size == 131_072


@pytest.mark.parametrize(
    "values",
    [
        {"max_concurrency": 0},
        {"max_concurrency": -1},
        {"max_concurrency": True},
        {"chunk_size": 0},
        {"max_retries": -1},
        {"timeout_seconds": 0},
        {"timeout_seconds": -5},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": float("nan")},
        {"connect_timeout_seconds": 100},
        {"retry_base_delay": 0},
        {"retry_base_delay": 60},
        {"download_dir": ""},
    ],
)
def test_invalid_configuration(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        DownloadConfig(**values)  # type: ignore[arg-type]


def test_configuration_to_dict() -> None:
    config = DownloadConfig()
    data = config.to_dict()

    assert data["max_concurrency"] == 4
    assert data["download_dir"] == "downloads"


def test_configuration_from_dict() -> None:
    config = DownloadConfig.from_dict(
        {
            "max_concurrency": 6,
            "download_dir": "my_downloads",
        }
    )

    assert config.max_concurrency == 6
    assert config.download_dir == Path("my_downloads")


def test_unknown_configuration_key() -> None:
    with pytest.raises(ValidationError):
        DownloadConfig.from_dict({"not_a_real_setting": 123})


def test_save_and_load_configuration(tmp_path: Path) -> None:
    config_path = tmp_path / "pyfetch.json"

    original = DownloadConfig(
        max_concurrency=7,
        download_dir=Path("custom_downloads"),
    )

    save_config(original, config_path)
    restored = load_config(config_path)

    assert restored == original


def test_missing_config_returns_defaults(tmp_path: Path) -> None:
    config = load_config(tmp_path / "missing.json")

    assert config == DownloadConfig()


def test_invalid_json_raises_config_error(tmp_path: Path) -> None:
    path = tmp_path / "invalid.json"
    path.write_text("{invalid json", encoding="utf-8")

    with pytest.raises(ConfigError):
        load_config(path)


def test_json_root_must_be_object(tmp_path: Path) -> None:
    path = tmp_path / "invalid.json"
    path.write_text('["unexpected", "array"]', encoding="utf-8")

    with pytest.raises(ConfigError):
        load_config(path)


def test_existing_config_is_not_overwritten(
    tmp_path: Path,
) -> None:
    path = tmp_path / "pyfetch.json"

    first = DownloadConfig(max_concurrency=2)
    second = DownloadConfig(max_concurrency=8)

    save_config(first, path)

    with pytest.raises(ConfigError):
        save_config(second, path)

    assert load_config(path) == first


def test_explicit_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "pyfetch.json"

    save_config(DownloadConfig(max_concurrency=2), path)

    save_config(
        DownloadConfig(max_concurrency=8),
        path,
        overwrite=True,
    )

    assert load_config(path).max_concurrency == 8


def test_saved_json_is_readable(tmp_path: Path) -> None:
    path = tmp_path / "pyfetch.json"

    save_config(DownloadConfig(), path)

    content = json.loads(path.read_text(encoding="utf-8"))

    assert content["max_concurrency"] == 4
    assert content["download_dir"] == "downloads"


def test_nested_configuration_directory(tmp_path: Path) -> None:
    path = tmp_path / "settings" / "pyfetch.json"

    save_config(DownloadConfig(), path)

    assert path.exists()


def test_configuration_is_immutable() -> None:
    config = DownloadConfig()

    with pytest.raises(AttributeError):
        config.max_concurrency = 10  # type: ignore[misc]
