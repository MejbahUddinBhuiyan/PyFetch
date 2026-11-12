"""Validated application configuration and JSON persistence."""

import json
import math
import os
import tempfile
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from pyfetch.exceptions import ConfigError, ValidationError

DEFAULT_CONFIG_PATH = Path("pyfetch.json")


@dataclass(frozen=True, slots=True, kw_only=True)
class DownloadConfig:
    """Validated settings for the PyFetch download engine."""

    max_concurrency: int = 4
    timeout_seconds: float = 60.0
    connect_timeout_seconds: float = 15.0

    chunk_size: int = 65_536

    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0

    download_dir: Path = Path("downloads")

    def __post_init__(self) -> None:
        """Reject invalid or inconsistent configuration values."""
        if type(self.max_concurrency) is not int or self.max_concurrency < 1:
            raise ValidationError("max_concurrency must be a positive integer.")

        if type(self.chunk_size) is not int or self.chunk_size < 1:
            raise ValidationError("chunk_size must be a positive integer.")

        if type(self.max_retries) is not int or self.max_retries < 0:
            raise ValidationError("max_retries must be a non-negative integer.")

        numeric_settings = {
            "timeout_seconds": self.timeout_seconds,
            "connect_timeout_seconds": self.connect_timeout_seconds,
            "retry_base_delay": self.retry_base_delay,
            "retry_max_delay": self.retry_max_delay,
        }

        for name, value in numeric_settings.items():
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValidationError(f"{name} must be a finite positive number.")

        if self.connect_timeout_seconds > self.timeout_seconds:
            raise ValidationError("Connection timeout cannot exceed total timeout.")

        if self.retry_base_delay > self.retry_max_delay:
            raise ValidationError("Retry base delay cannot exceed maximum delay.")

        if not isinstance(self.download_dir, Path):
            raise ValidationError("download_dir must be a pathlib.Path object.")

        if not str(self.download_dir).strip():
            raise ValidationError("download_dir cannot be empty.")

    def to_dict(self) -> dict[str, object]:
        """Convert configuration into JSON-compatible values."""
        return {
            "max_concurrency": self.max_concurrency,
            "timeout_seconds": self.timeout_seconds,
            "connect_timeout_seconds": self.connect_timeout_seconds,
            "chunk_size": self.chunk_size,
            "max_retries": self.max_retries,
            "retry_base_delay": self.retry_base_delay,
            "retry_max_delay": self.retry_max_delay,
            "download_dir": str(self.download_dir),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DownloadConfig":
        """Create validated configuration from JSON-like data."""
        allowed = {item.name for item in fields(cls)}
        unknown = set(data) - allowed

        if unknown:
            raise ValidationError(
                f"Unknown configuration keys: {', '.join(sorted(unknown))}"
            )

        values = dict(data)

        if "download_dir" in values:
            directory = values["download_dir"]

            if not isinstance(directory, str) or not directory.strip():
                raise ValidationError("download_dir must be a non-empty string.")

            values["download_dir"] = Path(directory)

        try:
            return cls(**values)
        except TypeError as exc:
            raise ValidationError("Configuration contains invalid values.") from exc


def load_config(
    path: Path = DEFAULT_CONFIG_PATH,
) -> DownloadConfig:
    """Load JSON settings, or defaults when no file exists."""
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError:
        return DownloadConfig()
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError(f"Unable to load configuration from {path}.") from exc

    if not isinstance(data, dict):
        raise ConfigError("Configuration root must be a JSON object.")

    try:
        return DownloadConfig.from_dict(data)
    except ValidationError as exc:
        raise ConfigError(f"Invalid configuration: {exc}") from exc


def save_config(
    config: DownloadConfig,
    path: Path = DEFAULT_CONFIG_PATH,
    *,
    overwrite: bool = False,
) -> None:
    """Save JSON settings using a temporary file.

    By default, existing configuration is not overwritten.
    Setting overwrite=True explicitly enables replacement.
    """
    temporary_path: Path | None = None

    try:
        path.parent.mkdir(parents=True, exist_ok=True)

        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary_path = Path(temporary_name)

        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(config.to_dict(), file, indent=4)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        if overwrite:
            os.replace(temporary_path, path)
        else:
            # Create the target only if it does not exist.
            # Linking avoids a check-then-replace overwrite race.
            os.link(temporary_path, path)

    except FileExistsError as exc:
        raise ConfigError(
            f"Configuration already exists: {path}. Use overwrite=True to replace it."
        ) from exc

    except OSError as exc:
        raise ConfigError(f"Unable to save configuration to {path}.") from exc

    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
