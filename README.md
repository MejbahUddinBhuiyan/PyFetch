
# PyFetch

High-Performance Asynchronous Download Manager

PyFetch is a Python 3.12+ terminal application being developed
to download files concurrently, safely, and reliably.

## Project Status

Milestone 1 — Project initialization.

The download engine is not yet implemented.

## Technology

- Python 3.12+
- asyncio
- aiohttp
- aiofiles
- Typer
- Rich
- pytest
- Ruff
- mypy

## Installation

Create and activate a virtual environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project:

```powershell
python -m pip install -e ".[dev]"
```

## Usage

```powershell
pyfetch --help
pyfetch --version
pyfetch doctor
```

## Development Checks

```powershell
python -m ruff format --check .
python -m ruff check .
python -m mypy
python -m pytest -v
```

## Planned Features

- Asynchronous streamed downloads
- Concurrent downloads
- Terminal progress tracking
- Safe pause and resume
- Retry and timeout handling
- SHA-256 file integrity checks
- Persistent queue and download history
- JSON/CSV reporting

## Author

Mejbah Uddin Bhuiyan

## License

MIT
