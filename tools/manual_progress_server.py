"""Local HTTP streaming server for manual progress testing."""

import asyncio

from aiohttp import web

CHUNK_SIZE = 65_536
CHUNK_COUNT = 80
DELAY_SECONDS = 0.06


async def stream_file(
    request: web.Request,
) -> web.StreamResponse:
    """Stream deterministic test data with optional Content-Length."""
    known_size = request.match_info["kind"] != "unknown"

    headers: dict[str, str] = {}

    if known_size:
        headers["Content-Length"] = str(CHUNK_SIZE * CHUNK_COUNT)

    response = web.StreamResponse(
        status=200,
        headers=headers,
    )

    await response.prepare(request)

    chunk = b"P" * CHUNK_SIZE

    try:
        for _ in range(CHUNK_COUNT):
            await response.write(chunk)
            await asyncio.sleep(DELAY_SECONDS)

        await response.write_eof()

    except ConnectionResetError:
        # Expected when a client cancels a transfer.
        pass

    return response


def main() -> None:
    """Start the local test server."""
    app = web.Application()
    app.router.add_get("/{kind}", stream_file)

    web.run_app(
        app,
        host="127.0.0.1",
        port=8765,
    )


if __name__ == "__main__":
    main()
