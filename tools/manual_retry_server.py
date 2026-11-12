"""Local HTTP server for manually testing PyFetch retries."""

from collections import defaultdict

from aiohttp import web

attempts: defaultdict[str, int] = defaultdict(int)


async def flaky(request: web.Request) -> web.Response:
    """Return HTTP 503 twice, then succeed."""
    attempts["flaky"] += 1
    count = attempts["flaky"]

    print(f"Flaky endpoint: request {count}", flush=True)

    if count <= 2:
        return web.Response(
            status=503,
            text="Temporary server failure",
        )

    return web.Response(body=b"PyFetch retry completed successfully!")


async def missing(request: web.Request) -> web.Response:
    """Always return a permanent HTTP 404."""
    attempts["missing"] += 1

    print(
        f"Missing endpoint: request {attempts['missing']}",
        flush=True,
    )

    return web.Response(
        status=404,
        text="File not found",
    )


async def unavailable(request: web.Request) -> web.Response:
    """Always return HTTP 503 to test retry exhaustion."""
    attempts["unavailable"] += 1

    print(
        f"Unavailable endpoint: request {attempts['unavailable']}",
        flush=True,
    )

    return web.Response(
        status=503,
        text="Service unavailable",
    )


async def reset(request: web.Request) -> web.Response:
    """Reset manual request counters."""
    attempts.clear()
    return web.Response(text="Counters reset")


def main() -> None:
    """Start the local retry test server."""
    app = web.Application()

    app.router.add_get("/flaky", flaky)
    app.router.add_get("/missing", missing)
    app.router.add_get("/unavailable", unavailable)
    app.router.add_get("/reset", reset)

    web.run_app(
        app,
        host="127.0.0.1",
        port=8766,
    )


if __name__ == "__main__":
    main()
