"""Read-only HTTP API for Market Pulse.

The existing screener may consume these routes; it must never read collector
sockets or the PostgreSQL writer directly.  Events expose their durable
``event_seq`` cursor and mark recovered records separately from live ones.
"""
from __future__ import annotations

import time

from aiohttp import web

from .contracts import SCHEMA_VERSION, WINDOW_SCHEMA_VERSION
from .storage import MarketPulseStore

STORE_KEY = web.AppKey("store", object)
VERSION_KEY = web.AppKey("service_version", str)


def create_app(store: MarketPulseStore, *, service_version: str = "0.1.0") -> web.Application:
    app = web.Application()
    app[STORE_KEY] = store
    app[VERSION_KEY] = service_version
    app.router.add_get("/health", health)
    app.router.add_get("/coverage", coverage)
    app.router.add_get("/windows", windows)
    app.router.add_get("/events", events)
    app.router.add_get("/", index)
    app.middlewares.append(_no_store)
    return app


async def index(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "service": "market-pulse",
            "version": request.app[VERSION_KEY],
            "schema_version": SCHEMA_VERSION,
            "routes": ["/health", "/coverage", "/windows", "/events"],
        }
    )


async def health(request: web.Request) -> web.Response:
    store: MarketPulseStore = request.app[STORE_KEY]
    detail = await store.health()
    coverage = await store.list_coverage()
    gaps = await store.list_gaps(limit=10_000)
    open_gaps = [gap.to_payload() for gap in gaps if gap.end_ms is None]
    body = {
        "service": "market-pulse",
        "version": request.app[VERSION_KEY],
        "schema_version": SCHEMA_VERSION,
        "status": detail.get("status", "ok"),
        "storage": detail,
        "channels": [item.to_payload() for item in coverage],
        "open_gaps": open_gaps,
        "limits": {
            "full_exchange_completeness": "not claimed",
            "note": "observed_no_detected_gap is not absolute exchange completeness",
        },
    }
    return web.json_response(body, status=200 if detail.get("status") == "ok" else 503)


async def coverage(request: web.Request) -> web.Response:
    store: MarketPulseStore = request.app[STORE_KEY]
    channels = await store.list_coverage()
    gaps = await store.list_gaps(
        venue=request.query.get("venue"),
        native_market_id=request.query.get("market"),
        state=request.query.get("state"),
        limit=_int_query(request, "limit", 500, minimum=1, maximum=10_000),
    )
    return web.json_response(
        {
            "schema_version": WINDOW_SCHEMA_VERSION,
            "channels": [item.to_payload() for item in channels],
            "gaps": [gap.to_payload() for gap in gaps],
            "note": "A live order book or heartbeat does not prove trade-channel completeness.",
        }
    )


async def windows(request: web.Request) -> web.Response:
    store: MarketPulseStore = request.app[STORE_KEY]
    limit = _int_query(request, "limit", 500, minimum=1, maximum=10_000)
    rows = await store.list_windows(
        venue=request.query.get("venue"),
        native_market_id=request.query.get("market"),
        from_ms=_optional_int_query(request, "from_ms"),
        to_ms=_optional_int_query(request, "to_ms"),
        limit=limit,
    )
    payload = [window.to_payload() for window in rows]
    return web.json_response(
        {
            "schema_version": WINDOW_SCHEMA_VERSION,
            "windows": payload,
            "next_from_ms": rows[-1].window_end_ms if len(rows) == limit else None,
        }
    )


async def events(request: web.Request) -> web.Response:
    store: MarketPulseStore = request.app[STORE_KEY]
    cursor = _int_query(request, "cursor", 0, minimum=0, maximum=9_223_372_036_854_775_807)
    limit = _int_query(request, "limit", 500, minimum=1, maximum=5_000)
    rows = await store.list_events_after(
        cursor,
        venue=request.query.get("venue"),
        native_market_id=request.query.get("market"),
        from_ms=_optional_int_query(request, "from_ms"),
        to_ms=_optional_int_query(request, "to_ms"),
        limit=limit,
    )
    return web.json_response(
        {
            "schema_version": SCHEMA_VERSION,
            "cursor": cursor,
            "next_cursor": rows[-1].event_seq if rows else None,
            "events": [row.to_payload() for row in rows],
        }
    )


@web.middleware
async def _no_store(request: web.Request, handler):
    response = await handler(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _int_query(
    request: web.Request,
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = request.query.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise web.HTTPBadRequest(text=f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise web.HTTPBadRequest(text=f"{name} must be between {minimum} and {maximum}")
    return value


def _optional_int_query(request: web.Request, name: str) -> int | None:
    raw = request.query.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise web.HTTPBadRequest(text=f"{name} must be an integer") from exc


async def run_api(store: MarketPulseStore, *, host: str = "127.0.0.1", port: int = 8431) -> web.AppRunner:
    app = create_app(store)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner


def now_ms() -> int:
    return int(time.time() * 1_000)
