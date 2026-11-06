"""Read-only HTTP API for Market Pulse.

The existing screener may consume these routes; it must never read collector
sockets or the PostgreSQL writer directly.  Events expose their durable
``event_seq`` cursor and mark recovered records separately from live ones.
"""
from __future__ import annotations

import os
import time

from aiohttp import web

from .contracts import SCHEMA_VERSION, WINDOW_SCHEMA_VERSION
from .paging import page_windows
from .storage import MarketPulseStore

STALE_AFTER_MS = 120_000

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
    app.router.add_get("/overview", overview)
    app.router.add_get("/attention", attention)
    app.router.add_get("/", index)
    app.middlewares.append(_no_store)
    return app


async def index(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "service": "market-pulse",
            "version": request.app[VERSION_KEY],
            "schema_version": SCHEMA_VERSION,
            "routes": ["/health", "/coverage", "/windows", "/events", "/attention", "/overview"],
        }
    )


async def overview(request):
    from .overview import activity
    now = now_ms()
    rows = await request.app[STORE_KEY].get_overview(now - 17*60_000)
    return web.json_response(activity(rows, now))


async def attention(request):
    store = request.app[STORE_KEY]
    return web.json_response({"plan": await store.get_control("attention"), "collectors": {
        venue: await store.get_control("collector:" + venue) for venue in ("hyperliquid", "okx")}})


async def health(request: web.Request) -> web.Response:
    store: MarketPulseStore = request.app[STORE_KEY]
    detail = await store.health()
    coverage = await store.list_coverage()
    gaps = await store.list_gaps(limit=10_000)
    open_gaps = [gap.to_payload() for gap in gaps if gap.state == "gap_detected"]
    stale = _stale_channels(coverage)
    plan = await store.get_control("attention")
    missing = []
    if plan:
        from .collectors.specs import market_for_venue
        from .control import desired_symbols
        existing = {(c.venue, c.native_market_id, c.channel) for c in coverage
                    if c.state != "outside_attention"}
        missing = [{"venue": venue, "market": market_for_venue(venue, symbol)}
                   for symbol in desired_symbols(plan, now_ms())
                   for venue, channel in (("hyperliquid", "trades"), ("okx", "trades-all"))
                   if (venue, market_for_venue(venue, symbol), channel) not in existing]
    status = detail.get("status", "ok")
    maintenance = await store.get_control("maintenance")
    if maintenance and maintenance.get("status") == "blocked":
        status = "degraded"
    if stale or missing:
        status = "degraded"
    body = {
        "service": "market-pulse",
        "version": request.app[VERSION_KEY],
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "storage": detail,
        "maintenance": maintenance,
        "channels": [_channel_payload(item) for item in coverage],
        "stale_channels": stale,
        "missing_channels": missing,
        "open_gaps": open_gaps,
        "limits": {
            "full_exchange_completeness": "not claimed",
            "note": "observed_no_detected_gap is not absolute exchange completeness",
        },
    }
    return web.json_response(body, status=200 if status == "ok" else 503)


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
            "channels": [_channel_payload(item) for item in channels],
            "gaps": [gap.to_payload() for gap in gaps],
            "note": "A live order book or heartbeat does not prove trade-channel completeness.",
        }
    )


async def windows(request: web.Request) -> web.Response:
    store: MarketPulseStore = request.app[STORE_KEY]
    limit = _int_query(request, "limit", 500, minimum=1, maximum=10_000)
    rows, next_from_ms = await page_windows(
        store,
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
            "next_from_ms": next_from_ms,
        }
    )


async def events(request: web.Request) -> web.Response:
    store: MarketPulseStore = request.app[STORE_KEY]
    cursor = _int_query(request, "cursor", 0, minimum=0, maximum=9_223_372_036_854_775_807)
    limit = _int_query(request, "limit", 500, minimum=1, maximum=5_000)
    head = await store.event_head()
    rows = await store.list_events_after(
        cursor,
        venue=request.query.get("venue"),
        native_market_id=request.query.get("market"),
        from_ms=_optional_int_query(request, "from_ms"),
        to_ms=_optional_int_query(request, "to_ms"),
        limit=limit,
    )
    rows = [row for row in rows if row.event_seq <= head]
    return web.json_response(
        {
            "schema_version": SCHEMA_VERSION,
            "head_cursor": head,
            "cursor": cursor,
            "next_cursor": rows[-1].event_seq if rows else None,
            "events": [row.to_payload() for row in rows],
        }
    )


def _stale_after_ms() -> int:
    raw = os.getenv("PULSE_STALE_AFTER_MS", str(STALE_AFTER_MS))
    try:
        return max(1_000, int(raw))
    except ValueError:
        return STALE_AFTER_MS


def _channel_payload(item) -> dict:
    last = _last_evidence(item)
    now = now_ms()
    silence = None if last is None else max(0, now - last)
    payload = item.to_payload()
    payload["stale"] = last is None or silence > _stale_after_ms()
    payload["silence_ms"] = silence
    return payload


def _last_evidence(item) -> int | None:
    values = [
        value
        for value in (item.heartbeat_ms, item.last_receive_ms)
        if value is not None
    ]
    return max(values) if values else None


def _stale_channels(channels) -> list[dict]:
    stale: list[dict] = []
    threshold = _stale_after_ms()
    now = now_ms()
    for item in channels:
        if item.state == "outside_attention":
            continue
        last = _last_evidence(item)
        silence = None if last is None else max(0, now - last)
        if last is None or silence > threshold:
            stale.append(
                {
                    "venue": item.venue,
                    "native_market_id": item.native_market_id,
                    "channel": item.channel,
                    "silence_ms": silence,
                    "coverage_state": item.state,
                }
            )
    return stale


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
