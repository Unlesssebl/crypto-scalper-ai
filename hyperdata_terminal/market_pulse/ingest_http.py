"""Private HTTP ingest endpoints used only by collectors."""
from __future__ import annotations

import time
from typing import Any

from aiohttp import web

from .contracts import SCHEMA_VERSION, InstrumentSpec
from .control import validate_plan
from .gaps import GapTracker
from .ingest import BatchError, GapControlService, IngestionService, decode_batch
from .storage import MarketPulseStore, RawSegmentRecord


def create_ingest_app(
    store: MarketPulseStore,
    ingest: IngestionService,
    gap_control: GapControlService,
    *,
    token: str | None = None,
) -> web.Application:
    app = web.Application(client_max_size=16 * 1024 * 1024)
    app["store"] = store
    app["ingest"] = ingest
    app["gap_control"] = gap_control
    app["token"] = token
    app.router.add_post("/ingest/v1/overview", overview)
    app.router.add_post("/ingest/v1/batches", batches)
    app.router.add_post("/ingest/v1/gaps", gaps)
    app.router.add_post("/ingest/v1/instruments", instruments)
    app.router.add_post("/ingest/v1/raw-segments", raw_segments)
    app.router.add_get("/ingest/v1/health", health)
    app.router.add_post("/ingest/v1/attention", attention)
    app.router.add_get("/ingest/v1/attention", attention)
    app.router.add_post("/ingest/v1/collector-status/{venue:hyperliquid|okx|binance}", collector_status)
    app.middlewares.append(_auth)
    return app


@web.middleware
async def _auth(request: web.Request, handler):
    token = request.app["token"]
    if token and request.method == "POST":
        supplied = request.headers.get("Authorization", "")
        if supplied != f"Bearer {token}":
            raise web.HTTPUnauthorized()
    return await handler(request)


async def batches(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
        events = decode_batch(payload)
        result = await request.app["ingest"].append_batch(events, now_ms=now_ms())
    except BatchError as exc:
        return web.json_response({"status": "invalid", "detail": str(exc)}, status=400)
    except (ValueError, TypeError) as exc:
        return web.json_response({"status": "invalid", "detail": str(exc)}, status=400)
    status = 409 if result["status"] == "conflict" else 200
    return web.json_response({"schema_version": SCHEMA_VERSION, **result}, status=status)


async def overview(request):
    from .overview import validate_candle
    now = now_ms()
    try:
        payload = await request.json()
        if (not isinstance(payload, dict) or not isinstance(payload.get("candles"), list)
                or not 1 <= len(payload["candles"]) <= 1000):
            raise ValueError("expected 1..1000 candles")
        rows = [validate_candle(r, now) for r in payload["candles"]]
    except (ValueError, TypeError) as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    await request.app["store"].put_overview(rows, now)
    return web.json_response({"status": "ok", "accepted": len(rows)})


async def attention(request):
    store = request.app["store"]
    if request.method == "POST":
        try:
            plan = validate_plan(await request.json(), now_ms())
        except (ValueError, TypeError) as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        await store.set_control("attention", plan, plan["generated_at_ms"])
    return web.json_response({"plan": await store.get_control("attention"),
                              "maintenance": await store.get_control("maintenance")})


async def collector_status(request):
    payload = await request.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("markets"), dict):
        raise web.HTTPBadRequest(text="invalid collector status")
    payload["updated_at_ms"] = now_ms()
    # The saved author plan, rather than a collector's local memory, owns leases.
    plan = await request.app["store"].get_control("attention")
    if plan:
        from .control import desired_symbols
        await request.app["gap_control"].release_outside_attention(
            request.match_info["venue"], desired_symbols(plan, payload["updated_at_ms"]), payload["updated_at_ms"])
    await request.app["store"].set_control("collector:" + request.match_info["venue"], payload, now_ms())
    return web.json_response({"status": "ok"})


async def gaps(request: web.Request) -> web.Response:
    try:
        payload: dict[str, Any] = await request.json()
        result = await request.app["gap_control"].apply(payload, now_ms=int(payload.get("now_ms") or now_ms()))
    except BatchError as exc:
        return web.json_response({"status": "invalid", "detail": str(exc)}, status=400)
    except (ValueError, TypeError) as exc:
        return web.json_response({"status": "invalid", "detail": str(exc)}, status=400)
    return web.json_response(result)


async def instruments(request: web.Request) -> web.Response:
    try:
        payload: dict[str, Any] = await request.json()
        rows = payload.get("instruments")
        if not isinstance(rows, list) or not rows:
            raise BatchError("instruments must be a non-empty list")
        specs = [InstrumentSpec.from_payload(row) for row in rows]
    except (BatchError, KeyError, TypeError, ValueError) as exc:
        return web.json_response({"status": "invalid", "detail": str(exc)}, status=400)
    await request.app["ingest"].register_instruments(specs)
    return web.json_response({"status": "ok", "registered": len(specs)})


async def raw_segments(request: web.Request) -> web.Response:
    try:
        payload: dict[str, Any] = await request.json()
        rows = payload.get("segments")
        if not isinstance(rows, list):
            raise BatchError("segments must be a list")
        segments = [RawSegmentRecord(**row) for row in rows]
    except (BatchError, KeyError, TypeError, ValueError) as exc:
        return web.json_response({"status": "invalid", "detail": str(exc)}, status=400)
    await request.app["ingest"].register_raw_segments(segments)
    return web.json_response({"status": "ok", "registered": len(segments)})


async def health(request: web.Request) -> web.Response:
    store: MarketPulseStore = request.app["store"]
    detail = await store.health()
    return web.json_response({"status": detail.get("status", "ok"), "storage": detail})


async def run_ingest(
    store: MarketPulseStore,
    ingest: IngestionService,
    gap_control: GapControlService,
    *,
    host: str = "127.0.0.1",
    port: int = 8430,
    token: str | None = None,
) -> web.AppRunner:
    app = create_ingest_app(store, ingest, gap_control, token=token)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner


def now_ms() -> int:
    return int(time.time() * 1_000)


def gap_tracker_for(store: MarketPulseStore) -> GapTracker:
    return GapTracker(store)
