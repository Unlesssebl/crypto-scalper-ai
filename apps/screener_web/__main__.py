"""External screener: native HyperData API + the scalper service.

This is the migration target from `apps/scalper_web`: it does not start a hub
or duplicate exchange sockets. It polls the native `/v1/market`,
`/v1/snapshot` and `/v1/candles` endpoints and serves the existing scalper
dashboard from the same cache.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from dataclasses import asdict
from pathlib import Path

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

from extensions.scalper.activity import active_anomalies
from extensions.scalper.journal import Journal
from extensions.scalper.market import WINDOWS, orderbook_details
from extensions.scalper.opportunity import REASON_LABELS, OpportunityPolicy
from extensions.scalper.parity import Recorder
from extensions.scalper.pulse import PulseContext
from extensions.scalper.remote import RemoteHub
from extensions.scalper.runtime import analyst, terminal_signals
from extensions.scalper.service import Screener
from extensions.scalper.telegram import announce_online, listen_reports, run_digest
from hyperdata_terminal.api_server import (
    _make_host_guard_middleware,
    _make_rate_limit_middleware,
    _RateLimiter,
)
from hyperdata_terminal.dashboards.market_overview import MarketOverviewDashboard

LOG = logging.getLogger(__name__)
DASHBOARD = Path(__file__).resolve().parent / "dashboard.html"


def data_dir() -> Path:
    configured = os.getenv("HYPERDATA_SCREENER_DATA_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    if os.name == "nt":
        base = Path(os.getenv("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
        return base / "hyperdata-screener"
    return Path.home() / ".local" / "share" / "hyperdata-screener"


def journal_path(configured: Path | None, demo: bool) -> Path:
    if configured is not None:
        return configured
    return data_dir() / ("scalper-demo.sqlite" if demo else "scalper.sqlite")


def window_of(request: web.Request) -> str:
    window = request.query.get("window", "1m")
    if window not in WINDOWS:
        raise web.HTTPBadRequest(text="Expected 1m or 5m")
    return window


async def dashboard(_request: web.Request) -> web.FileResponse:
    return web.FileResponse(
        DASHBOARD,
        headers={
            "Cache-Control": "no-cache",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
        },
    )


async def screener(request: web.Request) -> web.Response:
    service: Screener = request.app["service"]
    hub: RemoteHub = request.app["hub"]
    window = window_of(request)
    rows = []
    for (symbol, frame), row in service.rows.items():
        if frame != window:
            continue
        latest = service.journal.latest(symbol, frame)
        accepted = service.journal.latest(symbol, frame, accepted=True)
        rows.append(
            {
                **row,
                "latest": latest[0] if latest else None,
                "accepted": accepted[0] if accepted else None,
                "anomalies": sorted(active_anomalies(row["snapshot"])),
            }
        )
    return web.json_response(
        {
            "mode": hub.status.mode,
            "runtime_mode": service.runtime_mode,
            "runtime_error": None if hub.demo else service.runtime_diagnostic(),
            "market": hub.status.market_data,
            "gemini": "configured" if service.gemini.key else "disabled",
            "ai_provider": service.gemini.provider,
            "ai_policy": asdict(service.policy),
            "ai_reasons": REASON_LABELS,
            "context": getattr(hub, "context", {}),
            "market_pulse": (
                {
                    **service.pulse.summary(),
                    "symbols": {symbol: service.pulse.facts(symbol, time.time()) for symbol in service.pulse.markets},
                }
                if getattr(service, "pulse", None)
                else {"status": "disabled"}
            ),
            "attention_plan": service.attention.plan,
            "attention": getattr(hub, "attention", {"enabled": False}),
            "consistency": service.context_state(time.time()),
            "rows": rows,
        }
    )


async def detail(request: web.Request) -> web.Response:
    service: Screener = request.app["service"]
    hub: RemoteHub = request.app["hub"]
    symbol = request.match_info["symbol"]
    window = window_of(request)
    if (symbol, window) not in service.rows:
        raise web.HTTPNotFound()
    fetch = getattr(hub, "fetch_orderbook", None)
    book = await fetch(symbol) if fetch is not None else None
    return web.json_response(
        {
            **service.rows[symbol, window],
            "book": asdict(book) if book else orderbook_details(hub, symbol),
            "observation": next(
                (r for r in getattr(hub, "attention", {}).get("active", []) if r["symbol"] == symbol), None
            ),
            "history": service.journal.latest(symbol, window, limit=20),
        }
    )


async def update_policy(request: web.Request) -> web.Response:
    if request.content_type != "application/json":
        raise web.HTTPUnsupportedMediaType()
    if request.headers.get("Origin") and request.headers["Origin"] != f"{request.scheme}://{request.host}":
        raise web.HTTPForbidden()
    try:
        body = await request.json()
        if not isinstance(body, dict) or set(body) != set(OpportunityPolicy.__dataclass_fields__):
            raise ValueError("Expected all AI policy fields")
        if (
            type(body["hourly_limit"]) is not int
            or type(body["cooldown_seconds"]) is not int
            or type(body["early"]) is not bool
            or type(body["experiments"]) is not bool
        ):
            raise ValueError("Invalid policy field types")
        policy = OpportunityPolicy(**body)
    except (ValueError, TypeError) as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    service = request.app["service"]
    service.journal.save_policy(asdict(policy))
    service.policy = policy
    return web.json_response(asdict(policy))


async def context_packet(request: web.Request) -> web.Response:
    service = request.app["service"]
    service.refresh_context_packets(time.time())
    symbol = request.match_info["symbol"]
    packet = next((p for s, p in service.context_packets.items() if s.casefold() == symbol.casefold()), None)
    if packet is None:
        raise web.HTTPNotFound(text="Instrument is outside current attention")
    return web.json_response(packet)


async def review_context(request: web.Request) -> web.Response:
    if request.content_type != "application/json":
        raise web.HTTPUnsupportedMediaType()
    if request.headers.get("Origin") and request.headers["Origin"] != f"{request.scheme}://{request.host}":
        raise web.HTTPForbidden()
    try:
        body = await request.json()
        if (
            not isinstance(body, dict)
            or set(body) not in ({"packet_id"}, {"packet_id", "route"})
            or body.get("route", "strong") not in {"free", "strong"}
            or not isinstance(body["packet_id"], str)
            or len(body["packet_id"]) != 64
            or any(c not in "0123456789abcdef" for c in body["packet_id"])
        ):
            raise ValueError("Expected fact packet hash")
    except (ValueError, TypeError) as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    result = await request.app["service"].review_context(
        request.match_info["symbol"], body["packet_id"], route=body.get("route", "strong")
    )
    return web.json_response(result)


async def healthz(request: web.Request) -> web.Response:
    hub: RemoteHub = request.app["hub"]
    service = request.app.get("service")
    ready = bool(hub.ready)
    healthy = ready and hub.last_error is None and hub.status.market_data in ("connected", "synthetic")
    attention = getattr(hub, "attention", {})
    plan_status = attention.get("plan_status")
    observation_error = (
        attention.get("subscription_error")
        or attention.get("plan_error")
        or attention.get("archive", {}).get("error")
        or (f"attention_plan_{plan_status}" if plan_status in {"expired", "missing", "unavailable"} else None)
    )
    runtime_error = (
        service.runtime_diagnostic() if service is not None and not getattr(hub, "demo", False) else None
    )
    healthy = healthy and not observation_error and not runtime_error
    return web.json_response(
        {
            "ready": ready,
            "healthy": healthy,
            "last_error": hub.last_error,
            "mode": hub.status.mode,
            "runtime_mode": getattr(service, "runtime_mode", None),
            "runtime_error": runtime_error,
            "market": hub.status.market_data,
            "warnings": hub.warnings,
            "attention_plan_status": plan_status,
            "observation_error": observation_error,
        },
        status=200 if healthy else 503,
    )


async def system_status(request):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with session.get("http://127.0.0.1:8422/status") as response:
                response.raise_for_status()
                data = await response.json()
                return web.json_response({"phase": data["phase"], "services": data["services"]})
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError):
        return web.json_response({"phase": "unmanaged", "services": {}}, status=503)


def build_app(hub: RemoteHub, service: Screener) -> web.Application:
    middlewares = []
    guard = _make_host_guard_middleware("127.0.0.1")
    if guard is not None:
        middlewares.append(guard)
    middlewares.append(_make_rate_limit_middleware(_RateLimiter()))
    app = web.Application(middlewares=middlewares)
    app["hub"] = hub
    app["service"] = service
    app.router.add_get("/scalper", dashboard)
    app.router.add_get("/v1/scalper", screener)
    app.router.add_post("/v1/scalper/policy", update_policy)
    app.router.add_get("/v1/scalper/context/{symbol}", context_packet)
    app.router.add_post("/v1/scalper/context/{symbol}", review_context)
    app.router.add_get("/v1/scalper/{symbol}", detail)
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/v1/system", system_status)
    return app


async def run(args: argparse.Namespace) -> None:
    journal = None
    recorder = Recorder(args.record) if args.record else None
    hub = RemoteHub(args.api_url, timeout=args.timeout, demo=args.demo)
    runner: web.AppRunner | None = None
    tasks: list[asyncio.Task] = []
    try:
        await hub.start()
        path = journal_path(args.journal, hub.demo)
        journal = Journal(path)
        if hub.demo:
            hub.alerts.telegram_token = ""
            hub.alerts.telegram_chat_id = ""
            hub.alerts.discord_webhook = ""
            hub.alerts.enabled = False
        async with aiohttp.ClientSession() as session:
            gemini = analyst(session, hub.demo)
            notify = bool(args.notify and not hub.demo and hub.alerts.telegram_token and hub.alerts.telegram_chat_id)
            pulse = (
                None
                if hub.demo
                else PulseContext(
                    os.getenv("PULSE_API_URL", "http://127.0.0.1:8431"),
                    (),
                    journal=journal if os.getenv("PULSE_SHADOW", "1") == "1" else None,
                )
            )
            service = Screener(hub, journal, gemini, notify, recorder=recorder, pulse=pulse)
            if not hub.demo and not service.validate_runtime():
                LOG.warning(
                    "Native API is not reachable yet; waiting for unified attention instead of restarting"
                )
            if pulse is not None:
                tasks.append(asyncio.create_task(pulse.run(), name="market-pulse-context"))
            if service.ai_routes:
                from dataclasses import asdict

                from extensions.scalper.history.archive import Archive
                from extensions.scalper.history.runtime import history_dir

                total = int(os.getenv("HYPERDATA_HISTORY_MAX_BYTES", str(5 * 1024**3)))
                service.attention.archive = Archive(
                    history_dir().parent / "attention-decisions", quota_bytes=min(256 * 1024**2, total // 20)
                )
                service.attention.archive.record("policy", asdict(service.attention.engine.policy), time.time())
                service.attention.archive.record("attention_state", service.attention.state(), time.time())
                if notify:
                    tasks.append(asyncio.create_task(service.outbox.run(), name="telegram-card-outbox"))
            app = build_app(hub, service)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", args.port)
            await site.start()
            for interrupted in service.recover_pending():
                LOG.info("Closed interrupted candidate %s", interrupted)
            tasks.append(asyncio.create_task(service.run(), name="screener-run"))
            # Legacy directional quality statistics are not part of the unified
            # runtime; the old reports remain available as a frozen archive.
            if not hub.demo:
                tasks.append(asyncio.create_task(service.auto_context(), name="screener-context-ai"))
                tasks.append(asyncio.create_task(service.refresh_activity(), name="screener-activity"))
                tasks.append(asyncio.create_task(service.refresh_profiles(path), name="screener-profiles"))
            if notify:
                if not service.telegram_ux:
                    tasks.append(asyncio.create_task(announce_online(service), name="telegram-online"))
                tasks.append(asyncio.create_task(listen_reports(session, service), name="telegram-reports"))
                digest_seconds = int(os.getenv("HYPERDATA_DIGEST_SECONDS", "0"))
                if digest_seconds > 0 and not service.telegram_ux:
                    tasks.append(
                        asyncio.create_task(
                            run_digest(session, service, digest_seconds),
                            name="telegram-digest",
                        )
                    )
            LOG.info("Dashboard: http://127.0.0.1:%d/scalper (native %s)", args.port, args.api_url)
            if args.tui:

                async def native_panels():
                    flows = []
                    for asset in hub.get_all_assets()[:6]:
                        try:
                            data = await hub.fetch_orderflow(asset.symbol)
                        except Exception:
                            continue
                        frame = (data.get("timeframes") or {}).get("1m") or {}
                        if not frame:
                            continue
                        flows.append(
                            {
                                "symbol": asset.symbol,
                                "window": "1m",
                                "signal": frame.get("signal") or "NEUTRAL",
                                "venue": frame.get("venue") or "combined",
                                "cvd": frame.get("net_volume"),
                                "ofi": frame.get("ofi"),
                                "trades": frame.get("trade_count"),
                                "quality": "warming_up" if frame.get("warming_up") else "ok",
                            }
                        )
                    try:
                        liquidations = await hub.fetch_liquidation_stats(60)
                    except Exception:
                        liquidations = None
                    try:
                        danger = await hub.fetch_danger_zone(5.0)
                    except Exception:
                        danger = None
                    try:
                        whales = await hub.fetch_whales(250_000.0, 10)
                    except Exception:
                        whales = None
                    try:
                        hlp = await hub.fetch_hlp()
                    except Exception:
                        hlp = None
                    try:
                        heatmap = await hub.fetch_heatmap("BTC", 20)
                    except Exception:
                        heatmap = None
                    try:
                        smart = await hub.fetch_smart_money()
                    except Exception:
                        smart = None
                    return {
                        "orderflow": flows,
                        "liquidations": liquidations,
                        "danger_zone": danger,
                        "whales": whales,
                        "hlp": hlp,
                        "heatmap": heatmap,
                        "smart_money": smart,
                    }

                dashboard_view = MarketOverviewDashboard(
                    market_data=None,
                    demo=hub.demo,
                    asset_reader=hub.get_all_assets,
                    signal_reader=lambda symbol: terminal_signals(journal, symbol),
                    context_reader=lambda: hub.context,
                    health_reader=lambda: hub.health_summary,
                    extra_reader=native_panels,
                    page=args.tui_page,
                    rotate_seconds=args.tui_rotate,
                )
                await dashboard_view.run()
            else:
                await asyncio.Event().wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if "service" in locals():
            await asyncio.to_thread(service.attention.close)
        if runner is not None:
            await runner.cleanup()
        await hub.stop()
        if recorder is not None:
            recorder.close()
        if journal is not None:
            journal.close()


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-url",
        default=os.getenv("HYPERDATA_API_URL", "http://127.0.0.1:8420"),
        help="native HyperData API base URL (default: http://127.0.0.1:8420)",
    )
    parser.add_argument("--port", type=int, default=8421, help="screener web port (default: 8421)")
    parser.add_argument(
        "--journal",
        type=Path,
        default=None,
        help="SQLite journal path (default: <screener data dir>/scalper.sqlite; scalper-demo.sqlite in demo mode)",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="native API timeout in seconds")
    parser.add_argument("--tui", action="store_true", help="also show the Rich Market Overview")
    parser.add_argument(
        "--tui-page",
        choices=["auto", "all", "market", "risk", "vault"],
        default=os.getenv("HYPERDATA_TUI_PAGE", "auto"),
        help="TUI layout: auto (rotate fitted pages on short terminals), all, market, risk or vault",
    )
    parser.add_argument(
        "--tui-rotate", type=float, default=0.0, help="seconds between rotated TUI pages (default: 0 = manual keys)"
    )
    parser.add_argument("--notify", action="store_true", help="enable Telegram delivery")
    parser.add_argument("--record", type=Path, help="append every scan to this JSONL file")
    parser.add_argument("--demo", action="store_true", help="synthetic data; disables AI, Telegram and Market Pulse")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    logging.basicConfig(level=logging.WARNING if args.tui else logging.INFO)
    try:
        from hyperdata_terminal.lifecycle import run_managed

        asyncio.run(run_managed(run(args)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
