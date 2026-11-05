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
from dataclasses import asdict
from pathlib import Path

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

from apps.scalper_web.__main__ import analyst, terminal_signals
from extensions.scalper.activity import active_anomalies
from extensions.scalper.journal import Journal
from extensions.scalper.market import WINDOWS, orderbook_details
from extensions.scalper.parity import Recorder
from extensions.scalper.remote import RemoteHub
from extensions.scalper.service import Screener
from extensions.scalper.telegram import announce_online, listen_reports, run_digest
from hyperdata_terminal.api_server import (
    _make_host_guard_middleware,
    _make_rate_limit_middleware,
    _RateLimiter,
)
from hyperdata_terminal.dashboards.market_overview import MarketOverviewDashboard

LOG = logging.getLogger(__name__)
DASHBOARD = Path(__file__).resolve().parent.parent / "scalper_web" / "dashboard.html"


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
    return web.FileResponse(DASHBOARD, headers={
        "Cache-Control": "no-cache",
        "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
    })


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
        rows.append({
            **row,
            "latest": latest[0] if latest else None,
            "accepted": accepted[0] if accepted else None,
            "queued": (symbol, frame) in service.pending,
            "anomalies": sorted(active_anomalies(row["snapshot"])),
        })
    return web.json_response({
        "mode": hub.status.mode,
        "market": hub.status.market_data,
        "gemini": "configured" if service.gemini.key else "disabled",
        "ai_provider": service.gemini.provider,
        "quality_report": service.quality_report,
        "context": hub.context,
        "rows": rows,
    })


async def detail(request: web.Request) -> web.Response:
    service: Screener = request.app["service"]
    hub: RemoteHub = request.app["hub"]
    symbol = request.match_info["symbol"]
    window = window_of(request)
    if (symbol, window) not in service.rows:
        raise web.HTTPNotFound()
    book = await hub.fetch_orderbook(symbol)
    return web.json_response({
        **service.rows[symbol, window],
        "book": asdict(book) if book else orderbook_details(hub, symbol),
        "history": service.journal.latest(symbol, window, limit=20),
    })


async def healthz(request: web.Request) -> web.Response:
    hub: RemoteHub = request.app["hub"]
    ready = bool(hub.ready)
    healthy = ready and hub.last_error is None and hub.status.market_data in ("connected", "synthetic")
    return web.json_response({
        "ready": ready,
        "healthy": healthy,
        "last_error": hub.last_error,
        "mode": hub.status.mode,
        "market": hub.status.market_data,
        "warnings": hub.warnings,
    }, status=200 if healthy else 503)


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
    app.router.add_get("/v1/scalper/{symbol}", detail)
    app.router.add_get("/healthz", healthz)
    return app


async def run(args: argparse.Namespace) -> None:
    journal = None
    recorder = Recorder(args.record) if args.record else None
    hub = RemoteHub(args.api_url, timeout=args.timeout)
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
            notify = bool(args.notify and not hub.demo
                          and hub.alerts.telegram_token and hub.alerts.telegram_chat_id)
            service = Screener(hub, journal, gemini, notify, recorder=recorder)
            app = build_app(hub, service)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", args.port)
            await site.start()
            journal.recover()
            tasks.extend(asyncio.create_task(worker()) for worker in
                         (service.run, service.analyze, service.deliver))
            tasks.append(asyncio.create_task(service.refresh_quality(path), name="screener-quality"))
            if not hub.demo:
                tasks.append(asyncio.create_task(service.refresh_activity(), name="screener-activity"))
                tasks.append(asyncio.create_task(service.refresh_profiles(path), name="screener-profiles"))
            if notify:
                tasks.append(asyncio.create_task(announce_online(service), name="telegram-online"))
                tasks.append(asyncio.create_task(listen_reports(session, service), name="telegram-reports"))
                tasks.append(asyncio.create_task(
                    run_digest(session, service, int(os.getenv("HYPERDATA_DIGEST_SECONDS", "900"))),
                    name="telegram-digest",
                ))
            LOG.info("Dashboard: http://127.0.0.1:%d/scalper (native %s)", args.port, args.api_url)
            if args.tui:
                dashboard_view = MarketOverviewDashboard(
                    market_data=None, demo=hub.demo, asset_reader=hub.get_all_assets,
                    signal_reader=lambda symbol: terminal_signals(journal, symbol),
                )
                await dashboard_view.run()
            else:
                await asyncio.Event().wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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
    parser.add_argument("--api-url", default=os.getenv("HYPERDATA_API_URL", "http://127.0.0.1:8420"),
                        help="native HyperData API base URL (default: http://127.0.0.1:8420)")
    parser.add_argument("--port", type=int, default=8421, help="screener web port (default: 8421)")
    parser.add_argument("--journal", type=Path, default=None,
                        help="SQLite journal path (default: <screener data dir>/scalper.sqlite; "
                             "scalper-demo.sqlite in demo mode)")
    parser.add_argument("--timeout", type=float, default=10.0, help="native API timeout in seconds")
    parser.add_argument("--tui", action="store_true", help="also show the Rich Market Overview")
    parser.add_argument("--notify", action="store_true", help="enable Telegram delivery")
    parser.add_argument("--record", type=Path, help="append every scan to this JSONL file")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    logging.basicConfig(level=logging.WARNING if args.tui else logging.INFO)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
