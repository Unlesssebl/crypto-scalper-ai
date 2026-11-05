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
from pathlib import Path

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

from apps.scalper_web.__main__ import analyst, terminal_signals
from extensions.scalper.activity import active_anomalies
from extensions.scalper.journal import Journal
from extensions.scalper.market import WINDOWS, orderbook_details
from extensions.scalper.remote import RemoteHub
from extensions.scalper.service import Screener
from extensions.scalper.telegram import listen_reports
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
        "rows": rows,
    })


async def detail(request: web.Request) -> web.Response:
    service: Screener = request.app["service"]
    hub: RemoteHub = request.app["hub"]
    symbol = request.match_info["symbol"]
    window = window_of(request)
    if (symbol, window) not in service.rows:
        raise web.HTTPNotFound()
    return web.json_response({
        **service.rows[symbol, window],
        "book": orderbook_details(hub, symbol),
        "history": service.journal.latest(symbol, window, limit=20),
    })


async def healthz(request: web.Request) -> web.Response:
    hub: RemoteHub = request.app["hub"]
    ready = bool(hub.ready)
    return web.json_response({
        "ready": ready,
        "last_error": hub.last_error,
        "mode": hub.status.mode,
        "market": hub.status.market_data,
        "warnings": hub.warnings,
    }, status=200 if ready else 503)


def build_app(hub: RemoteHub, service: Screener) -> web.Application:
    app = web.Application()
    app["hub"] = hub
    app["service"] = service
    app.router.add_get("/scalper", dashboard)
    app.router.add_get("/v1/scalper", screener)
    app.router.add_get("/v1/scalper/{symbol}", detail)
    app.router.add_get("/healthz", healthz)
    return app


async def run(args: argparse.Namespace) -> None:
    journal = Journal(args.journal)
    hub = RemoteHub(args.api_url, timeout=args.timeout)
    runner: web.AppRunner | None = None
    tasks: list[asyncio.Task] = []
    try:
        await hub.start()
        async with aiohttp.ClientSession() as session:
            gemini = analyst(session, hub.demo)
            notify = bool(args.notify and hub.alerts.telegram_token and hub.alerts.telegram_chat_id)
            service = Screener(hub, journal, gemini, notify)
            app = build_app(hub, service)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", args.port)
            await site.start()
            journal.recover()
            tasks.extend(asyncio.create_task(worker()) for worker in
                         (service.run, service.analyze, service.deliver))
            tasks.append(asyncio.create_task(service.refresh_quality(args.journal), name="screener-quality"))
            tasks.append(asyncio.create_task(service.refresh_activity(), name="screener-activity"))
            tasks.append(asyncio.create_task(service.refresh_profiles(args.journal), name="screener-profiles"))
            if notify:
                tasks.append(asyncio.create_task(listen_reports(session, service), name="telegram-reports"))
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
        journal.close()


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default=os.getenv("HYPERDATA_API_URL", "http://127.0.0.1:8420"),
                        help="native HyperData API base URL (default: http://127.0.0.1:8420)")
    parser.add_argument("--port", type=int, default=8421, help="screener web port (default: 8421)")
    parser.add_argument("--journal", type=Path, default=None,
                        help="SQLite journal path (default: <screener data dir>/scalper.sqlite)")
    parser.add_argument("--timeout", type=float, default=10.0, help="native API timeout in seconds")
    parser.add_argument("--tui", action="store_true", help="also show the Rich Market Overview")
    parser.add_argument("--notify", action="store_true", help="enable Telegram delivery")
    args = parser.parse_args()
    if args.journal is None:
        args.journal = data_dir() / "scalper.sqlite"
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    logging.basicConfig(level=logging.WARNING if args.tui else logging.INFO)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
