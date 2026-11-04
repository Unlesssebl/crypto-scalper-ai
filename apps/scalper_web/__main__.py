"""Single-process HyperData + Crypto Scalper web and terminal entry point."""
import argparse
import asyncio
import logging
import os
from pathlib import Path

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

from extensions.scalper.analysis import Gemini
from extensions.scalper.journal import Journal
from extensions.scalper.market import WINDOWS, ScalperHub, orderbook_details
from extensions.scalper.service import Screener
from hyperdata_terminal.api_server import HyperDataAPI
from hyperdata_terminal.dashboards.market_overview import MarketOverviewDashboard
from hyperdata_terminal.paths import DATA_DIR


class ScalperAPI(HyperDataAPI):
    def __init__(self, hub, service, port):
        super().__init__(hub, host="127.0.0.1", port=port)
        self.service = service

    def add_routes(self, app):
        app.router.add_get("/scalper", self.dashboard)
        app.router.add_get("/v1/scalper", self.screener)
        app.router.add_get("/v1/scalper/{symbol}", self.detail)

    async def dashboard(self, request):
        return web.FileResponse(Path(__file__).with_name("dashboard.html"), headers={
            "Cache-Control": "no-cache",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
        })

    def window(self, request):
        window = request.query.get("window", "1m")
        if window not in WINDOWS:
            raise web.HTTPBadRequest(text="Expected 1m or 5m")
        return window

    async def screener(self, request):
        window = self.window(request)
        rows = []
        for (symbol, frame), row in self.service.rows.items():
            if frame == window:
                latest = self.service.journal.latest(symbol, frame, completed=True)
                rows.append({**row, "latest": latest[0] if latest else None})
        return web.json_response({"mode": self.hub.status.mode,
                                  "market": self.hub.status.market_data,
                                  "gemini": "configured" if self.service.gemini.key else "disabled",
                                  "rows": rows})

    async def detail(self, request):
        symbol = request.match_info["symbol"]
        window = self.window(request)
        if (symbol, window) not in self.service.rows:
            raise web.HTTPNotFound()
        return web.json_response({
            **self.service.rows[symbol, window],
            "book": orderbook_details(self.hub, symbol),
            "history": self.service.journal.latest(symbol, window, limit=20),
        })


async def run(args):
    journal = Journal(args.journal)
    try:
        hub = ScalperHub(demo=args.demo)
    except BaseException:
        journal.close()
        raise
    if args.demo:
        hub.alerts.telegram_token = ""
        hub.alerts.telegram_chat_id = ""
        hub.alerts.discord_webhook = ""
        hub.alerts.enabled = False
    tasks = []
    try:
        async with aiohttp.ClientSession() as session:
            gemini = Gemini(session, os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite"),
                            "" if args.demo else os.getenv("GEMINI_API_KEY", ""))
            notify = bool(args.notify and hub.alerts.telegram_token and hub.alerts.telegram_chat_id)
            service = Screener(hub, journal, gemini, notify)
            hub._api_server = ScalperAPI(hub, service, args.port)
            try:
                await hub._api_server.start()
                journal.recover()
                await hub.start()
                tasks.extend(asyncio.create_task(worker()) for worker in
                             (service.run, service.analyze, service.deliver))
                logging.info("Dashboard: http://127.0.0.1:%d/scalper", args.port)
                if args.tui:
                    dashboard = MarketOverviewDashboard(
                        market_data=hub.market, demo=args.demo, asset_reader=hub.get_all_assets,
                    )
                    await dashboard.run()
                else:
                    await asyncio.Event().wait()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await hub.stop()
    finally:
        journal.close()


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8420)
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--tui", action="store_true")
    parser.add_argument("--notify", action="store_true", help="Enable Telegram delivery")
    args = parser.parse_args()
    if args.journal is None:
        args.journal = DATA_DIR / ("scalper-demo.sqlite" if args.demo else "scalper.sqlite")
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")
    logging.basicConfig(level=logging.WARNING if args.tui else logging.INFO)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
