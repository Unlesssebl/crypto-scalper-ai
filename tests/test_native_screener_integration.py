"""End-to-end: native API handlers -> HTTP -> RemoteHub -> Screener.scan."""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from extensions.scalper.journal import Journal
from extensions.scalper.remote import RemoteHub
from extensions.scalper.service import Screener
from hyperdata_terminal.api_server import HyperDataAPI
from hyperdata_terminal.data_layer.market_data import AssetInfo
from hyperdata_terminal.data_layer.orderbook import OrderBookLevel, OrderBookSnapshot
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine, ShardState, Trade


def _asset(symbol: str, price: float, volume: float) -> AssetInfo:
    return AssetInfo(
        symbol=symbol, price=price, funding_rate=0.00001,
        open_interest=1_000_000.0, volume_24h=volume,
        price_change_24h_pct=0.01, mark_price=price, index_price=price * 0.999,
    )


def _trade(symbol: str, price: float, ts: float) -> Trade:
    return Trade(timestamp=ts, symbol=symbol, side="buy", price=price, size=1.0, size_usd=price)


class FakeHub:
    def __init__(self, now: float) -> None:
        self.demo = False
        self.market = SimpleNamespace(
            assets={"BTC": _asset("BTC", 70000.0, 5_000_000.0),
                    "ETH": _asset("ETH", 3500.0, 3_000_000.0)},
            observed_fields={
                "BTC": {"price", "volume_24h", "open_interest", "funding_rate"},
                "ETH": {"price", "volume_24h", "open_interest", "funding_rate"},
            },
            observed_at=now,
        )
        self.orderflow = OrderFlowEngine(symbols=["BTC", "ETH"])
        self.orderflow._hl_shard_plan = [["BTC", "ETH"]]
        self.orderflow._hl_shard_state[0] = ShardState(connected_at=now - 120)
        for symbol, price in (("BTC", 70000.0), ("ETH", 3500.0)):
            self.orderflow._process_trade(_trade(symbol, price, now), venue="hyperliquid")
            for window in ("1m", "5m"):
                self.orderflow.venue_buckets["hyperliquid"][symbol][window].first_trade_at = (
                    now - 60 if window == "1m" else now - 300
                )
        self.books = {
            symbol: OrderBookSnapshot(
                timestamp=now, symbol=symbol,
                bids=[OrderBookLevel(price=price - 1, size=1.0)],
                asks=[OrderBookLevel(price=price + 1, size=1.0)],
                imbalance=0.1, best_bid=price - 1, best_ask=price + 1, spread=2.0,
            )
            for symbol, price in (("BTC", 70000.0), ("ETH", 3500.0))
        }

    def get_all_assets(self) -> list[AssetInfo]:
        return sorted(self.market.assets.values(), key=lambda asset: asset.volume_24h, reverse=True)

    def get_orderbook(self, symbol: str):
        return self.books.get(symbol)


@pytest.mark.asyncio
async def test_healthz_rejects_stale_or_failed_native_feed() -> None:
    from apps.screener_web.__main__ import healthz

    hub = SimpleNamespace(
        ready=True, last_error=None,
        status=SimpleNamespace(mode="live", market_data="connected"),
        warnings=[],
    )
    app = web.Application()
    app["hub"] = hub
    app.router.add_get("/healthz", healthz)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        assert (await client.get("/healthz")).status == 200
        hub.status.market_data = "error"
        assert (await client.get("/healthz")).status == 503
        hub.status.market_data = "connected"
        hub.last_error = "boom"
        assert (await client.get("/healthz")).status == 503
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_native_snapshot_flows_through_remote_hub_into_screener(tmp_path) -> None:
    now = time.time()
    fake = FakeHub(now)
    api = HyperDataAPI(hub=fake)
    app = web.Application()
    app.router.add_get("/v1/market", api.handle_market)
    app.router.add_get("/v1/snapshot", api.handle_snapshot)
    app.router.add_get("/v1/candles", api.handle_candles)
    client = TestClient(TestServer(app))
    await client.start_server()
    remote = RemoteHub(str(client.server.make_url("")).rstrip("/"))
    journal = Journal(tmp_path / "screener.sqlite")
    try:
        await remote.start()
        assert remote.ready is True
        assert [asset.symbol for asset in remote.get_all_assets()] == ["BTC", "ETH"]
        assert remote.orderflow.get_snapshot("BTC", "1m").trade_count == 1

        service = Screener(
            remote, journal,
            SimpleNamespace(key=None, provider="test", analyze=None), False,
        )
        service.scan(now)
        assert set(service.rows) == {
            ("BTC", "1m"), ("BTC", "5m"), ("ETH", "1m"), ("ETH", "5m"),
        }
        row = service.rows[("BTC", "1m")]
        assert row["snapshot"]["metrics"]["cvd"]["value"] == 70000.0
        assert row["snapshot"]["metrics"]["price"]["value"] == 70000.0
        assert row["hyperdata"]["direction"] == "BULLISH"
    finally:
        await remote.stop()
        journal.close()
        await client.close()
