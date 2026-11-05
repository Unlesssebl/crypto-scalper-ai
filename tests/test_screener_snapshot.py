"""Contract tests for the batch `/v1/snapshot` builder."""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from hyperdata_terminal.data_layer.market_data import AssetInfo
from hyperdata_terminal.data_layer.orderbook import OrderBookLevel, OrderBookSnapshot
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine, ShardState, Trade
from hyperdata_terminal.screener_snapshot import build_candles, build_snapshot


def _trade(side: str, price: float, size: float, ts: float) -> Trade:
    return Trade(timestamp=ts, symbol="BTC", side=side, price=price, size=size, size_usd=price * size)


def _asset() -> AssetInfo:
    return AssetInfo(
        symbol="BTC", price=70000.0, funding_rate=0.00001,
        open_interest=1_000_000.0, volume_24h=5_000_000.0,
        price_change_24h_pct=0.01, mark_price=70000.0, index_price=69990.0,
    )


class FakeHub:
    def __init__(self, *, demo: bool = False, candles: list[dict] | None = None) -> None:
        self.demo = demo
        self.orderflow = OrderFlowEngine(symbols=["BTC"])
        self.market = SimpleNamespace(
            assets={"BTC": _asset()},
            observed_fields={"BTC": {"price", "volume_24h", "open_interest", "funding_rate"}},
            observed_at=time.time(),
            get_candles=self._get_candles,
        )
        self._candles = candles or []
        self.book = OrderBookSnapshot(
            timestamp=time.time(), symbol="BTC",
            bids=[OrderBookLevel(price=69999.0, size=1.0)],
            asks=[OrderBookLevel(price=70001.0, size=1.0)],
            imbalance=0.1, best_bid=69999.0, best_ask=70001.0, spread=2.0,
        )

    async def _get_candles(self, symbol: str, interval: str, bars: int = 100) -> list[dict]:
        return self._candles

    def get_all_assets(self) -> list[AssetInfo]:
        return list(self.market.assets.values())

    def get_orderbook(self, symbol: str):
        return self.book if symbol == "BTC" else None


def _candles(now: float, count: int = 50) -> list[dict]:
    start = int((now - count * 60) // 60 * 60)
    return [
        {
            "timestamp": (start + i * 60) * 1000,
            "open": 70000.0, "high": 70100.0, "low": 69900.0, "close": 70050.0,
            "volume": 10.0, "trade_count": 20,
        }
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_snapshot_live_row_uses_hyperliquid_flow() -> None:
    now = time.time()
    hub = FakeHub()
    hub.orderflow._hl_shard_plan = [["BTC"]]
    hub.orderflow._hl_shard_state[0] = ShardState(connected_at=now - 120)
    hub.orderflow._process_trade(_trade("buy", 70000.0, 1.0, now), venue="hyperliquid")
    hub.orderflow._process_trade(_trade("sell", 70000.0, 2.0, now), venue="binance")
    hub.orderflow.venue_buckets["hyperliquid"]["BTC"]["1m"].first_trade_at = now - 60

    payload = await build_snapshot(hub, symbols=["BTC"], windows=["1m"], now=now, include_candles=False)

    assert payload["mode"] == "live"
    assert payload["market"]["quality"] == "ok"
    row = payload["rows"][0]
    assert row["flow"]["venue"] == "hyperliquid"
    assert row["flow"]["trade_count"] == 1
    assert row["flow"]["first_trade_price"] == 70000.0
    assert row["flow"]["last_trade_price"] == 70000.0
    assert row["flow"]["quality"] == "ok"
    assert row["book"]["quality"] == "ok"
    assert row["market"]["price"] == 70000.0
    assert row["candles"]["quality"] == "unavailable"


@pytest.mark.asyncio
async def test_snapshot_demo_uses_combined_flow_and_synthetic_quality() -> None:
    now = time.time()
    hub = FakeHub(demo=True)
    hub.orderflow._process_trade(_trade("buy", 70000.0, 1.0, now))

    payload = await build_snapshot(hub, symbols=["BTC"], windows=["1m"], now=now, include_candles=False)

    row = payload["rows"][0]
    assert payload["mode"] == "demo"
    assert row["flow"]["venue"] == "combined"
    assert row["flow"]["quality"] == "synthetic"
    assert row["book"]["quality"] == "synthetic"
    assert row["market"]["quality"] == "synthetic"
    assert row["market"]["price"] == 70000.0


@pytest.mark.asyncio
async def test_snapshot_rejects_unknown_window() -> None:
    hub = FakeHub()
    with pytest.raises(ValueError, match="unsupported windows"):
        await build_snapshot(hub, symbols=["BTC"], windows=["3m"], include_candles=False)


@pytest.mark.asyncio
async def test_snapshot_warns_for_unlisted_symbol() -> None:
    hub = FakeHub()
    payload = await build_snapshot(hub, symbols=["NOPE"], windows=["1m"], include_candles=False)
    assert payload["rows"] == []
    assert any("NOPE" in warning for warning in payload["warnings"])


@pytest.mark.asyncio
async def test_snapshot_and_candles_routes() -> None:
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    from hyperdata_terminal.api_server import HyperDataAPI

    now = time.time()
    hub = FakeHub(candles=_candles(now))
    hub.orderflow._hl_shard_plan = [["BTC"]]
    hub.orderflow._hl_shard_state[0] = ShardState(connected_at=now - 120)
    hub.orderflow._process_trade(_trade("buy", 70000.0, 1.0, now), venue="hyperliquid")
    hub.orderflow.venue_buckets["hyperliquid"]["BTC"]["1m"].first_trade_at = now - 60
    api = HyperDataAPI(hub=hub)
    app = web.Application()
    app.router.add_get("/v1/snapshot", api.handle_snapshot)
    app.router.add_get("/v1/candles", api.handle_candles)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.get("/v1/snapshot?symbols=BTC&windows=1m&include_candles=0")
        assert response.status == 200
        body = await response.json()
        assert body["rows"][0]["symbol"] == "BTC"
        assert body["rows"][0]["flow"]["trade_count"] == 1

        bad = await client.get("/v1/snapshot?windows=3m")
        assert bad.status == 400

        candles = await client.get("/v1/candles?symbols=BTC&interval=5m&bars=50")
        assert candles.status == 200
        assert len((await candles.json())["series"][0]["candles"]) == 50
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_candles_builder_returns_raw_series() -> None:
    now = time.time()
    hub = FakeHub(candles=_candles(now))
    payload = await build_candles(hub, ["BTC"], interval="5m", bars=50)
    assert payload["series"][0]["symbol"] == "BTC"
    assert len(payload["series"][0]["candles"]) == 50
