"""Contract tests for the batch `/v1/snapshot` builder."""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from hyperdata_terminal.data_layer.market_data import AssetInfo
from hyperdata_terminal.data_layer.orderbook import OrderBookLevel, OrderBookSnapshot
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine, ShardState, Trade
from hyperdata_terminal.screener_snapshot import build_candles, build_extra, build_health, build_snapshot


def _trade(side: str, price: float, size: float, ts: float, symbol: str = "BTC") -> Trade:
    return Trade(timestamp=ts, symbol=symbol, side=side, price=price, size=size, size_usd=price * size)


def _asset(symbol: str = "BTC", price: float = 70000.0) -> AssetInfo:
    return AssetInfo(
        symbol=symbol, price=price, funding_rate=0.00001,
        open_interest=1_000_000.0, volume_24h=5_000_000.0,
        price_change_24h_pct=0.01, mark_price=price, index_price=price * 0.999,
    )


class FakeHub:
    def __init__(self, *, demo: bool = False, candles: list[dict] | None = None,
                 symbol: str = "BTC", price: float = 70000.0) -> None:
        self.demo = demo
        self.symbol = symbol
        self.orderflow = OrderFlowEngine(symbols=[symbol])
        self.market = SimpleNamespace(
            assets={symbol: _asset(symbol, price)},
            observed_fields={symbol: {"price", "volume_24h", "open_interest", "funding_rate"}},
            observed_at=time.time(),
            get_candles=self._get_candles,
        )
        self._candles = candles or []
        self.book = OrderBookSnapshot(
            timestamp=time.time(), symbol=symbol,
            bids=[OrderBookLevel(price=price - 1, size=1.0)],
            asks=[OrderBookLevel(price=price + 1, size=1.0)],
            imbalance=0.1, best_bid=price - 1, best_ask=price + 1, spread=2.0,
        )

    async def _get_candles(self, symbol: str, interval: str, bars: int = 100) -> list[dict]:
        return self._candles

    def get_all_assets(self) -> list[AssetInfo]:
        return list(self.market.assets.values())

    def get_orderbook(self, symbol: str):
        return self.book if symbol == self.symbol else None


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
async def test_snapshot_resolves_mixed_case_ticker() -> None:
    now = time.time()
    hub = FakeHub(symbol="kPEPE", price=0.0123)
    hub.orderflow._hl_shard_plan = [["kPEPE"]]
    hub.orderflow._hl_shard_state[0] = ShardState(connected_at=now - 120)
    hub.orderflow._process_trade(_trade("buy", 0.0123, 1000.0, now, symbol="kPEPE"), venue="hyperliquid")
    hub.orderflow.venue_buckets["hyperliquid"]["kPEPE"]["1m"].first_trade_at = now - 60

    payload = await build_snapshot(hub, symbols=["kpepe"], windows=["1m"], now=now, include_candles=False)
    assert payload["rows"][0]["symbol"] == "kPEPE"
    assert payload["rows"][0]["market"]["price"] == 0.0123


@pytest.mark.asyncio
async def test_candles_resolve_mixed_case_ticker() -> None:
    now = time.time()
    hub = FakeHub(symbol="kPEPE", candles=_candles(now))
    payload = await build_candles(hub, ["KPEPE"], interval="5m", bars=50)
    assert payload["series"][0]["symbol"] == "kPEPE"


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


class _ContextHub(FakeHub):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.lsr = SimpleNamespace(get_latest=lambda s: SimpleNamespace(
            long_short_ratio=1.2, long_ratio=0.55, short_ratio=0.45,
            timestamp=time.time(), source="binance"))
        self.spot = SimpleNamespace(get_latest=lambda s: SimpleNamespace(
            spot_price=69900.0, perp_price=70000.0, basis_pct=0.143,
            timestamp=time.time(), source="binance"))
        self.deribit = SimpleNamespace(get_latest=lambda s: SimpleNamespace(
            mark_iv=45.0, index_price=70000.0, oi_usd=1_000_000_000.0, timestamp=time.time()))
        self.liquidations = SimpleNamespace(get_stats=lambda **k: {
            "window_minutes": k["window_minutes"], "confirmed_count": 3,
            "confirmed_volume_usd": 150_000.0, "long_count": 2, "short_count": 1,
            "by_symbol": {"BTC": {"count": 3, "volume_usd": 150_000.0}},
            "coverage": {"binance": {"method": "ws"}},
        })

    def get_whale_positions(self, min_size_usd=100_000.0):
        position = SimpleNamespace(symbol="BTC", size_usd=1_000_000.0,
                                   leverage=5.0, distance_pct=12.0)
        return [position]

    def get_extreme_funding(self, threshold_annualized=0.10):
        return [SimpleNamespace(symbol="SOL", funding_rate=0.00002)]


@ pytest.mark.asyncio
async def test_snapshot_includes_market_context() -> None:
    hub = _ContextHub()
    payload = await build_snapshot(hub, symbols=["BTC"], windows=["1m"],
                                   now=time.time(), include_candles=False)
    context = payload["context"]
    assert context["lsr"]["BTC"]["long_short_ratio"] == 1.2
    assert context["basis"]["BTC"]["basis_pct"] == 0.143
    assert context["iv"]["BTC"]["mark_iv"] == 45.0
    assert context["liquidations"]["confirmed_count"] == 3
    assert context["whales"][0]["symbol"] == "BTC"
    assert context["extreme_funding"][0]["annualized_pct"] > 0


class _ExtraHub:
    class _OF:
        def get_snapshot(self, symbol, window):
            return SimpleNamespace(signal="BULLISH", cvd=100.0, ofi=0.2,
                                   trade_count=5, warming_up=False)

    class _Liq:
        def get_stats(self, **kwargs):
            return {"confirmed_count": 1, "confirmed_volume_usd": 10.0,
                    "by_exchange": {}, "by_symbol": {}}

    class _Pos:
        market_prices = {"BTC": 100000.0}
        positions = []

        def get_danger_zone(self, threshold_pct=5.0):
            return []

        def scan_age_seconds(self):
            return 1.0

    class _HLP:
        def get_stats(self):
            return {"account_value": 400_000_000.0, "num_positions": 1,
                    "net_delta": 1.0, "delta_zscore": 0.0, "total_exposure": 1.0,
                    "session_pnl": 0.0, "pnl_known": True, "total_trades": 0}

        def get_latest_snapshot(self):
            return object()

        def get_top_positions(self, n):
            return []

        def get_liquidation_absorptions(self, minutes):
            return []

    class _SM:
        def get_stats(self):
            return {"total_wallets": 0, "ranked_wallets": 0,
                    "smart_wallets": 0, "dumb_wallets": 0}

    orderflow = _OF()
    liquidations = _Liq()
    positions = _Pos()
    hlp = _HLP()
    smart_money = _SM()

    def get_all_assets(self):
        return [SimpleNamespace(symbol="BTC")]

    def get_whale_positions(self, min_size_usd=250_000.0):
        return []

    def get_smart_money(self, n=20):
        return []

    def get_dumb_money(self, n=20):
        return []

    def get_smart_money_signals(self, n=50):
        return []


def test_build_extra_collects_native_panels() -> None:
    extra = build_extra(_ExtraHub())
    assert extra["orderflow"][0]["symbol"] == "BTC"
    assert extra["orderflow"][0]["venue"] == "combined"
    assert extra["orderflow"][0]["quality"] == "ok"
    assert extra["liquidations"]["confirmed_count"] == 1
    assert extra["danger_zone"]["positions"] == []
    assert extra["hlp"]["account_value"] == 400_000_000.0
    assert extra["heatmap"]["symbol"] == "BTC"
    assert extra["smart_money"]["stats"]["total_wallets"] == 0


def test_build_health_reports_feeds_and_venues() -> None:
    hub = FakeHub()
    hub.status = SimpleNamespace(
        liquidation_feed="connected", orderflow_engine="partial",
        orderbook_feed="connected", market_data="connected", hlp_status="connected",
        failed_components=[], mode="live",
    )
    hub.health = SimpleNamespace(latest=lambda: {"overall": "warn"})
    health = build_health(hub)
    assert health["overall"] == "warn"
    assert health["feeds"]["orderflow_engine"] == "partial"
    assert set(health["orderflow_venues"]) == {"hyperliquid", "binance"}


@ pytest.mark.asyncio
async def test_snapshot_context_is_empty_for_bare_hub() -> None:
    hub = FakeHub()
    payload = await build_snapshot(hub, symbols=["BTC"], windows=["1m"],
                                   now=time.time(), include_candles=False)
    assert payload["context"] == {}


@ pytest.mark.asyncio
async def test_snapshot_warns_when_universe_drifts() -> None:
    hub = FakeHub()  # orderflow subscribes only `BTC`
    # Add a top-volume asset the orderflow engine does not know about.
    hub.market = SimpleNamespace(
        assets={"BTC": _asset("BTC"), "NEWCOIN": _asset("NEWCOIN", price=5.0)},
        observed_fields={"BTC": {"price"}, "NEWCOIN": {"price"}},
        observed_at=time.time(),
        get_candles=hub._get_candles,
    )
    payload = await build_snapshot(hub, symbols=["BTC"], windows=["1m"],
                                   now=time.time(), include_candles=False)
    assert any("universe drifted" in warning for warning in payload["warnings"])


@ pytest.mark.asyncio
async def test_snapshot_no_drift_warning_when_subscribed() -> None:
    hub = FakeHub()  # orderflow subscribes `BTC`; market only has `BTC`
    payload = await build_snapshot(hub, symbols=["BTC"], windows=["1m"],
                                   now=time.time(), include_candles=False)
    assert not any("universe drifted" in warning for warning in payload["warnings"])
