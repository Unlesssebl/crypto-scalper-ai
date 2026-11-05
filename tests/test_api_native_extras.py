"""New native API surfaces: HLP, liquidation heatmap and smart money."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from hyperdata_terminal.api_server import HyperDataAPI


class _Positions:
    def __init__(self) -> None:
        self.positions = [
            SimpleNamespace(symbol="BTC", side="long", liq_price=92_000.0, size_usd=1_000_000.0),
            SimpleNamespace(symbol="BTC", side="short", liq_price=108_000.0, size_usd=500_000.0),
        ]
        self.market_prices = {"BTC": 100_000.0}

    def as_of(self, items=None) -> float:
        return 0.0

    def scan_age_seconds(self) -> float:
        return 1.0


class _HLP:
    def get_stats(self) -> dict:
        return {
            "account_value": 400_000_000.0, "net_delta": 12_000_000.0,
            "delta_zscore": 0.4, "total_exposure": 90_000_000.0,
            "num_positions": 2, "session_pnl": 1_500.0, "pnl_known": True,
            "total_unrealized_pnl": 900.0, "total_snapshots": 10,
            "total_trades": 5, "liquidation_absorptions": 1, "aum_source": "vaultDetails",
        }

    def get_latest_snapshot(self):
        return object()

    def get_top_positions(self, n: int):
        return [{"symbol": "BTC", "side": "long", "size_usd": 5_000_000.0,
                 "unrealized_pnl": 1000.0, "leverage": 5.0}][:n]

    def get_liquidation_absorptions(self, minutes: int):
        return []


class _SmartMoney:
    def get_stats(self) -> dict:
        return {"total_wallets": 10, "ranked_wallets": 5, "smart_wallets": 3,
                "dumb_wallets": 2, "total_signals": 7}


class _Hub:
    def __init__(self) -> None:
        self.positions = _Positions()
        self.hlp = _HLP()
        self.smart_money = _SmartMoney()
        self.market = SimpleNamespace(assets={
            "BTC": SimpleNamespace(mark_price=100_000.0, price=100_000.0)})

    def get_smart_money(self, n: int):
        return [SimpleNamespace(rank=1, tier="smart", win_rate=0.62, confidence=0.8,
                                total_realized_pnl=12_000.0, account_value=500_000.0,
                                active_symbols=["BTC", "ETH"])]

    def get_dumb_money(self, n: int):
        return [SimpleNamespace(rank=99, tier="dumb", win_rate=0.31, confidence=0.6,
                                total_realized_pnl=-8_000.0, account_value=90_000.0,
                                active_symbols=["SOL"])]

    def get_smart_money_signals(self, n: int):
        return [SimpleNamespace(timestamp=1.0, tier="smart", action="OPEN_LONG",
                                symbol="BTC", size_usd=250_000.0, wallet_rank=1,
                                wallet_win_rate=0.62, signal_type="follow",
                                wallet_confidence=0.8)]


def _app() -> web.Application:
    api = HyperDataAPI(hub=_Hub())
    app = web.Application()
    app.router.add_get("/v1/positions/heatmap", api.handle_heatmap)
    app.router.add_get("/v1/hlp", api.handle_hlp)
    app.router.add_get("/v1/smart-money", api.handle_smart_money)
    return app


@pytest.mark.asyncio
async def test_hlp_endpoint_reports_vault_and_positions() -> None:
    async with TestClient(TestServer(_app())) as client:
        response = await client.get("/v1/hlp")
        assert response.status == 200
        body = await response.json()
    assert body["account_value"] == 400_000_000.0
    assert body["num_positions"] == 2
    assert body["warmup"] is False
    assert body["positions"][0]["symbol"] == "BTC"
    assert body["absorbed_24h"] == 0


@pytest.mark.asyncio
async def test_heatmap_endpoint_buckets_liquidations() -> None:
    async with TestClient(TestServer(_app())) as client:
        response = await client.get("/v1/positions/heatmap?symbol=BTC&buckets=20&range_pct=15")
        assert response.status == 200
        body = await response.json()
    assert body["symbol"] == "BTC"
    assert body["price"] == 100_000.0
    assert len(body["buckets"]) == 20
    long_usd = sum(bucket["long_usd"] for bucket in body["buckets"])
    short_usd = sum(bucket["short_usd"] for bucket in body["buckets"])
    assert long_usd == 1_000_000.0
    assert short_usd == 500_000.0


@pytest.mark.asyncio
async def test_smart_money_endpoint_omits_addresses() -> None:
    async with TestClient(TestServer(_app())) as client:
        response = await client.get("/v1/smart-money")
        assert response.status == 200
        body = await response.json()
    assert body["stats"]["smart_wallets"] == 3
    assert body["smart"][0]["rank"] == 1
    assert "address" not in body["smart"][0]
    assert body["signals"][0]["action"] == "OPEN_LONG"
