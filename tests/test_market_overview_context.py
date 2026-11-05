"""The Market Overview TUI renders the native market-context block."""
from __future__ import annotations

import pytest

from hyperdata_terminal.dashboards.market_overview import MarketOverviewDashboard
from hyperdata_terminal.data_layer.market_data import AssetInfo
from hyperdata_terminal.screener_snapshot import build_context


def _asset(symbol: str = "BTC", price: float = 70000.0) -> AssetInfo:
    return AssetInfo(symbol, price, 0.00001, 1_000_000.0, 5_000_000.0, 0.01, price, price)


CONTEXT = {
    "lsr": {"BTC": {"long_short_ratio": 1.8, "source": "okx"},
            "ETH": {"long_short_ratio": 1.1, "source": "okx"}},
    "basis": {"BTC": {"basis_pct": 0.12, "source": "coinbase"}},
    "iv": {"BTC": {"mark_iv": 45.0}, "ETH": {"mark_iv": 51.0}},
    "liquidations": {"confirmed_count": 4, "confirmed_volume_usd": 250_000},
    "whales": [{"symbol": "BTC", "size_usd": 1_200_000}],
    "extreme_funding": [{"symbol": "MINA", "annualized_pct": -303.9}],
}


def test_context_panel_is_none_without_context():
    dashboard = MarketOverviewDashboard()
    assert dashboard.build_context_panel({}) is None
    assert dashboard.build_context_panel(None) is None


def test_context_panel_renders_native_block():
    dashboard = MarketOverviewDashboard()
    table = dashboard.build_context_panel(CONTEXT)
    assert table is not None
    titles = " ".join(column.header for column in table.columns)
    assert "Signal" in titles and "BTC" in titles


def test_build_dashboard_includes_context_without_assets_error():
    dashboard = MarketOverviewDashboard()
    dashboard.assets = [_asset()]
    dashboard.context = CONTEXT
    panel = dashboard.build_dashboard()
    assert panel is not None


def test_context_panel_uses_fmt_usd_for_whales():
    dashboard = MarketOverviewDashboard()
    table = dashboard.build_context_panel({"whales": [{"symbol": "BTC", "size_usd": 1_200_000}]})
    assert table is not None


@pytest.mark.asyncio
async def test_update_data_reads_context_reader():
    dashboard = MarketOverviewDashboard(
        asset_reader=lambda: [_asset()],
        context_reader=lambda: CONTEXT,
    )
    await dashboard.update_data()
    assert dashboard.context["lsr"]["BTC"]["long_short_ratio"] == 1.8


@pytest.mark.asyncio
async def test_context_reader_failure_does_not_break_market_table():
    def boom():
        raise RuntimeError("native API down")

    dashboard = MarketOverviewDashboard(asset_reader=lambda: [_asset()], context_reader=boom)
    await dashboard.update_data()
    assert dashboard.context == {}
    assert len(dashboard.assets) == 1


class _Hub:
    """Minimal hub exposing the sub-engines build_context reads."""

    demo = False

    class _Asset:
        symbol = "BTC"
        price = 70000.0
        mark_price = 70000.0
        index_price = 69990.0
        premium_pct = 0.01
        funding_rate = 0.00001
        open_interest = 1_000_000.0
        volume_24h = 5_000_000.0
        price_change_24h_pct = 0.01

    class _Engine:
        def get_latest(self, symbol):
            return None

    class _Liq:
        def get_stats(self, **kwargs):
            return {"window_minutes": 60, "confirmed_count": 0,
                    "confirmed_volume_usd": 0.0, "long_count": 0, "short_count": 0,
                    "by_symbol": {}, "coverage": {}}

    def __init__(self):
        self.lsr = self._Engine()
        self.spot = self._Engine()
        self.deribit = self._Engine()
        self.liquidations = self._Liq()
        self.market = type("M", (), {"assets": {}})()

    def get_whale_positions(self, min_size_usd=250_000.0):
        return []

    def get_extreme_funding(self, threshold_annualized=0.10):
        return []


def test_build_context_is_safe_on_empty_hub():
    context = build_context(_Hub())
    assert isinstance(context, dict)
    assert "as_of" in context
    assert "lsr" not in context and "basis" not in context and "iv" not in context


def test_header_renders_health_badge_and_flow_label():
    dashboard = MarketOverviewDashboard()
    dashboard.assets = [_asset()]
    dashboard.health = {
        "overall": "warn",
        "orderflow_venues": {
            "hyperliquid": {"status": "ok"},
            "binance": {"status": "silent"},
        },
    }
    header = dashboard.build_header().plain
    assert "⚠ PARTIAL" in header
    assert "Flow: HL" in header
    assert "BN" not in header


def test_header_marks_live_when_health_is_ok():
    dashboard = MarketOverviewDashboard()
    dashboard.assets = [_asset()]
    dashboard.health = {"overall": "ok", "orderflow_venues": {
        "hyperliquid": {"status": "ok"}, "binance": {"status": "ok"}}}
    header = dashboard.build_header().plain
    assert "✓ LIVE" in header
    assert "Flow: HL + BN" in header


@pytest.mark.asyncio
async def test_update_data_reads_health_reader():
    dashboard = MarketOverviewDashboard(
        asset_reader=lambda: [_asset()],
        health_reader=lambda: {"overall": "ok"},
    )
    await dashboard.update_data()
    assert dashboard.health["overall"] == "ok"


@pytest.mark.asyncio
async def test_update_data_reads_async_extra_reader_and_renders_panels():
    async def extra():
        return {
            "orderflow": [{"symbol": "BTC", "signal": "BULLISH", "venue": "combined",
                           "cvd": 100.0, "ofi": 0.2, "trades": 5, "quality": "ok"}],
            "liquidations": {"confirmed_count": 1, "confirmed_volume_usd": 1000.0},
            "danger_zone": {"positions": [{"symbol": "ETH", "side": "short",
                                           "size_usd": 1e6, "leverage": 5, "distance_pct": 2.0}]},
            "whales": {"positions": [{"symbol": "BTC", "side": "long",
                                      "size_usd": 2e6, "leverage": 10, "distance_pct": 8.0}]},
        }

    dashboard = MarketOverviewDashboard(asset_reader=lambda: [_asset()], extra_reader=extra)
    await dashboard.update_data()
    assert dashboard.extra["orderflow"][0]["symbol"] == "BTC"
    assert dashboard.build_dashboard() is not None


@pytest.mark.asyncio
async def test_extra_reader_failure_does_not_break_dashboard():
    async def boom():
        raise RuntimeError("native API down")

    dashboard = MarketOverviewDashboard(asset_reader=lambda: [_asset()], extra_reader=boom)
    await dashboard.update_data()
    assert dashboard.extra == {}
    assert len(dashboard.assets) == 1
