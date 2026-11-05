"""Renderers for the native panels shared by the TUI entry points."""
from __future__ import annotations

from rich.console import Console

from hyperdata_terminal.dashboards.native_panels import (
    danger_zone_panel,
    heatmap_panel,
    hlp_panel,
    liquidations_panel,
    orderflow_panel,
    smart_money_panel,
    whales_panel,
)


def _render(renderable) -> str:
    console = Console(width=120, record=True, force_terminal=False)
    console.print(renderable)
    return console.export_text()


def test_empty_panels_return_none():
    assert orderflow_panel([]) is None
    assert liquidations_panel(None) is None
    assert danger_zone_panel({}) is None
    assert whales_panel({"positions": []}) is None
    assert hlp_panel({"warmup": True}) is None
    assert heatmap_panel({"buckets": []}) is None
    assert smart_money_panel({}) is None


def test_orderflow_panel_shows_venue_and_values():
    text = _render(orderflow_panel([{
        "symbol": "BTC", "window": "1m", "signal": "STRONG_BULL",
        "venue": "combined", "cvd": 12457.75, "ofi": 0.84,
        "trades": 51, "quality": "ok",
    }]))
    assert "ORDER FLOW" in text
    assert "BTC" in text
    assert "STRONG_BULL" in text
    assert "combined" in text
    assert "+84%" in text


def test_liquidations_panel_totals_and_exchanges():
    text = _render(liquidations_panel({
        "confirmed_count": 3, "confirmed_volume_usd": 250_000.0,
        "by_exchange": {"bybit": {"count": 2, "volume_usd": 200_000.0}},
        "by_symbol": {"BTC": {"count": 1, "volume_usd": 50_000.0}},
    }))
    assert "LIQUIDATIONS" in text
    assert "TOTAL" in text
    assert "bybit" in text
    assert "BTC" in text


def test_positions_panels_render_side_and_distance():
    positions = {"positions": [{
        "symbol": "ETH", "side": "short", "size_usd": 1_200_000.0,
        "leverage": 10.0, "distance_pct": 2.4,
    }]}
    danger = _render(danger_zone_panel(positions))
    whales = _render(whales_panel(positions))
    assert "DANGER ZONE" in danger and "ETH" in danger and "2.4%" in danger
    assert "LARGEST POSITIONS" in whales and "10x" in whales


def test_hlp_panel_renders_vault_and_positions():
    text = _render(hlp_panel({
        "account_value": 400_000_000.0, "session_pnl": 1500.0, "pnl_known": True,
        "num_positions": 2, "net_delta": 12_000_000.0, "delta_zscore": 0.4,
        "total_exposure": 90_000_000.0, "total_trades": 5, "absorbed_24h": 1,
        "warmup": False,
        "positions": [{"symbol": "BTC", "side": "long", "size_usd": 5_000_000.0,
                       "unrealized_pnl": 1000.0, "leverage": 5.0}],
    }))
    assert "HLP VAULT" in text
    assert "AUM" in text and "Net" in text
    assert "BTC" in text and "5x" in text


def test_heatmap_panel_buckets_around_price():
    text = _render(heatmap_panel({
        "symbol": "BTC", "price": 100_000.0, "range_pct": 10.0,
        "buckets": [
            {"mid": 92_000.0, "long_usd": 1_000_000.0, "short_usd": 0.0},
            {"mid": 100_500.0, "long_usd": 0.0, "short_usd": 500_000.0},
        ],
    }))
    assert "LIQ HEATMAP" in text
    assert "92,000" in text or "92000" in text
    assert "100,500" in text or "100500" in text


def test_smart_money_panel_renders_wallets_and_signals():
    text = _render(smart_money_panel({
        "stats": {"total_wallets": 10, "ranked_wallets": 5,
                  "smart_wallets": 3, "dumb_wallets": 2},
        "smart": [{"rank": 1, "win_rate": 0.62, "confidence": 0.8,
                   "realized_pnl": 12_000.0, "account_value": 500_000.0,
                   "active_symbols": ["BTC"]}],
        "signals": [{"timestamp": 1_700_000_000, "tier": "smart",
                     "action": "OPEN_LONG", "symbol": "BTC", "size_usd": 250_000.0,
                     "wallet_rank": 1, "wallet_win_rate": 0.62,
                     "signal_type": "follow", "wallet_confidence": 0.8}],
    }))
    assert "SMART MONEY" in text
    assert "#1" in text and "62%" in text
    assert "FOLLOW" in text and "OPEN_LONG" in text
