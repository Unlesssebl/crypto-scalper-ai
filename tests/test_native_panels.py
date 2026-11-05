"""Renderers for the native panels shared by the TUI entry points."""
from __future__ import annotations

from rich.console import Console

from hyperdata_terminal.dashboards.native_panels import (
    danger_zone_panel,
    liquidations_panel,
    orderflow_panel,
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
