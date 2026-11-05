"""Tests for the native-API adapter used by the external screener."""
from __future__ import annotations

import time

import pytest

from extensions.scalper.market import capture
from extensions.scalper.remote import RemoteHub


def _payload(now: float) -> dict:
    return {
        "as_of": now,
        "mode": "live",
        "market": {"age_seconds": 1.0, "observed_at": now, "quality": "ok"},
        "warnings": [],
        "rows": [{
            "symbol": "BTC", "window": "1m",
            "market": {
                "price": 70000.0, "volume_24h": 5_000_000.0,
                "open_interest": 1_000_000.0, "funding_rate": 0.00001,
                "observed_fields": ["price", "volume_24h", "open_interest", "funding_rate"],
                "quality": "ok", "source": "hyperliquid_meta_and_asset_ctxs",
            },
            "flow": {
                "cvd": 70000.0, "ofi": 0.2, "volume": 140000.0,
                "trade_count": 2, "trades_per_second": 0.033,
                "coverage": 0.98, "warming_up": False, "signal": "BULLISH",
                "first_trade_price": 69900.0, "last_trade_price": 70100.0,
                "connected_at": now - 120, "last_trade_at": now,
                "quality": "ok", "venue": "hyperliquid", "source": "hyperliquid_trades",
            },
            "book": {
                "spread": 2.0, "imbalance": 0.1, "best_bid": 69999.0,
                "best_ask": 70001.0, "timestamp": now, "quality": "ok",
                "source": "hyperliquid_l2book",
            },
            "candles": {
                "natr": 0.5, "range_ratio": 1.2, "tape_baseline": 20.0,
                "closed_at": now - 60, "quality": "ok", "source": "hyperliquid_candles",
            },
        }],
    }


def _assets() -> list[dict]:
    return [{
        "symbol": "BTC", "price": 70000.0, "funding_rate": 0.00001,
        "open_interest": 1_000_000.0, "volume_24h": 5_000_000.0,
        "price_change_24h_pct": 0.01, "mark_price": 70000.0,
        "index_price": 69990.0, "premium_pct": 0.01,
    }]


def test_remote_hub_apply_builds_capture_compatible_state() -> None:
    now = time.time()
    hub = RemoteHub("http://127.0.0.1:8420")
    hub._apply(_payload(now), _assets())

    assert hub.demo is False
    assert hub.market_timestamp() == now
    assert hub.market_age() < 1
    assert [asset.symbol for asset in hub.get_all_assets()] == ["BTC"]

    flow = hub.orderflow.get_snapshot("BTC", "1m")
    assert flow.trade_count == 2
    assert flow.signal == "BULLISH"
    assert hub.orderflow.hyperliquid_connected_at("BTC") == now - 120

    book = hub.get_orderbook("BTC")
    assert book is not None and book.best_bid == 69999.0

    snapshot, native = capture(hub, hub.get_all_assets()[0], "1m", now)
    assert snapshot["metrics"]["cvd"]["value"] == 70000.0
    assert snapshot["metrics"]["first_trade_price"]["value"] == 69900.0
    assert snapshot["metrics"]["last_trade_price"]["value"] == 70100.0
    assert snapshot["metrics"]["natr"]["quality"] == "unavailable"
    assert native["direction"] == "BULLISH"
    assert native["quality"] == "ok"


@pytest.mark.asyncio
async def test_remote_hub_get_candles_uses_native_series(monkeypatch) -> None:
    hub = RemoteHub("http://127.0.0.1:8420")

    async def fake_get(path: str, **params):
        assert path == "/v1/candles"
        assert params["symbols"] == "BTC"
        return {"series": [{"symbol": "BTC", "interval": "5m", "candles": [{"timestamp": 1}]}]}

    monkeypatch.setattr(hub, "_get", fake_get)
    candles = await hub.get_candles("btc", "5m", bars=36)
    assert candles == [{"timestamp": 1}]
