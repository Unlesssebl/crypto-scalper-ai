"""Tests for the API startup universe prefetch."""
from __future__ import annotations

import pytest

from hyperdata_terminal import cli
from hyperdata_terminal.data_layer.market_data import AssetInfo, MarketData


def _asset(symbol: str, volume: float) -> AssetInfo:
    return AssetInfo(
        symbol=symbol, price=1.0, funding_rate=0.0, open_interest=1.0,
        volume_24h=volume, price_change_24h_pct=0.0, mark_price=1.0, index_price=1.0,
    )


@pytest.mark.asyncio
async def test_api_universe_ranks_by_volume_and_limits(monkeypatch) -> None:
    async def fake_refresh(self) -> None:
        self.assets = {"A": _asset("A", 10.0), "B": _asset("B", 30.0), "C": _asset("C", 20.0)}

    monkeypatch.setattr(MarketData, "refresh", fake_refresh)
    assert await cli._api_universe(2) == ["B", "C"]


@pytest.mark.asyncio
async def test_api_universe_falls_back_on_failure(monkeypatch) -> None:
    async def broken_refresh(self) -> None:
        raise RuntimeError("exchange unavailable")

    monkeypatch.setattr(MarketData, "refresh", broken_refresh)
    assert await cli._api_universe() is None
