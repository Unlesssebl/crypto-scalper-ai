"""No instrument is pinned, excluded by name, or added outside the attention plan."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.activity import observation_assets
from extensions.scalper.history.pulse_runtime import PulseObservation
from extensions.scalper.remote import RemoteHub
from extensions.scalper.unified_attention import UnifiedAttention
from hyperdata_terminal.market_pulse import service
from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
from hyperdata_terminal.market_pulse.collectors.okx import OkxCollector
from hyperdata_terminal.market_pulse.control import desired_symbols, validate_plan


@pytest.mark.parametrize("symbol", ["BTC", "XRP", "SMALL"])
def test_every_instrument_can_win_attention_by_activity(symbol):
    selector = UnifiedAttention(None, slots=2, detail_slots=1)
    now = 1_000_000
    target = int((now * 1000 - 5000) // 60000 * 60000 - 60000)
    selector.activity = {symbol: dict(status="ready", start_ms=target, ratios=dict(trade_count=8))}
    selector.choose([dict(symbol=s, mark_price=1, volume_24h=10**12) for s in (symbol, "QUIET")], now)
    assert selector.plan["controls"] == []
    assert selector.plan["detail"] == [symbol]
    assert desired_symbols(selector.plan, now * 1000) == (symbol,)
    assert desired_symbols(selector.plan, (now + 61) * 1000) == ()


def test_old_control_pins_are_ignored_and_new_pins_rejected():
    old = dict(
        schema_version="attention.v1",
        generated_at_ms=1000,
        expires_at_ms=10000,
        selected=["ACTIVE"],
        detail=["ACTIVE"],
        controls=["BTC", "XRP"],
        active=[dict(symbol="ACTIVE", assigned_at=1, state="focus", reason="relative")],
    )
    assert desired_symbols(old, 2000) == ("ACTIVE",)
    with pytest.raises(ValueError, match="fixed control"):
        validate_plan(old, 2000)
    observer = PulseObservation.__new__(PulseObservation)
    observer.plan = old
    assert set(observer.assignments(2)) == {"ACTIVE"}
    assert observer.assignments(11) == {}


@pytest.mark.asyncio
async def test_remote_and_candle_universe_are_exactly_the_same_plan():
    now = time.time()
    symbols = [f"DYNAMIC{i}" for i in range(20)]
    selector = SimpleNamespace(
        plan=dict(schema_version="attention.v1", selected=symbols, controls=[], generated_at_ms=int(now * 1000)),
        choose=lambda *_: symbols,
    )
    hub = RemoteHub("http://127.0.0.1:1")
    hub.attention_selector = selector
    overview = [
        dict(symbol=s, mark_price=1, volume_24h=i, observed_fields=["price"])
        for i, s in enumerate(["BTC", "XRP", *symbols])
    ]

    async def get(path, **params):
        if path == "/v1/attention":
            return dict(enabled=True, runtime_mode="unified", active=[])
        if path == "/v1/market":
            return dict(assets=overview, as_of=now)
        assert path == "/v1/snapshot"
        assert set(params["symbols"].split(",")) == set(symbols)
        return dict(as_of=now, market=dict(quality="ok", observed_at=now), rows=[], context={})

    hub._get = get
    hub._refresh_health = AsyncMock()
    await hub.refresh_snapshot()
    assert {a.symbol for a in observation_assets(hub)} == set(symbols)
    assert not {"BTC", "XRP"} & set(hub.market.assets)


@pytest.mark.asyncio
async def test_dynamic_collectors_start_empty_even_with_old_env_and_cli_pins(monkeypatch, tmp_path):
    monkeypatch.setenv("PULSE_DYNAMIC_ATTENTION", "1")
    monkeypatch.setenv("PULSE_MARKETS", "BTC,XRP")
    monkeypatch.setenv("PULSE_RAW_DIR", str(tmp_path))
    assert service.markets() == ()
    stop = asyncio.Event()
    seen = []

    class Collector:
        _ws = None

        def __init__(self, config, *args, **kwargs):
            seen.append(config.markets)

        async def run(self, stop):
            stop.set()

    async def follow(*args):
        await stop.wait()

    client = SimpleNamespace(register_instruments=AsyncMock(), register_raw_segments=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(service, "HttpIngestClient", lambda *a, **k: client)
    monkeypatch.setattr(service, "HyperliquidCollector", Collector)
    monkeypatch.setattr(service, "follow_attention", follow)
    await service.run_collector("hyperliquid", markets_override=("BTC", "XRP"), stop=stop)
    assert seen == [()]
    client.register_instruments.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_adaptive_socket_does_not_wait_for_nonexistent_trade_ack(tmp_path):
    collector = OkxCollector(CollectorConfig("okx", (), str(tmp_path), ""), None, None, None)
    collector.dynamic = True
    socket = SimpleNamespace(receive=AsyncMock(side_effect=AssertionError("no subscription to acknowledge")))
    assert await collector._wait_for_subscription(socket) == []
    socket.receive.assert_not_awaited()
