"""Broad summary discovery must not contaminate execution evidence."""
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from extensions.scalper.attention import Attention
from extensions.scalper.pulse import PulseContext
from hyperdata_terminal.market_pulse.api import create_app
from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
from hyperdata_terminal.market_pulse.collectors.hyperliquid import HyperliquidCollector
from hyperdata_terminal.market_pulse.gaps import GapTracker
from hyperdata_terminal.market_pulse.ingest import GapControlService, IngestionService
from hyperdata_terminal.market_pulse.ingest_http import create_ingest_app
from hyperdata_terminal.market_pulse.overview import VERSION, activity, validate_candle
from hyperdata_terminal.market_pulse.raw_archive import RawArchive
from hyperdata_terminal.market_pulse.storage import InMemoryMarketPulseStore
from hyperdata_terminal.market_pulse.windows import WindowService

M = 60_000


def candle(start, count=10):
    return dict(schema_version=VERSION, symbol="NEW", start_ms=start, received_at_ms=start + 59_000,
                epoch_started_ms=0, trade_count=count, base_volume=str(count), close="100",
                source_epoch="same", raw_ref="raw:1")


def history():
    return [candle(i*M, 200 if i == 20 else 10) for i in range(6, 21)]


def test_flat_price_activity_drives_discovery_and_stale_does_not():
    now = 21*M + 6000
    rows = history()
    measured = activity(rows, now)["items"][0]
    assert measured["status"] == "ready" and measured["ratios"]["trade_count"] == 20
    attention = Attention(None, slots=1, detail_slots=1)
    assets = [dict(symbol=s, mark_price=100, volume_24h=v) for s, v in (("ETH", 10**9), ("NEW", 10**5))]
    attention.activity = {"NEW": measured}
    assert attention.choose(assets, now/1000) == ["NEW"]
    assert attention.plan["reasons"]["NEW"]["reason"] == "candle_activity"
    attention = Attention(None, slots=1, detail_slots=1)
    attention.activity = {"NEW": measured}
    assert attention.choose(assets, (now + M)/1000) == ["ETH"]


@pytest.mark.parametrize("mutation", ["missing", "epoch", "early", "partial_start"])
def test_gap_or_partial_reference_never_qualifies(mutation):
    rows = history()
    if mutation == "missing":
        rows.pop(2)
    elif mutation == "epoch":
        rows[2]["source_epoch"] = "old"
    elif mutation == "early":
        rows[2]["received_at_ms"] = rows[2]["start_ms"] + 10_000
    else:
        rows[2]["epoch_started_ms"] = rows[2]["start_ms"] + 1
    assert activity(rows, 21*M + 6000)["items"][0]["status"] == "warming_up"


def test_zero_reference_explicit_and_target_not_in_reference():
    rows = [candle(i*M, 200 if i == 20 else 0) for i in range(6, 21)]
    result = activity(rows, 21*M + 6000)["items"][0]
    assert result["zero_reference"] and result["ratios"]["trade_count"] is None
    assert activity(rows[:-1], 21*M + 6000)["items"][0]["status"] == "missing"


@pytest.mark.parametrize("change", [{"base_volume": "NaN"}, {"trade_count": -1}, {"trade_count": True},
                                    {"start_ms": 1}, {"source_epoch": ""}, {"close": "0"}])
def test_invalid_candles_rejected(change):
    with pytest.raises(ValueError):
        validate_candle({**candle(0), **change}, M)


@pytest.mark.asyncio
async def test_candle_is_not_trade_and_ack_does_not_confirm_trade(tmp_path):
    sink = AsyncMock()
    archive = RawArchive(tmp_path, "hyperliquid")
    config = CollectorConfig("hyperliquid", ("BTC",), str(tmp_path), "http://unused")
    collector = HyperliquidCollector(config, archive, sink)
    collector.overview_markets = {"NEW"}
    collector._overview_epoch_start = 0
    payload = dict(channel="candle", data=dict(t=0, T=59_999, s="NEW", i="1m", n=20, v="10", c="100"))
    await collector.auxiliary_payload(payload, "epoch", 59_000, "raw:1")
    assert collector.normalize(payload, receive_time_ms=59_000, source_epoch="epoch", raw_ref="raw:1") == []
    await collector.flush_overview()
    sink.overview.assert_awaited_once()
    sink.append_batch.assert_not_awaited()
    assert collector.acknowledged_market(dict(channel="subscriptionResponse", data=dict(
        method="subscribe", subscription=dict(type="candle", coin="BTC", interval="1m")))) is None
    archive.close()


@pytest.mark.asyncio
async def test_overview_auth_atomic_validation_and_read_api():
    import time
    store = InMemoryMarketPulseStore()
    gaps = GapTracker(store)
    app = create_ingest_app(store, IngestionService(store, WindowService(store), gaps),
                            GapControlService(store, gaps), token="secret")
    now = int(time.time()*1000)
    row = candle(now//M*M - M)
    async with TestClient(TestServer(app)) as client:
        response = await client.post("/ingest/v1/overview", json={"candles": [row]})
        assert response.status == 401
        headers = {"Authorization": "Bearer secret"}
        response = await client.post("/ingest/v1/overview", json={"candles": [row, {}]}, headers=headers)
        assert response.status == 400 and await store.get_overview(0) == []
        response = await client.post("/ingest/v1/overview", json={"candles": [row]}, headers=headers)
        assert response.status == 200
    async with TestClient(TestServer(create_app(store))) as client:
        response = await client.get("/overview")
        assert response.status == 200 and (await response.json())["schema_version"] == VERSION


@pytest.mark.asyncio
async def test_control_still_publishes_when_shadow_disabled(monkeypatch):
    import time

    import aiohttp
    from aiohttp import web
    now = time.time()
    saved = []

    async def control(request):
        if request.method == "POST":
            saved.append(await request.json())
        return web.json_response({"plan": saved[-1] if saved else None})

    async def overview(request):
        return web.json_response({"schema_version": VERSION, "items": []})

    async def context(request):
        return web.json_response({"schema_version": "market-pulse.window.v1", "channels": [], "windows": []})

    app = web.Application()
    app.router.add_route("*", "/ingest/v1/attention", control)
    app.router.add_get("/attention", control)
    app.router.add_get("/overview", overview)
    app.router.add_get("/coverage", context)
    app.router.add_get("/windows", context)
    async with TestClient(TestServer(app)) as client, aiohttp.ClientSession() as session:
        url = str(client.make_url("")).rstrip("/")
        monkeypatch.setenv("PULSE_INGEST_URL", url)
        monkeypatch.setenv("PULSE_INGEST_TOKEN", "secret")
        pulse = PulseContext(url)
        pulse.desired_plan = dict(schema_version="attention.v1", generated_at_ms=int(now*1000),
                                 expires_at_ms=int(now*1000)+300_000, selected=["ETH"], detail=["ETH"],
                                 controls=[])
        assert pulse.shadow is None
        await pulse.refresh(session, now)
        assert len(saved) == 1 and pulse.publisher_status == "published"
        assert "ETH" in pulse.markets and pulse.status == "connected"


@pytest.mark.asyncio
async def test_wide_frames_do_not_flood_liveness_writes(tmp_path):
    gaps = AsyncMock()
    collector = HyperliquidCollector(CollectorConfig("hyperliquid", ("BTC",), str(tmp_path), ""),
                                     RawArchive(tmp_path, "hyperliquid"), AsyncMock(), gaps)
    for stamp in range(10_000, 11_000, 10):
        await collector._report_heartbeat(stamp)
    assert gaps.heartbeat.await_count == 1
    await collector._report_heartbeat(11_000)
    assert gaps.heartbeat.await_count == 2
    collector.archive.close()
