import asyncio
import json
import time
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession, web

from apps.scalper_web.__main__ import ScalperAPI
from extensions.scalper.analysis import Gemini, blind_payload, compare, snapshot_key, validate
from extensions.scalper.journal import Journal
from extensions.scalper.market import capture
from extensions.scalper.service import Screener
from hyperdata_terminal.data_layer.market_data import AssetInfo
from hyperdata_terminal.data_layer.orderbook import OrderBookEngine
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine, ShardState, Trade


@pytest.fixture
def market():
    now = time.time()
    symbol = "kPEPE"
    asset = AssetInfo(symbol, 12, 0, 300, 400, 0, 12, 12)
    engine = OrderFlowEngine([symbol], include_binance=False)
    engine._hl_shard_plan = [[symbol]]
    engine._hl_shard_state = {0: ShardState(connected_at=now - 301)}
    engine._process_trade(Trade(now - 299, symbol, "buy", 12, 10, 120), venue="hyperliquid")
    engine._process_trade(Trade(now - 1, symbol, "buy", 12, 10, 120), venue="hyperliquid")
    book = OrderBookEngine([symbol])
    book._update_book(symbol, {"levels": [[{"px": "11", "sz": "2"}], [{"px": "13", "sz": "1"}]]})
    hub = SimpleNamespace(
        demo=False, market_timestamp=lambda: now - 1, market_age=lambda: 1,
        market=SimpleNamespace(observed_fields={symbol: {"price", "volume_24h", "open_interest"}}),
        orderflow=engine, get_orderbook=book.get_snapshot, get_all_assets=lambda: [asset],
        status=SimpleNamespace(mode="live", market_data="connected"),
    )
    return now, hub, asset


def model_reply(direction):
    return {
        "direction": direction, "summary": "Инициатива меняется", "scenario": "Наблюдать за спросом",
        "invalidation": "Поток разворачивается", "watch": "Следить за стаканом",
        "arguments": [{"metric": "cvd", "reason": "Односторонний поток"},
                      {"metric": "imbalance", "reason": "Стакан даёт дополнительный контекст"}],
        "contradictions": [],
    }


def test_capture_uses_native_flow_and_venue_quality(market):
    now, hub, asset = market
    snapshot, native = capture(hub, asset, "5m", now)
    assert native["native_signal"] == "STRONG_BULL"
    assert snapshot["metrics"]["cvd"]["value"] == 240
    assert snapshot["metrics"]["funding_rate"]["value"] is None
    assert snapshot["metrics"]["spread"]["value"] == 2
    assert snapshot["metrics"]["liquidations"]["quality"] == "unavailable"
    assert capture(hub, asset, "5m", now - 10)[1]["quality"] == "clock_skew"
    hub.orderflow._hl_shard_state[0].connected_at = now - 10
    assert capture(hub, asset, "5m", now)[1]["quality"] == "warming_up"
    hub.orderflow._hl_shard_state[0].connected_at = 0
    assert capture(hub, asset, "5m", now)[1]["direction"] == "NOT_AVAILABLE"


@pytest.mark.parametrize(("native_dir", "ai_dir", "category"), [
    ("BULLISH", "BULLISH", "CONSENSUS_LONG"),
    ("BEARISH", "BEARISH", "CONSENSUS_SHORT"),
    ("BEARISH", "BULLISH", "DIVERGENCE"),
    ("BEARISH", "NEUTRAL", "HYPERDATA_ONLY"),
    ("NEUTRAL", "BULLISH", "AI_ONLY"),
    ("NEUTRAL", "NEUTRAL", "NEUTRAL"),
    ("NOT_AVAILABLE", "INSUFFICIENT_DATA", "INSUFFICIENT_DATA"),
])
def test_consensus_categories(market, native_dir, ai_dir, category):
    now, hub, asset = market
    snapshot, native = capture(hub, asset, "5m", now)
    native.update(snapshot=snapshot_key(snapshot), direction=native_dir)
    ai = {**model_reply(ai_dir), "snapshot": snapshot_key(snapshot), "status": "ok"}
    assert compare(snapshot, native, ai)["category"] == category
    ai["snapshot"]["window"] = "1m"
    with pytest.raises(ValueError, match="different market snapshots"):
        compare(snapshot, native, ai)


def test_gemini_input_and_validation(market):
    now, hub, asset = market
    snapshot, _ = capture(hub, asset, "5m", now)
    snapshot.update(direction="BULLISH", native_signal="STRONG_BULL", event={"source": "OFI"})
    assert "STRONG_BULL" not in json.dumps(blind_payload(snapshot))
    reply = model_reply("BULLISH")
    assert validate(json.dumps(reply), snapshot) == reply
    reply["arguments"][1]["metric"] = "liquidations"
    with pytest.raises(ValueError, match="unavailable evidence"):
        validate(json.dumps(reply), snapshot)


async def test_provider_request_uses_json_schema_and_blind_snapshot(market):
    now, hub, asset = market
    snapshot, _ = capture(hub, asset, "5m", now)
    snapshot["direction"] = "BULLISH"

    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            pass

        async def json(self):
            return {"candidates": [{"finishReason": "STOP", "content": {"parts": [
                {"text": json.dumps(model_reply("BEARISH"))},
            ]}}]}

    def post(url, **request):
        assert url.endswith("/models/test-model:generateContent")
        body = request["json"]
        sent = json.loads(body["contents"][0]["parts"][0]["text"])
        assert "direction" not in sent and sent["window"] == "5m"
        assert body["generationConfig"]["responseMimeType"] == "application/json"
        return Response()

    gemini = Gemini(SimpleNamespace(post=post), "test-model", "fake-key")
    result = await gemini.analyze(snapshot)
    assert result["direction"] == "BEARISH" and result["status"] == "ok"


async def test_journal_worker_delivery_and_shared_api(market, tmp_path):
    now, hub, asset = market
    journal = Journal(tmp_path / "scalper.sqlite")
    sent = []

    async def analyze(snapshot):
        return {**model_reply("BULLISH"), "snapshot": snapshot_key(snapshot), "status": "ok"}

    async def send(message):
        sent.append(message)
        return True

    hub.alerts = SimpleNamespace(send_telegram=send)
    service = Screener(hub, journal, SimpleNamespace(key=None, analyze=analyze), True)
    service.scan(now)
    service.scan(now + 5)
    assert service.jobs.qsize() == 2
    workers = [asyncio.create_task(worker()) for worker in (service.analyze, service.deliver)]
    try:
        await asyncio.wait_for(service.jobs.join(), 2)
        await asyncio.wait_for(service.notifications.join(), 2)
        rows = journal.latest(asset.symbol, "5m")
        assert len(rows) == 1 and len(sent) == 2
        assert "kPEPE-PERP" in sent[0]
        event = rows[0]
        assert event["consensus"]["category"] == "CONSENSUS_LONG"
        assert event["delivery"] == "sent"
        assert not journal.record(event["id"], event["event"], event["snapshot"], event["hyperdata"])

        journal.observe(now + 59, {asset.symbol: (14, now + 59)})
        journal.observe(now + 61, {asset.symbol: (14, now + 59)})
        assert journal.latest(asset.symbol, "5m")[0]["observations"][0]["status"] == "pending"
        journal.observe(now + 62, {asset.symbol: (15, now + 61)})
        journal.observe(now + 332, {})
        observations = journal.latest(asset.symbol, "5m")[0]["observations"]
        assert [entry["status"] for entry in observations] == ["observed", "missed", "pending"]
        assert observations[0]["price"] == 15

        api = ScalperAPI(hub, service, 0)
        from aiohttp.test_utils import TestClient, TestServer
        app = web.Application()
        api.add_routes(app)
        async with TestClient(TestServer(app)) as client:
            response = await client.get("/v1/scalper/kPEPE?window=5m")
            assert response.status == 200
            assert (await response.json())["history"][0]["delivery"] == "sent"
            assert (await client.get("/v1/scalper?window=4h")).status == 400
            assert (await client.get("/scalper")).status == 200
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        journal.close()



def test_journal_has_one_service_owner(tmp_path):
    path = tmp_path / "scalper.sqlite"
    journal = Journal(path)
    try:
        with pytest.raises(RuntimeError, match="journal lock"):
            Journal(path)
    finally:
        journal.close()
    Journal(path).close()
