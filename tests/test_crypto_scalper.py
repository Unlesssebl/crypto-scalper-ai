import asyncio
import json
import time
from types import SimpleNamespace

import pytest
from aiohttp import ClientSession, web

from apps.scalper_web.__main__ import ScalperAPI
from extensions.scalper.activity import active_anomalies, candle_indicators, tape_baseline
from extensions.scalper.analysis import Gemini, OpenRouter, blind_payload, compare, snapshot_key, validate
from extensions.scalper.journal import Journal
from extensions.scalper.market import capture
from extensions.scalper.quality import calculate_quality
from extensions.scalper.service import Screener
from extensions.scalper.telegram import listen_reports, report_command, report_text
from hyperdata_terminal.data_layer.market_data import AssetInfo, MarketData
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
    assert snapshot["metrics"]["first_trade_price"]["updated_at"] == now - 299
    assert snapshot["metrics"]["last_trade_price"]["updated_at"] == now - 1
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
    reply["summary"] = "Непроверенный уровень 12.5"
    reply["arguments"][0]["reason"] = "Отрицательная дельта за 5m"
    cleaned = validate(json.dumps(reply), snapshot)
    assert cleaned["summary"] == "Непроверенный уровень …"
    assert cleaned["arguments"][0]["reason"] == "Отрицательная дельта за …"
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
    service = Screener(hub, journal, SimpleNamespace(key=None, provider="test", analyze=analyze), True)
    service.scan(now)
    service.scan(now + 5)
    assert service.jobs.qsize() == 2
    workers = [asyncio.create_task(worker()) for worker in (service.analyze, service.deliver)]
    try:
        await asyncio.wait_for(service.jobs.join(), 2)
        await asyncio.wait_for(service.notifications.join(), 2)
        rows = journal.latest(asset.symbol, "5m")
        assert len(rows) == 1 and len(sent) == 2
        assert "<b>kPEPE-PERP · Обе оценки указывают вверх</b>" in sent[0]
        assert "<b>HyperData</b> — вверх (в потоке преобладают покупки)" in sent[0]
        assert "<b>AI</b> — вверх" in sent[0]
        assert "<b>Почему</b>" in sent[0] and "МСК" in sent[0]
        assert "CONSENSUS_LONG" not in sent[0] and "OFI" not in sent[0]
        event = rows[0]
        assert event["consensus"]["category"] == "CONSENSUS_LONG"
        assert event["event"]["detected_at"] == now
        assert event["captured_at"] == now + 5
        assert event["snapshot"]["captured_at"] == event["hyperdata"]["snapshot"]["captured_at"]
        now = event["captured_at"]
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
            newer = {**event["snapshot"], "captured_at": now + 1}
            identity = snapshot_key(newer)
            native = {**event["hyperdata"], "snapshot": identity}
            journal.record("newer", event["event"], newer, native)
            failure = {"snapshot": identity, "status": "network_error"}
            journal.finish("newer", failure, compare(newer, native, failure), now + 2)
            response = await client.get("/v1/scalper?window=5m")
            row = (await response.json())["rows"][0]
            assert row["latest"]["id"] == "newer"
            assert row["accepted"]["id"] == event["id"]
            assert not row["queued"]
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


async def test_openrouter_accepts_non_google_model_and_preserves_blind_analysis(market):
    now, hub, asset = market
    snapshot, _ = capture(hub, asset, "5m", now)
    snapshot["native_signal"] = "STRONG_BULL"

    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            pass

        async def json(self):
            return {"model": "vendor/model", "choices": [{
                "finish_reason": "stop", "message": {"content": json.dumps(model_reply("BEARISH"))},
            }]}

    def post(url, **request):
        assert url == "https://openrouter.ai/api/v1/chat/completions"
        assert request["headers"] == {"Authorization": "Bearer test-key"}
        body = request["json"]
        assert body["model"] == "vendor/model"
        assert body["provider"]["require_parameters"]
        output = body["response_format"]["json_schema"]
        assert output["strict"]
        allowed = output["schema"]["properties"]["arguments"]["items"]["properties"]["metric"]["enum"]
        assert "ofi" in allowed and "liquidations" not in allowed
        assert "native_signal" not in json.loads(body["messages"][1]["content"])
        return Response()

    client = OpenRouter(SimpleNamespace(post=post), "vendor/model", "test-key")
    result = await client.analyze(snapshot)
    assert result["provider"] == "openrouter"
    assert result["status"] == "ok" and result["direction"] == "BEARISH"


def test_expired_ai_does_not_notify_about_hypothesis_change():
    previous = {
        "captured_at": 990, "hyperdata": {"direction": "BULLISH"},
        "ai": {"direction": "BULLISH", "status": "ok"},
    }
    deliveries = []
    journal = SimpleNamespace(
        latest=lambda *args, **kwargs: [previous],
        delivery=lambda key, state: deliveries.append((key, state)),
    )
    service = Screener(None, journal, None, True)
    service.schedule_alert("event", {"symbol": "BTC", "window": "1m", "captured_at": 1000},
                           {"direction": "BEARISH"}, {"status": "expired"},
                           {"category": "HYPERDATA_ONLY"})
    assert service.notifications.empty()
    assert deliveries == [("event", "not_applicable")]


def test_quality_counts_each_attempt_once_and_checks_each_horizon(tmp_path):
    path = tmp_path / "scalper.sqlite"
    journal = Journal(path)
    now = 100_000

    def record(key, window, at, price, status, direction=None):
        snapshot = {"symbol": key, "window": window, "captured_at": at,
                    "metrics": {"price": {"value": price, "quality": "ok"}}}
        journal.record(key, {}, snapshot, {})
        if status:
            journal.finish(key, {"status": status, "direction": direction}, {}, at + 1)

    try:
        record("long", "1m", now - 350, 100, "ok", "BULLISH")
        record("short", "1m", now - 350, 100, "ok", "BEARISH")
        record("neutral", "1m", now - 30, 100, "ok", "NEUTRAL")
        record("insufficient", "1m", now - 30, 100, "ok", "INSUFFICIENT_DATA")
        record("failed", "1m", now - 30, 100, "network_error")
        record("skipped", "1m", now - 30, 100, "expired")
        record("pending", "1m", now - 10, 100, None)
        record("other", "5m", now - 350, 100, "ok", "BEARISH")
        record("old", "1m", now - 90_000, 100, "ok", "BULLISH")
        journal.observe(now - 285, {"long": (101, now - 286), "short": (100, now - 286)})
        journal.observe(now - 45, {"long": (98, now - 46), "short": (99, now - 46)})
        journal.observe(now, {})

        report = calculate_quality(path, now, ("1m", "5m"))["windows"]
        assert report["1m"]["attempts"] == {
            "directional": 2, "neutral": 1, "insufficient": 1, "error": 1, "skipped": 1, "pending": 1,
        }
        assert report["1m"]["horizons"][1] == {
            "correct": 1, "incorrect": 0, "flat": 1, "pending": 0, "missing": 0,
        }
        assert report["1m"]["horizons"][5] == {
            "correct": 1, "incorrect": 1, "flat": 0, "pending": 0, "missing": 0,
        }
        assert report["1m"]["horizons"][15]["pending"] == 2
        assert report["5m"]["horizons"][1]["missing"] == 1
    finally:
        journal.close()


def test_activity_uses_closed_candles_and_nonoverlapping_trade_baseline():
    candles = [{"timestamp": i * 60_000, "high": 101, "low": 99, "close": 100, "trade_count": 10}
               for i in range(20)]
    candles[-2].update(high=104, low=96, trade_count=500)
    candles[-1].update(high=140, low=50, trade_count=900)
    now = 19 * 60 + 10
    indicators = candle_indicators(candles, 60, now)
    assert indicators["closed_at"] == 19 * 60
    assert indicators["range_ratio"] == pytest.approx(4)
    assert indicators["natr"] == pytest.approx((13 * 2 + 8) / 14)
    assert tape_baseline(indicators, now, 60) == 10
    snapshot = {"metrics": {"activity_ratio": {"quality": "ok", "value": 2},
                            "trade_count": {"value": 20},
                            "range_ratio": {"quality": "ok", "value": 4}}}
    assert active_anomalies(snapshot) == {"tape_burst", "range_burst"}


async def test_native_candle_request_preserves_market_symbol_and_trade_count(monkeypatch):
    market = MarketData()
    market.assets["kPEPE"] = None

    async def post(session, request):
        assert request["type"] == "candleSnapshot"
        assert request["req"]["coin"] == "kPEPE"
        assert request["req"]["interval"] == "1m"
        return [{"t": 0, "o": "1", "h": "2", "l": "1", "c": "2", "v": "3", "n": 7}]

    monkeypatch.setattr(market, "_post", post)
    assert (await market.get_candles("kPEPE", "1m"))[0]["trade_count"] == 7


def test_telegram_report_is_restricted_to_configured_chat(tmp_path):
    update = {"message": {"date": 1000, "chat": {"id": 42}, "text": "/report@ScalperBot 5m"}}
    assert report_command(update, "42", "scalperbot", 1010) == ("5m",)
    assert report_command(update, "43", "scalperbot", 1010) is None
    assert report_command(update, "42", "otherbot", 1010) is None
    assert report_command(update, "42", "scalperbot", 1300) is None
    journal = Journal(tmp_path / "reports.sqlite")
    try:
        journal.save_telegram_offset(12, 7)
        assert journal.telegram_offset(12) == 7 and journal.telegram_offset(13) == 0
        report = calculate_quality(tmp_path / "reports.sqlite", 1000, ("1m", "5m"))
        text = report_text(report, "5m")
        assert "Окно 5m" in text and "Окно 1m" not in text
        assert "15 мин: верно 0, неверно 0" in text
    finally:
        journal.close()


async def test_telegram_listener_replies_and_advances_offset(monkeypatch, tmp_path):
    path = tmp_path / "reports.sqlite"
    journal = Journal(path)
    replies = []
    sent = asyncio.Event()

    async def call(session, token, method, body=None, timeout=10):
        if method == "getWebhookInfo":
            return {"url": ""}
        if method == "getMe":
            return {"id": 12, "username": "ScalperBot"}
        if method == "getMyCommands":
            return [{"command": "report", "description": "Report"}]
        assert method == "getUpdates"
        if body["offset"] == 0:
            return [
                {"update_id": 6, "message": {"date": int(time.time()), "chat": {"id": 43}, "text": "/report"}},
                {"update_id": 7, "message": {"date": int(time.time()), "chat": {"id": 42}, "text": "/report 1m"}},
            ]
        assert body["offset"] == 8
        await asyncio.Future()

    async def send(message):
        replies.append(message)
        sent.set()
        return True

    monkeypatch.setattr("extensions.scalper.telegram.telegram_call", call)
    alerts = SimpleNamespace(telegram_token="test", telegram_chat_id="42", send_telegram=send)
    service = SimpleNamespace(hub=SimpleNamespace(alerts=alerts), journal=journal,
                              quality_report=calculate_quality(path, time.time(), ("1m", "5m")))
    task = asyncio.create_task(listen_reports(None, service))
    try:
        await asyncio.wait_for(sent.wait(), 1)
        assert len(replies) == 1 and "Окно 1m" in replies[0] and "Окно 5m" not in replies[0]
        assert journal.telegram_offset(12) == 8
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        journal.close()
