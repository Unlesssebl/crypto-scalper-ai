import asyncio
import json
import time
from types import SimpleNamespace

import pytest

from apps.screener_web.__main__ import build_app
from extensions.scalper.activity import (
    active_anomalies,
    btc_context,
    candle_indicators,
    finding_strength,
    market_risk_context,
    tape_baseline,
)
from extensions.scalper.analysis import Gemini, OpenRouter, blind_payload, compare, snapshot_key, validate
from extensions.scalper.journal import Journal
from extensions.scalper.market import capture
from extensions.scalper.profile import calculate_profiles, profile_context, sample_window
from extensions.scalper.quality import calculate_quality
from extensions.scalper.service import (
    Screener,
    five_minute_relation,
    minute_gate,
    with_context,
    with_movement_context,
    with_senior_context,
)
from extensions.scalper.telegram import digest_text, listen_reports, report_command, report_text
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
        "direction": direction, "summary": "Инициатива меняется",
        "scenario": "Если спрос сохранится, покупатели могут удержать инициативу",
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
    characterization = (
        "Покупатели удерживают инициативу, и цена отвечает на их агрессию. "
        "Лента ускорилась, но встречная ликвидность в стакане ограничивает продвижение цены. "
        "На более длинном окне преобладает противоположный поток, поэтому пока видна локальная реакция. "
        "Доступные метрики описывают разные стороны этой реакции."
    )
    descriptive_reply = {**reply, "summary": characterization, "scenario": ""}
    assert len(characterization) > 240
    assert validate(json.dumps(descriptive_reply), snapshot) == descriptive_reply
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


async def test_journal_worker_delivery_and_shared_api(market, tmp_path, monkeypatch):
    monkeypatch.setattr("extensions.scalper.service.AI_INTERVAL", 0)
    monkeypatch.setattr("extensions.scalper.opportunity.OpportunityPolicy.classify",
                        lambda *args: ("confirmed", "eligible"))
    monkeypatch.setattr("extensions.scalper.service.active_anomalies", lambda snapshot: {"tape_burst"})
    monkeypatch.setattr("extensions.scalper.service.finding_strength", lambda snapshot, native: {
        "rank": 4 if snapshot["window"] == "5m" else 3,
        "label": "Выраженное движение", "icon": "🟠", "reasons": [],
    })
    now, hub, asset = market
    journal = Journal(tmp_path / "scalper.sqlite")
    sent, edits = [], []
    facts_sent = asyncio.Event()

    async def analyze(snapshot):
        await facts_sent.wait()
        return {**model_reply("BULLISH"), "snapshot": snapshot_key(snapshot), "status": "ok"}

    async def send(message, *, rich_html, copy_text, return_message):
        assert return_message and ("анализ выполняется" in message or "в очереди" in message)
        assert "Гипотеза AI</b>" not in rich_html
        sent.append((message, rich_html, copy_text))
        facts_sent.set()
        return len(sent), True

    async def edit(message_id, message, *, rich_html, copy_text):
        assert 1 <= message_id <= len(sent)
        edits.append((message, rich_html, copy_text))
        return True

    hub.alerts = SimpleNamespace(send_telegram=send, edit_telegram=edit)
    service = Screener(hub, journal, SimpleNamespace(key="test", provider="test", analyze=analyze), True)
    service.scan(now)
    service.scan(now + 5)
    assert service.jobs.qsize() == 1
    workers = [asyncio.create_task(worker()) for worker in (service.analyze, service.deliver)]
    try:
        await asyncio.wait_for(service.jobs.join(), 2)
        await asyncio.wait_for(service.notifications.join(), 2)
        rows = journal.latest(asset.symbol, "1m")
        assert len(rows) == 1 and len(sent) == 1
        assert len(edits) >= 2
        plain, rich, ticker = edits[-1]
        assert "Обновление" in plain and "Обновление" in rich
        assert ticker == "HYPERLIQUID:KPEPEUSDC.P"
        headline = f'<b><a href="https://app.hyperliquid.xyz/trade/kPEPE">{ticker}</a></b> · '
        assert plain.startswith(headline)
        assert rich.startswith("<p>" + headline)
        assert f'<tg-button type="copy_text" text="{ticker}">' in rich
        assert "<hr/>\n<p><b>Рынок</b>" in rich
        assert "<table striped compact>" in rich
        assert "<th>Стакан</th><td>дисбаланс" in rich
        assert "<b>Стакан</b> · дисбаланс" in plain
        assert "<blockquote><b>Гипотеза AI</b>" in rich
        for message in (plain, rich):
            assert "Сила:" in message and "Mark $" in message and "МСК" in message
            assert "<b>HyperData</b> 🟢 вверх · <b>AI</b> 🟢 вверх" in message
            assert "bid $" not in message and "ask $" not in message
            assert "<b>🧠 Характеристика AI</b>" in message and "Инициатива меняется" in message
            assert "Что наблюдать" not in message and "Когда пересмотреть" not in message
            assert "<details" not in message and "expandable" not in message
            assert message.index("Всплеск ленты") < message.index("<b>HyperData</b>")
            assert message.index("Характеристика AI") < message.index("<blockquote>")
        assert rich.index("Гипотеза AI") < rich.index("Копировать фьючерс")
        assert "CONSENSUS_LONG" not in rich
        event = rows[0]
        assert event["consensus"]["category"] == "CONSENSUS_LONG"
        assert event["event"]["detected_at"] == now
        assert event["captured_at"] >= now
        assert event["snapshot"]["captured_at"] == event["hyperdata"]["snapshot"]["captured_at"]
        minute_event = journal.latest(asset.symbol, "1m")[0]
        assert {journal.latest(asset.symbol, "5m")[0]["delivery"], minute_event["delivery"]} == {"sent", "merged"}
        minute = minute_event["snapshot"]
        assert minute["context_5m"]["captured_at"] == minute["captured_at"]
        assert "direction" not in minute["context_5m"]
        assert minute["context_5m"]["flow_quality"] == {"ofi": "ok", "cvd": "ok"}
        assert minute["movement_context"]["status"] == "untracked"
        now = event["captured_at"]
        assert not journal.record(event["id"], event["event"], event["snapshot"], event["hyperdata"])

        journal.observe(now + 59, {asset.symbol: (14, now + 59)})
        journal.observe(now + 61, {asset.symbol: (14, now + 59)})
        assert journal.latest(asset.symbol, "1m")[0]["observations"][0]["status"] == "pending"
        journal.observe(now + 62, {asset.symbol: (15, now + 61)})
        journal.observe(now + 332, {})
        observations = journal.latest(asset.symbol, "1m")[0]["observations"]
        assert [entry["status"] for entry in observations] == ["observed", "missed", "pending"]
        assert observations[0]["price"] == 15

        from aiohttp.test_utils import TestClient, TestServer
        app = build_app(hub, service)
        async with TestClient(TestServer(app)) as client:
            response = await client.get("/v1/scalper/kPEPE?window=1m")
            assert response.status == 200
            assert (await response.json())["history"][0]["delivery"] == event["delivery"]
            newer = {**event["snapshot"], "captured_at": now + 1}
            identity = snapshot_key(newer)
            native = {**event["hyperdata"], "snapshot": identity}
            journal.record("newer", event["event"], newer, native)
            failure = {"snapshot": identity, "status": "network_error"}
            journal.finish("newer", failure, compare(newer, native, failure), now + 2)
            response = await client.get("/v1/scalper?window=1m")
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


def test_expired_ai_edits_existing_telegram_message_with_real_status():
    edits = []
    journal = SimpleNamespace(fact_message=lambda key: {
        "message_id": 1, "ai": {"status": "expired"}, "edit_status": None,
    }, fact_message_edit=lambda *args: edits.append(args))
    service = Screener(None, journal, None, True)
    service.queue_hypothesis("event")
    assert service.notifications.get_nowait() == ("edit", "event")
    assert edits == [("event", "queued")]


def test_quality_counts_each_attempt_once_and_checks_each_horizon(tmp_path):
    path = tmp_path / "scalper.sqlite"
    journal = Journal(path)
    now = 100_000

    def record(key, window, at, price, status, direction=None):
        snapshot = {"symbol": key, "window": window, "captured_at": at,
                    "metrics": {"price": {"value": price, "quality": "ok"}}}
        event = {"type": "orderflow_transition", "detected_at": at - 9}
        if key == "long":
            event["anomalies"] = ["tape_burst"]
        journal.record(key, event, snapshot, {})
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
        tape = report["1m"]["triggers"]["tape_burst"]
        assert tape["horizons"][1]["correct"] == 1
        assert tape["horizons"][5]["incorrect"] == 1
        assert tape["median_delay_seconds"] == 10 and tape["delay_samples"] == 1
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
        assert "15 мин</b> · 🟢 верно 0 · 🔴 неверно 0" in text
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
        if method == "setMyCommands":
            assert any(c["command"] == "analyze" for c in body["commands"])
            return True
        assert method == "getUpdates"
        if body["offset"] == 0:
            return [
                {"update_id": 6, "message": {"date": int(time.time()), "chat": {"id": 43}, "text": "/report"}},
                {"update_id": 7, "message": {"date": int(time.time()), "chat": {"id": 42}, "text": "/report 1m"}},
            ]
        assert body["offset"] == 8
        await asyncio.Future()

    async def send(message, *, rich_html):
        assert "<table striped compact>" in rich_html
        assert "<th>Точность</th>" in rich_html
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


def test_minute_gate_and_blind_five_minute_context(market):
    now, hub, asset = market
    snapshot, native = capture(hub, asset, "1m", now)
    longer, other = capture(hub, asset, "5m", now)
    other["direction"] = "BEARISH"
    row = {"snapshot": longer, "hyperdata": other}
    assert minute_gate(snapshot) == "ready"
    snapshot["metrics"]["natr"].update(value=0.5, quality="ok")
    assert minute_gate(snapshot) == "wide_spread"
    snapshot["metrics"]["spread"]["quality"] = "stale"
    assert minute_gate(snapshot) == "book_unavailable"
    assert minute_gate(longer) == "ready"
    assert five_minute_relation(native, row) == "opposed"
    other["direction"] = native["direction"]
    assert five_minute_relation(native, row) == "aligned"
    payload = blind_payload(with_context(snapshot, row))
    assert payload["context_5m"]["window"] == "5m"
    assert "ofi" in payload["context_5m"]["metrics"]
    assert other["direction"] not in json.dumps(payload)


def test_minute_queue_does_not_schedule_ordinary_flow_without_key(market):
    now, hub, asset = market
    snapshot, native = capture(hub, asset, "1m", now)
    service = Screener(hub, None, None, False)
    for symbol, gate, relation in (
        ("blocked", "book_unavailable", "aligned"),
        ("wide", "wide_spread", "neutral"),
        ("normal", "ready", "opposed"),
        ("aligned", "ready", "aligned"),
    ):
        service.detect((symbol, "1m"), snapshot, native, now, gate, relation)
    assert service.jobs.empty()


def test_senior_context_uses_closed_candles_and_is_in_blind_payload(market):
    now, hub, asset = market
    snapshot, _ = capture(hub, asset, "1m", now)
    latest_open = int(now // 900) * 900_000
    bars = [{"timestamp": latest_open + i * 900_000, "high": 101, "low": 99, "close": 100}
            for i in range(-20, 1)]
    bars[-2].update(high=105, close=104)
    bars[-1].update(high=190, close=190)
    senior = candle_indicators(bars, 900, now)
    assert senior["return_4"] == pytest.approx(4)
    assert senior["range_position_4"] == pytest.approx(5 / 6)
    enriched = with_senior_context(snapshot, senior)
    payload = blind_payload(enriched)
    assert payload["context_15m"]["metrics"]["return_4"]["value"] == pytest.approx(4)
    assert payload["metric_roles"]["trade_rate"] == "intensity"
    assert payload["metric_roles"]["ofi"] == "flow"
    assert "context_15m" not in with_senior_context({**snapshot, "captured_at": now + 1800}, senior)
    assert snapshot_key(enriched) == snapshot_key(snapshot)


def test_context_trial_scores_three_arms_from_a_common_post_reply_price(tmp_path):
    path = tmp_path / "scalper.sqlite"
    journal = Journal(path)
    try:
        snapshot = {"symbol": "BTC", "window": "1m", "captured_at": 100,
                    "metrics": {"price": {"quality": "ok", "value": 90}}}
        journal.record("pair", {}, snapshot, {"direction": "BEARISH"})
        journal.finish("pair", {"status": "ok", "direction": "BULLISH"}, {}, 103)
        journal.record_context_trial("pair", {"status": "ok", "direction": "NEUTRAL"}, 110)
        journal.observe(110, {"BTC": (95, 109)})
        journal.observe(115, {"BTC": (100, 114)})
        journal.observe(172, {"BTC": (99, 171)})
        report = calculate_quality(path, 172, ("1m", "5m"))["context_experiment"]["1m"]
        assert (report["attempted"], report["matched"], report["changed"]) == (1, 1, 1)
        assert report["horizons"][1]["hyperdata"]["correct"] == 1
        assert report["horizons"][1]["baseline"]["no_direction"] == 1
        assert report["horizons"][1]["senior"]["incorrect"] == 1
        assert report["horizons"][5]["senior"]["pending"] == 1
    finally:
        journal.close()


def test_instrument_profiles_count_regular_windows_and_filter_missing_metrics(market, tmp_path):
    now, hub, asset = market
    slot = int(now // 60) * 60
    snapshot, _ = capture(hub, asset, "1m", now)
    snapshot["captured_at"] = slot + 4
    sample = sample_window(snapshot, 60)
    assert sample[:3] == (asset.symbol, "1m", slot)
    assert sample[3] is not None
    path = tmp_path / "profiles.sqlite"
    journal = Journal(path)
    try:
        samples = [(asset.symbol, "1m", slot - i * 60, i + 1, None, 2.0, 0.0, None) for i in range(20)]
        journal.record_instrument_samples(samples + samples)
        profile = calculate_profiles(path, now, [asset.symbol])[(asset.symbol, "1m")]
        period = profile["periods"][7]
        assert period["windows"] == 20
        assert period["metrics"]["trades"] == {"samples": 20, "median": 10.5, "p90": 18.0}
        assert "natr" not in period["metrics"]
        assert "trades" in profile_context(profile)["periods"][7]
    finally:
        journal.close()


def test_finding_strength_requires_price_confirmation_for_critical_rank(market):
    now, hub, asset = market
    snapshot, native = capture(hub, asset, "1m", now)
    metrics = snapshot["metrics"]
    for name, value in {
        "spread": 0.001, "natr": 1.0, "first_trade_price": 12, "last_trade_price": 12,
        "activity_ratio": 1, "trade_count": 20, "range_ratio": 1,
    }.items():
        metrics[name].update(value=value, quality="ok")

    assert finding_strength(snapshot, native)["rank"] == 1
    metrics["activity_ratio"]["value"] = 2.5
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["range_ratio"]["value"] = 3
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["last_trade_price"]["value"] = 12.05
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["last_trade_price"]["value"] = 12.14
    assert finding_strength(snapshot, native)["rank"] == 3
    metrics["last_trade_price"]["value"] = 12.20
    assert finding_strength(snapshot, native)["rank"] == 4
    metrics["spread"]["value"] = 0.04
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["spread"]["value"] = 0.001
    metrics["spread"]["quality"] = "stale"
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["ofi"]["quality"] = "stale"
    assert finding_strength(snapshot, native)["rank"] == 0


def test_btc_context_requires_aligned_closed_returns_and_ignores_flat_series():
    returns = {300 * i: (i % 7 - 3) / 10 for i in range(1, 37)}
    btc = {"returns": returns}
    asset = {"returns": {stamp: -change for stamp, change in returns.items()}}
    context = btc_context(asset, btc, 36 * 300 + 10)
    assert context["correlation"] == pytest.approx(-1)
    assert context["samples"] == 36
    assert context["relative_last_return_pp"] == pytest.approx(-2 * returns[36 * 300])
    assert btc_context(asset, btc, 36 * 300 + 500) is None
    assert btc_context({"returns": {stamp: 0 for stamp in returns}}, btc, 36 * 300) is None
    assert btc_context({"returns": dict(list(returns.items())[1:])}, btc, 36 * 300) is None
    assert btc_context(btc, btc, 36 * 300) is None


def test_blind_context_distinguishes_warming_flow_and_observed_impulse(market):
    now, hub, asset = market
    minute, _ = capture(hub, asset, "1m", now)
    longer, _ = capture(hub, asset, "5m", now)
    longer["metrics"]["ofi"]["quality"] = "warming_up"
    longer["metrics"]["cvd"]["quality"] = "warming_up"
    snapshot = with_context(minute, {"snapshot": longer})
    phase = {"phase": "confirmed", "direction": "BULLISH", "started_at": now - 25,
             "age_seconds": 25, "distance_natr": 1.25, "retracement_natr": 0.25, "natr_at_start": 0.1}
    payload = blind_payload(with_movement_context(snapshot, phase))
    assert payload["context_5m"]["flow_quality"] == {"ofi": "warming_up", "cvd": "warming_up"}
    assert "ofi" not in payload["context_5m"]["metrics"]
    assert "spread" in payload["context_5m"]["metrics"]
    assert payload["movement_context"] == {
        "status": "observed", "age_seconds": 25, "distance_natr": 1.25,
        "retracement_natr": 0.25, "natr_at_start": 0.1,
    }
    missing = blind_payload(with_context(minute, None))
    assert missing["context_5m"]["flow_quality"] == {"ofi": "unavailable", "cvd": "unavailable"}
    assert missing["context_5m"]["metrics"] == {}


def test_market_risk_context_flags_skew_and_iv():
    context = {
        "lsr": {"BTC": {"long_short_ratio": 1.8, "source": "okx"}},
        "iv": {"BTC": {"mark_iv": 65.0}},
        "basis": {"BTC": {"basis_pct": -0.6, "source": "coinbase"}},
        "liquidations": {"confirmed_count": 7, "confirmed_volume_usd": 500_000},
    }
    notes = market_risk_context(context)
    assert any("L/S BTC" in note for note in notes)
    assert any("IV BTC" in note for note in notes)
    assert any("Базис BTC" in note for note in notes)
    assert any("Ликвидации" in note for note in notes)


def test_market_risk_context_is_empty_without_context():
    assert market_risk_context(None) == []
    assert market_risk_context({}) == []


def test_blind_payload_carries_market_context_when_present(market):
    now, hub, asset = market
    snapshot, _ = capture(hub, asset, "1m", now)
    snapshot["market_context"] = {"lsr": {"BTC": {"long_short_ratio": 1.2}}}
    payload = blind_payload(snapshot)
    assert payload["market_context"]["lsr"]["BTC"]["long_short_ratio"] == 1.2


def test_digest_text_renders_context_and_directional_rows():
    context = {
        "lsr": {"BTC": {"long_short_ratio": 1.4, "source": "okx"}},
        "iv": {"BTC": {"mark_iv": 45.0}},
        "basis": {"BTC": {"basis_pct": 0.12}},
        "liquidations": {"confirmed_count": 3, "confirmed_volume_usd": 250_000},
    }
    rows = {
        ("BTC", "1m"): {"hyperdata": {"direction": "BULLISH"}},
        ("ETH", "1m"): {"hyperdata": {"direction": "BEARISH"}},
        ("ETH", "5m"): {"hyperdata": {"direction": "NEUTRAL"}},
    }
    text = digest_text(rows, context)
    assert text is not None
    assert "L/S BTC 1.40" in text
    assert "IV BTC 45%" in text
    assert "Базис BTC +0.12%" in text
    assert "Ликвидации (1 ч): 3" in text
    assert "BTC 🟢" in text
    assert "ETH 🔴" in text
    assert "5m" not in text


def test_digest_text_requires_context():
    assert digest_text({}, None) is None
    assert digest_text({}, {}) is None


def test_alert_min_rank_defaults_and_overrides(monkeypatch):
    from extensions.scalper.audit import STRONG_RANK
    from extensions.scalper.service import alert_min_rank

    monkeypatch.delenv("HYPERDATA_ALERT_MIN_RANK", raising=False)
    assert alert_min_rank() == STRONG_RANK
    monkeypatch.setenv("HYPERDATA_ALERT_MIN_RANK", "2")
    assert alert_min_rank() == 2
    monkeypatch.setenv("HYPERDATA_ALERT_MIN_RANK", "not-a-number")
    assert alert_min_rank() == STRONG_RANK
