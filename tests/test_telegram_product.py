"""End-to-end notification policy, destinations and explicit local terminal actions."""

import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from extensions.scalper.consistency_store import ConsistencyStore
from extensions.scalper.context_auto import ContextMemory, summarize
from extensions.scalper.delivery import CardOutbox
from extensions.scalper.fact_packet import build_packet
from extensions.scalper.journal import Journal
from extensions.scalper.telegram_ux import TelegramUX, operational, scale
from extensions.scalper.tiger import TigerBridge


def packet(now, *, price=10, level=9.99, flow=0.8, count=30, assigned=1):
    card = dict(
        status="observing",
        received_at=now,
        midpoint=price,
        spread_bps=2,
        observed_turnover_60s_usd=100,
        observed_trade_imbalance=flow,
        observed_trade_count_60s=count,
        last_trade_received_at=now,
        trade_evidence=["run:trade"],
        evidence="run:book",
        densities=[
            dict(side="bid", price=level, size=100, relative_size=8, observed_age_seconds=40, evidence="run:book")
        ],
    )
    assignment = dict(symbol="BTC", state="focus", assigned_at=assigned, score=6, facts=card)
    return build_packet(dict(policy_version=4, archive=dict(error=None)), assignment, now)


@pytest.fixture
def service(tmp_path, monkeypatch):
    monkeypatch.delenv("SCALPER_ANALYTICS_CHAT_ID", raising=False)
    monkeypatch.delenv("SCALPER_TELEGRAM_OPERATOR_IDS", raising=False)
    monkeypatch.setenv("SCALPER_TIGER_LINK_GROUP", "S")
    monkeypatch.setenv("SCALPER_TIGER_EXCHANGE", "BINANCE-FUT")
    monkeypatch.setenv("SCALPER_TIGER_MARKET", "FUTURES")
    monkeypatch.delenv("SCALPER_TIGER_SYMBOLS", raising=False)
    journal = Journal(tmp_path / "journal.sqlite")
    alerts = SimpleNamespace(
        telegram_chat_id="42",
        send_telegram=AsyncMock(return_value=(123, True)),
        edit_telegram=AsyncMock(return_value=True),
        last_telegram_result=dict(state="sent", http_status=200),
    )
    s = SimpleNamespace(
        journal=journal,
        hub=SimpleNamespace(alerts=alerts, attention={}),
        context_memory=ContextMemory(journal.db),
        consistency=ConsistencyStore(journal.db),
        context_packets={},
        attention=SimpleNamespace(activity={}, plan={}),
        pulse=None,
    )
    s.outbox = CardOutbox(journal, alerts)
    s.ux = TelegramUX(s)
    yield s
    journal.close()


def observe(service, now, *, ratio=2, **kwargs):
    for at in (now - 20, now - 10, now):
        p = packet(at, **kwargs)
        service.context_memory.observe(p)
    service.context_packets = {"BTC": p}
    service.attention.activity = {
        "BTC": dict(status="ready", start_ms=int(now * 1000) // 60000 * 60000 - 60000, ratios=dict(trade_count=ratio))
    }
    service.ux.observe(now)
    return p


def operations(service):
    return list(
        service.journal.db.execute(
            "SELECT * FROM episode_cards WHERE destination='operations' AND episode_id<>'ux:watch'"
        )
    )


def test_operational_signal_is_independent_of_ai_and_ordinary_zone_is_quiet(service):
    now = time.time()
    observe(service, now, ratio=1, flow=0, count=2)
    assert not operations(service)
    observe(service, now + 30)
    rows = operations(service)
    assert len(rows) == 1
    text = rows[0]["text"]
    assert text.startswith("<b>▲ BTC</b>") and "Hyperliquid ·" in text
    assert "Спрос 9.99 (-0.10%)" in text and "× медианы" in text and "▲ покупки 90%" in text
    assert "score" not in rows[0]["text"] and "Бесплатный" not in rows[0]["text"]
    assert service.journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == 0


def test_material_strengthening_only_and_restart_does_not_repeat(service):
    now = time.time()
    observe(service, now)
    observe(service, now + 130, ratio=2.2)
    assert len(operations(service)) == 1
    service.ux = TelegramUX(service)
    observe(service, now + 150, ratio=4)
    assert len(operations(service)) == 2
    observe(service, now + 300, ratio=4)
    assert len(operations(service)) == 2
    assert "Усиление" in operations(service)[1]["text"]


def test_unknown_and_stale_data_cannot_push(service):
    now = time.time()
    p = packet(now - 30)
    service.context_packets = {"BTC": p}
    service.ux.observe(now)
    assert not operations(service)
    assert (
        "устарели"
        in service.journal.db.execute("SELECT text FROM episode_cards WHERE episode_id='ux:watch'").fetchone()[0]
    )


def test_timeframe_is_not_episode_age():
    p = packet(time.time())
    assert scale(p, summarize(p)) == "не определён"
    for key, stamp, val in (
        ("first_trade_price", p["captured_at"] - 260, 9),
        ("last_trade_price", p["captured_at"], 10),
        ("ofi", p["captured_at"], 0.8),
    ):
        p["facts"].append(dict(id="frame.5m." + key, source_time=stamp, quality="ok", value=val))
    assert scale(p, summarize(p)) == "5m"
    p["facts"][-2]["source_time"] = p["captured_at"] - 250
    assert scale(p, summarize(p)) == "не определён"


@pytest.mark.asyncio
async def test_analytics_missing_never_falls_back_to_operations(service, monkeypatch):
    now = time.time()
    response = dict(
        status="ok",
        packet=packet(now),
        review=dict(result=dict(abstention=False, expired_at_completion=False, claims=[])),
    )
    assert service.ux.publish_analysis(response)
    row = service.journal.db.execute("SELECT * FROM episode_cards WHERE destination='analytics'").fetchone()
    assert row["status"] == "blocked_destination" and row["chat_id"] == ""
    await service.outbox.once(now)
    service.hub.alerts.send_telegram.assert_not_awaited()
    monkeypatch.setenv("SCALPER_ANALYTICS_CHAT_ID", "-100123")
    await service.outbox.once(now + 1)
    assert service.hub.alerts.send_telegram.await_args.kwargs["chat_id"] == "-100123"
    assert service.hub.alerts.send_telegram.await_args.kwargs["silent"] is True


@pytest.mark.asyncio
async def test_watchlist_is_silent_and_keeps_same_message_after_restart(service):
    now = time.time()
    service.ux.watch(now)
    await service.outbox.once(now)
    assert service.hub.alerts.send_telegram.await_args.kwargs["silent"] is True
    service.outbox = CardOutbox(service.journal, service.hub.alerts)
    service.ux.watch(now + 70)
    await service.outbox.once(now + 70)
    assert service.hub.alerts.send_telegram.await_count == 1
    assert service.hub.alerts.edit_telegram.await_args.args[0] == 123


@pytest.mark.asyncio
async def test_callback_authorization_expiry_and_dedup(service):
    now = time.time()
    observe(service, now)
    await service.outbox.once(now)
    row = operations(service)[0]
    buttons = json.loads(row["reply_markup"])["inline_keyboard"][0]
    query = dict(
        id="q1",
        data=buttons[0]["callback_data"],
        message=dict(message_id=123, chat=dict(id=42)),
        **{"from": dict(id=41)},
    )
    service.ux.tiger.open = AsyncMock(return_value="accepted")
    assert await service.ux.callback(query, None) == "Действие недоступно."
    query["from"]["id"] = 42
    assert await service.ux.callback(query, None) == "accepted"
    await service.ux.callback(query, None)
    service.ux.tiger.open.assert_awaited_once()
    service.journal.db.execute("UPDATE telegram_actions SET expires_at=0")
    query["id"] = "q2"
    assert "устарела" in await service.ux.callback(query, None)
    service.ux.tiger.open.assert_awaited_once()


@pytest.mark.asyncio
async def test_details_use_same_card_and_never_accumulate_text(service):
    now = time.time()
    observe(service, now)
    await service.outbox.once(now)
    row = operations(service)[0]
    query = dict(
        id="d1",
        data=json.loads(row["reply_markup"])["inline_keyboard"][0][-1]["callback_data"],
        message=dict(message_id=123, chat=dict(id=42)),
        **{"from": dict(id=42)},
    )
    await service.ux.callback(query, None)
    first = operations(service)[0]["text"]
    query["id"] = "d2"
    await service.ux.callback(query, None)
    second = operations(service)[0]["text"]
    assert first == second and second.count("Сценарии наблюдения") == 1
    await service.outbox.once(now + 1)
    assert service.hub.alerts.send_telegram.await_count == 1
    assert service.hub.alerts.edit_telegram.await_args.args[0] == 123


@pytest.mark.asyncio
async def test_tiger_wire_only_switches_chart(service, monkeypatch):
    commands = []

    async def endpoint(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        message = await ws.receive_json()
        commands.append(message)
        await ws.send_json(dict(requestId=message["requestId"], response=dict(type="successfully")))
        await ws.close()
        return ws

    app = web.Application()
    app.router.add_get("/", endpoint)
    async with TestServer(app) as server:
        monkeypatch.setenv("SCALPER_TIGER_WS_URL", str(server.make_url("/")).replace("http:", "ws:"))
        bridge = TigerBridge()
        async with aiohttp.ClientSession() as session:
            assert "принял" in await bridge.open(session, "BTC")
    assert len(commands) == 1
    assert {k: v for k, v in commands[0].items() if k != "requestId"} == dict(
        type="SETLINKSYMBOL", e="BINANCE-FUT", m="FUTURES", symbol="BTCUSDT", linkGroup="S"
    )
    monkeypatch.setenv("SCALPER_TIGER_WS_URL", "ws://192.168.0.1:7819/")
    with pytest.raises(ValueError):
        TigerBridge().command("BTC")
    with pytest.raises(ValueError):
        bridge.command("kPEPE")  # Never invent a 1000-contract mapping.


@pytest.mark.asyncio
async def test_rich_fallback_keeps_destination_buttons_and_silent_flag():
    from hyperdata_terminal.data_layer.alerts import AlertManager

    alerts = AlertManager()
    alerts.telegram_token, alerts.telegram_chat_id = "test-token", "42"
    alerts._session = MagicMock()
    for_context = []
    for status, body in ((400, dict(ok=False)), (200, dict(ok=True, result=dict(message_id=9)))):
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=SimpleNamespace(status=status, json=AsyncMock(return_value=body)))
        context.__aexit__ = AsyncMock(return_value=None)
        for_context.append(context)
    alerts._session.post.side_effect = for_context
    markup = dict(inline_keyboard=[[dict(text="Test", callback_data="ux:test")]])
    assert await alerts.send_telegram(
        "text", rich_html="<p>text</p>", chat_id="-1001", reply_markup=markup, silent=True, return_message=True
    ) == (9, False)
    for call in alerts._session.post.call_args_list:
        assert call.kwargs["json"]["chat_id"] == "-1001"
        assert call.kwargs["json"]["reply_markup"] == markup
        assert call.kwargs["json"]["disable_notification"] is True


def test_unverified_ai_and_stale_reviews_cannot_publish(service):
    now = time.time()
    for status, expired in (("invalid_response", False), ("ok", True)):
        assert not service.ux.publish_analysis(
            dict(
                status=status,
                packet=packet(now),
                review=dict(result=dict(abstention=False, expired_at_completion=expired)),
            )
        )
    text, rich = operational(packet(now), summarize(packet(now)), "density_context", "situation")
    assert "AI" not in text and len(text) < 900
    assert "не определён" not in text and "Фаза не определена" not in text


def test_card_hides_side_on_a_thin_tape_and_flags_wide_spread():
    now = time.time()
    state = dict(summarize(packet(now)), flow=-1.0, count=4, wide_spread=True)
    text, _ = operational(packet(now), state, "density_context", "situation", ratio=0)
    assert "Сделок мало (4" in text and "продажи" not in text and "▼" not in text
    assert "0× нормы" not in text and "⚠ широкий спред" in text
    state = dict(state, count=40, zone=dict(state["zone"], notional_usd=48_000, size_to_observed_turnover=0.4))
    text, _ = operational(packet(now), state, "density_context", "situation")
    assert "▼ продажи 100%" in text and "$48k" in text and "≈24с оборота" in text


def test_attention_reacquisition_does_not_repeat_the_same_situation(service):
    now = time.time()
    observe(service, now)
    observe(service, now + 150, assigned=2)
    assert len(operations(service)) == 1


def test_new_level_is_a_separate_material_event(service):
    now = time.time()
    observe(service, now)
    observe(service, now + 150, level=9.95)
    assert len(operations(service)) == 2
    assert "новый значимый уровень" in operations(service)[1]["text"]


def test_migration_preserves_old_message_without_retrospective_push(service):
    now = time.time()
    p = packet(now)
    service.outbox.enqueue(p["episode_id"], "previous format", "<p>previous</p>", now + 120)
    service.journal.db.execute("UPDATE episode_cards SET status='sent',message_id=99")
    observe(service, now)
    assert not operations(service)
    assert (
        service.journal.db.execute(
            "SELECT message_id FROM episode_cards WHERE episode_id=?", (p["episode_id"],)
        ).fetchone()[0]
        == 99
    )


@pytest.mark.asyncio
async def test_expired_analytics_is_not_backfilled_and_same_chat_is_rejected(service, monkeypatch):
    now = time.time()
    monkeypatch.setenv("SCALPER_ANALYTICS_CHAT_ID", "42")
    response = dict(
        status="ok",
        packet=packet(now),
        review=dict(result=dict(abstention=False, expired_at_completion=False, claims=[])),
    )
    assert not service.ux.analytics_chat()
    service.ux.publish_analysis(response)
    monkeypatch.setenv("SCALPER_ANALYTICS_CHAT_ID", "-100123")
    await service.outbox.once(now + 301)
    service.hub.alerts.send_telegram.assert_not_awaited()
    assert (
        service.journal.db.execute("SELECT status FROM episode_cards WHERE destination='analytics'").fetchone()[0]
        == "expired"
    )


def test_invalid_tiger_configuration_does_not_stop_the_screener(service, monkeypatch):
    monkeypatch.setenv("SCALPER_TIGER_SYMBOLS", "[")
    service.ux = TelegramUX(service)
    observe(service, time.time())
    row = operations(service)[0]
    assert "TigerTrade" not in row["reply_markup"]
    assert service.ux.status()["tiger_config_error"]


def test_manual_report_is_concise_and_empty_meta_is_not_published(service):
    now = time.time()
    observe(service, now)
    text = service.ux.report(now + 1)
    assert "Отобрано событий: 1" in text and "торговых результатов" in text and len(text) < 500
    p = packet(now)
    p["symbol"] = "MARKET"
    assert not service.ux.publish_analysis(
        dict(status="ok", packet=p, review=dict(result=dict(abstention=False, expired_at_completion=False, claims=[])))
    )
