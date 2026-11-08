"""Several Telegram bots: routing, polling, /stats; liquidations as card context."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.channels import handle_update, load_channels, senders_by_destination, stats_text
from extensions.scalper.delivery import CardOutbox
from extensions.scalper.liquidation_context import LiquidationContext, liquidation_line
from extensions.scalper.signal_outcomes import SCORE_AFTER
from extensions.scalper.telegram_ux import operational, usd
from tests.test_signal_outcomes import QUIET, Hub, bars, history
from tests.test_telegram_product import observe, packet
from tests.test_telegram_product import service as service


def test_each_channel_has_its_own_bot_and_an_unset_one_stays_silent(monkeypatch):
    monkeypatch.setenv("SCALPER_MOVERS_BOT_TOKEN", "111:movers")
    monkeypatch.setenv("SCALPER_MOVERS_CHAT_ID", "42")
    monkeypatch.delenv("SCALPER_LEVELS_BOT_TOKEN", raising=False)
    main = SimpleNamespace(telegram_token="000:main", telegram_chat_id="42")
    channels = load_channels(main)
    assert channels["anomalies"].sender is main and channels["anomalies"].spec.desktop
    assert channels["movers"].sender.telegram_token == "111:movers" and channels["movers"].configured
    assert not channels["levels"].configured and not channels["movers"].spec.desktop
    assert set(senders_by_destination(channels)) == {"movers", "levels"}


@pytest.mark.asyncio
async def test_outbox_sends_each_card_through_its_destination_bot(service):
    movers = SimpleNamespace(send_telegram=AsyncMock(return_value=(7, True)), edit_telegram=AsyncMock(),
                             last_telegram_result=dict(state="sent"))
    outbox = CardOutbox(service.journal, service.hub.alerts, {"movers": movers})
    now = time.time()
    outbox.enqueue("ux:movers:1", "card", "<p>card</p>", now + 60, chat_id="42", destination="movers")
    await outbox.once(now)
    movers.send_telegram.assert_awaited_once()
    service.hub.alerts.send_telegram.assert_not_awaited()
    outbox.enqueue("ux:ops:1", "ops", "<p>ops</p>", now + 60, chat_id="42", destination="operations")
    await outbox.once(now)
    service.hub.alerts.send_telegram.assert_awaited_once()


@pytest.mark.asyncio
async def test_stats_counts_only_this_bots_cards(service):
    now = time.time()
    observe(service, now)  # a T1 notable ▲ BTC card from the anomalies bot
    sent_at = service.journal.db.execute("SELECT at FROM telegram_ux_events").fetchone()[0]
    start = int(sent_at // 60 * 60)
    # A quiet hour (1% range, the background never reaches it), then the card and a 2% rise.
    candles = bars(*history(), (10.05, 9.95, 10), (10.2, 9.97, 10.2), *[QUIET] * 15, start=start - 75 * 60)
    await service.ux.outcomes.once(Hub(candles), sent_at + SCORE_AFTER + 1)
    service.telegram_ux = service.ux
    channels = load_channels(service.hub.alerts)
    text = stats_text(service, channels["anomalies"], sent_at + SCORE_AFTER + 2)
    assert "сигналов 1 · мало данных" in text
    assert "Со стороной 1: ✅ подтвердилось 1 (100%) · ❌ против 0 · ➖ без движения 0 · фон 0%" in text
    assert "🔸 заметные: подтвердилось 1 из 1 · фон 0%" in text
    assert "сигналов 0" in stats_text(service, channels["movers"], sent_at + SCORE_AFTER + 2)


@pytest.mark.asyncio
async def test_extra_bot_answers_stats_and_buttons_only_from_its_chat(service, monkeypatch):
    monkeypatch.setenv("SCALPER_MOVERS_BOT_TOKEN", "111:movers")
    monkeypatch.setenv("SCALPER_MOVERS_CHAT_ID", "42")
    service.telegram_ux = service.ux
    channel = load_channels(service.hub.alerts)["movers"]
    channel.sender.send_telegram = AsyncMock(return_value=True)
    calls = []

    async def fake_call(session, token, method, params=None, timeout=None):
        calls.append((token, method, params))
        return True

    monkeypatch.setattr("extensions.scalper.telegram.telegram_call", fake_call)
    await handle_update(None, service, channel, "111:movers", {"message": {"chat": {"id": 99}, "text": "/stats"}})
    channel.sender.send_telegram.assert_not_awaited()
    await handle_update(None, service, channel, "111:movers", {"message": {"chat": {"id": 42}, "text": "/stats"}})
    channel.sender.send_telegram.assert_awaited_once()
    service.ux.callback = AsyncMock(return_value="TigerTrade: BTCUSDT открыт в группе S.")
    await handle_update(None, service, channel, "111:movers", {"callback_query": {"id": "q1", "data": "ux:x"}})
    assert calls[-1][:2] == ("111:movers", "answerCallbackQuery") and calls[-1][2]["show_alert"] is False


def event(symbol, side, size, ago, now, confirmed=True):
    return dict(timestamp=now - ago, symbol=symbol, side=side, size_usd=size, exchange="binance", confirmed=confirmed)


def test_only_confirmed_recent_liquidations_count_and_a_cascade_needs_the_price_on_its_side():
    now = 1_000_000.0
    context = LiquidationContext()
    context.ingest([
        event("BTC", "long", 900_000, 10, now),
        event("BTC", "long", 300_000, 30, now),
        event("BTC", "short", 40_000, 5, now),
        event("BTC", "long", 5_000_000, 20, now, confirmed=False),  # Hyperliquid estimate: ignored
        event("BTC", "long", 700_000, 120, now),  # older than 60s
        event("ETH", "long", 1_000_000, 5, now),
    ], now)
    falling = context.recent("BINANCE:BTC", now, turnover_60s=3_000_000, move_bps=-40)
    assert falling["longs_usd"] == 1_200_000 and falling["shorts_usd"] == 40_000 and falling["cascade"]
    assert not context.recent("BTC", now, turnover_60s=3_000_000, move_bps=+15)["cascade"]  # longs dumped, price up
    assert not context.recent("BTC", now, turnover_60s=20_000_000, move_bps=-40)["cascade"]  # small vs turnover
    assert context.recent("BTC", now + 100) is None  # feed stale: no line rather than a false zero
    assert liquidation_line(falling, usd) == "💥 Ликвидации за 60с: лонги $1.2M · шорты $40k · движение на каскаде"
    assert liquidation_line(context.recent("SOL", now), usd) is None


def test_card_shows_the_liquidation_line_and_marks_the_verdict():
    now = time.time()
    from extensions.scalper.context_auto import summarize

    p = packet(now)
    state = dict(summarize(p), liquidations=dict(longs_usd=0, shorts_usd=800_000, count=3, venues=["binance"],
                                                 cascade=True, cascade_side="short"))
    text, _ = operational(p, state, "density_context", "situation")
    assert "🟩 Покупатели давят · 💥 на каскаде" in text
    assert "💥 Ликвидации за 60с: шорты $800k · движение на каскаде" in text
