"""Signals for a terminal chart (TigerTrade indicator pilot)."""

import time

import pytest

from extensions.scalper.chart_marks import card_lines, chart_marks, screener_symbols
from tests.test_telegram_product import observe
from tests.test_telegram_product import service as service


@pytest.mark.parametrize(
    "code, exchange, expected",
    [
        ("BTCUSDT", "BINANCE-FUT", ["BINANCE:BTC", "BTC"]),
        ("IDOLUSDT_FUT_BINANCE-FUT", "BINANCE-FUT", ["BINANCE:IDOL", "IDOL"]),  # TigerTrade's Symbol.Code
        ("ETHUSDT.P", "BINANCE-FUT", ["BINANCE:ETH", "ETH"]),
        ("1000PEPEUSDT", "BINANCE-FUT", ["BINANCE:1000PEPE", "1000PEPE"]),
        ("SOL", "HYPERLIQUID", ["SOL", "BINANCE:SOL"]),
        ("bad code", "BINANCE-FUT", []),
        ("", "", []),
    ],
)
def test_terminal_instrument_maps_to_screener_symbols(code, exchange, expected):
    assert screener_symbols(code, exchange) == expected


def test_tape_numbers_follow_the_sides_not_the_impulse():
    from extensions.scalper.chart_marks import tape_of

    reading = dict(kind="counter_aggression", side=1, recent=dict(aggressor_usd=16_442, opposite_usd=17_024,
                   seconds=15, move_bps=-48.3), prior=dict(seconds=30, move_bps=175.75))
    tape = tape_of(dict(tape=reading))
    assert tape["buys_usd"] == 16_442 and tape["sells_usd"] == 17_024 and tape["prior_move_bps"] == 175.75
    falling = tape_of(dict(tape=dict(reading, side=-1)))
    assert falling["buys_usd"] == 17_024 and falling["sells_usd"] == 16_442


def test_card_lines_drop_html_but_keep_the_words():
    assert card_lines("<b>🔸 T1 · BTC ▲</b>\n🧱 Спрос <code>9.99</code> (-0.10%)\n\n<i>x &amp; y</i>") == [
        "🔸 T1 · BTC ▲", "🧱 Спрос 9.99 (-0.10%)", "x & y",
    ]


def test_zone_line_is_spelled_out_for_the_tooltip():
    from extensions.scalper.chart_marks import zone_lines

    state = dict(midpoint=1277.35, zone=dict(side="bid", price=1275.9, notional_usd=152_000,
                 size_to_observed_turnover=4.7, observed_age_seconds=300))
    assert zone_lines(state) == [
        "🧱 Зелёная полоса — заявки на покупку: 1275.9 (-0.11% от цены) на $152k",
        "Чтобы её съесть, нужно ~5 мин сделок в нынешнем темпе — крупная",
    ]
    ask = dict(midpoint=100, tape=dict(wall=dict(price=100.05)),
               zone=dict(side="ask", price=100.05, size_to_observed_turnover=0.2, observed_age_seconds=4))
    head, eat, new = zone_lines(ask)
    assert head.startswith("🧱 Красная полоса — заявки на продажу на пути цены: 100.05")
    assert eat.endswith("~12с сделок в нынешнем темпе — тонкая") and new.startswith("Появилась")
    assert zone_lines(dict(midpoint=1)) == []


def test_marks_return_the_sent_card_for_the_chart_instrument(service):
    now = time.time()
    observe(service, now)  # BTC on Hyperliquid, T1 notable → a sent card
    out = chart_marks(service, "BTCUSDT", "BINANCE-FUT", 60, now + 1)
    assert out["symbols"] == ["BINANCE:BTC", "BTC"]
    [mark] = out["marks"]
    assert mark["symbol"] == "BTC" and mark["venue"] == "Hyperliquid" and mark["side"] == 1
    assert mark["tier"] == 1 and mark["strength"] == "notable" and mark["price"] == 10
    assert mark["lines"][0] == "🔸 T1 · BTC ▲" and not any("<" in line for line in mark["lines"])
    assert any(line.startswith("🧱 Зелёная полоса — заявки на покупку") for line in mark["lines"])
    assert not any(line.startswith("🧱 Спрос") for line in mark["lines"])
    zone = mark["zone"]
    assert zone["side"] == "bid" and zone["price"] == 9.99 and zone["notional_usd"] is None
    assert zone["first_seen"] == pytest.approx(mark["at"] - 40_000, abs=1)  # visible 40s before the card
    assert mark["tape"] is None  # this packet has no 15s/30s tape segments
    assert not any("Hyperliquid ·" in line for line in mark["lines"])  # footer is the chart's own axis
    assert chart_marks(service, "ETHUSDT", "BINANCE-FUT", 60, now + 1)["marks"] == []
    assert chart_marks(service, "BTCUSDT", "BINANCE-FUT", 1, now + 3600)["marks"] == []  # outside the window
