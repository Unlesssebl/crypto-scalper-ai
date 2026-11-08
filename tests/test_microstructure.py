"""Effort vs result: recent 15s of the tape against the 30s impulse before it."""

import json

import pytest

from extensions.scalper.context_auto import summarize
from extensions.scalper.fact_packet import build_packet
from extensions.scalper.history.archive import Archive
from extensions.scalper.history.facts import Facts, tape_segments
from extensions.scalper.history.replay import replay
from extensions.scalper.microstructure import label, read_tape
from extensions.scalper.telegram_ux import operational, tape_lines


def fake(prior, recent, spread=1.0, natr=None):
    facts = [dict(id="book.spread_bps", value=spread, quality="ok")]
    if natr is not None:
        facts.append(dict(id="context.1m.natr", value=natr, quality="ok"))
    for name, seg in (("prior", prior), ("recent", recent)):
        for key, v in seg.items():
            facts.append(dict(id=f"tape.{name}.{key}", value=v, quality="ok"))
    return dict(facts=facts)


def seg(buy, sell, count, move):
    return dict(buy_usd=buy, sell_usd=sell, trade_count=count, price_change_bps=move)


IMPULSE = seg(300_000, 30_000, 60, 30)  # buyers lifted prices 30 bps in 30s


@pytest.mark.parametrize(
    "recent, kind",
    [
        (seg(200_000, 20_000, 40, 0.5), "absorption"),  # same pace of buying, prices stuck
        (seg(20_000, 10_000, 5, 0.3), "exhaustion"),  # buying faded, prices stuck
        (seg(30_000, 250_000, 40, -4), "counter_aggression"),  # sellers now hit the bid
        (seg(30_000, 5_000, 6, 25), "thin_move"),  # prices run faster on little buying
        (seg(160_000, 20_000, 30, 14), "continuation"),
        (seg(110_000, 20_000, 20, 1), "slowdown"),  # stuck, effort neither held nor faded
    ],
)
def test_reading_separates_why_a_move_stops(recent, kind):
    reading = read_tape(fake(IMPULSE, recent))
    assert reading["kind"] == kind and reading["side"] == 1


def test_counter_aggression_leads_with_the_side_taking_over():
    lines = tape_lines(read_tape(fake(IMPULSE, seg(30_000, 250_000, 40, -4))))
    assert lines[0] == "🟩 Покупки $30k · 🟥 Продажи $250k · за 15с"
    assert lines[1] == "↩️ До этого +0.30% за 30с · покупки ослабли в 5 раз"


def test_price_going_back_is_a_reversal_not_a_stall():
    # TA, 08.11 14:14: buyers faded, sellers matched them and prices gave back 48 bps of a 176 bps impulse.
    # NATR 1.84%: the 15s reversal threshold is half the 30s impulse one (27.6 bps), so -48 bps counts.
    ta = fake(seg(86_376, 52_347, 1899, 175.75), seg(16_442, 17_024, 466, -48.3), spread=16.6, natr=1.84)
    reading = read_tape(ta)
    assert reading["kind"] == "counter_aggression"
    state = dict(tape=reading, midpoint=0.09667, spread=16.6, flow=0.19, count=2384, window="observed_60s",
                 tape_change=118)
    text, _ = operational(dict(symbol="BINANCE:TA", captured_at=0, facts=[]), state, "active_instrument", "situation")
    assert text.startswith("<b>TA ▼ -0.48% за 15с</b>\n⚡ Продавцы перехватывают инициативу\n")
    assert "🟩 Покупки $16.4k · 🟥 Продажи $17k · за 15с" in text and "покупки ослабли в 2.6 раза" in text
    # Impulse buyers still outweighing sellers while prices fall: their buying is being absorbed.
    assert read_tape(fake(IMPULSE, seg(200_000, 40_000, 40, -12)))["kind"] == "absorption"


def test_mirror_for_a_falling_market_and_labels_are_hypotheses():
    reading = read_tape(fake(seg(30_000, 300_000, 60, -30), seg(20_000, 200_000, 40, -0.5)))
    assert reading["kind"] == "absorption" and reading["side"] == -1
    assert label(reading) == "Вероятно поглощение продаж"


def test_no_reading_without_an_impulse_or_a_complete_tape():
    assert read_tape(fake(seg(300_000, 30_000, 60, 1), seg(1, 1, 1, 0))) is None  # no travel: noise
    assert read_tape(fake(seg(160_000, 150_000, 60, 30), seg(1, 1, 1, 0))) is None  # no dominant side
    assert read_tape(fake(seg(300_000, 30_000, 4, 30), seg(1, 1, 1, 0))) is None  # too few prints
    assert read_tape(fake(IMPULSE, {})) is None  # recent stretch not observed
    assert read_tape(fake(IMPULSE, seg(1, 1, 1, 0), spread=20)) is None  # 30 bps < 2× a 20 bps spread
    assert read_tape(fake(seg(3_000, 300, 60, 30), seg(1, 1, 1, 0))) is None  # dust: $3k is not an impulse


def test_silent_recent_stretch_is_exhaustion_not_missing_data():
    reading = read_tape(fake(IMPULSE, dict(buy_usd=0, sell_usd=0, trade_count=0)))
    assert reading["kind"] == "exhaustion" and reading["recent"]["move_bps"] is None


def test_card_shows_effort_result_and_the_facing_wall():
    zone = dict(side="ask", price=100.05, notional_usd=95_000)
    reading = read_tape(fake(IMPULSE, seg(200_000, 20_000, 40, 0.5)), zone=zone, midpoint=100)
    assert tape_lines(reading) == [
        "🟩 Покупки $200k · 🟥 Продажи $20k · за 15с",
        "↩️ До этого +0.30% за 30с · покупки усилились в 1.3 раза · объём не двигает цену",
    ]
    state = dict(tape=reading, zone=dict(zone, relative_size=40, observed_age_seconds=40), midpoint=100,
                 spread=1, flow=0.8, count=40, window="1m")
    text, _ = operational(dict(symbol="BTC", captured_at=0, facts=[]), state, "flow_price_conflict", "situation")
    assert text.startswith("<b>BTC ▲ +0.01% за 15с</b>\n⚡ Вероятно поглощение покупок\n")
    assert "🧱 На пути: Предложение <code>100.05</code> (+0.05%) · $95k" in text


def test_segments_need_the_whole_span_observed():
    now = 100
    trades = [(now - 50 + i, 1000.0, f"r{i}", "B", 1 + i / 1000) for i in range(50)]
    out = tape_segments(trades, now, truncated=False)
    assert out["prior"]["complete"] and out["recent"]["complete"]
    assert out["prior"]["buy_usd"] == 30_000 and out["recent"]["trade_count"] == 14  # prints at 86..99
    assert out["recent"]["open_price"] == trades[35][4]  # last print at or before t=85
    late = tape_segments(trades[30:], now, truncated=False)  # tape started 20s ago
    assert not late["prior"]["complete"] and late["recent"]["complete"]
    assert tape_segments(trades, now, truncated=True) is None


def test_v3_facts_flow_into_the_packet_card_and_replay(tmp_path):
    now = 200.0
    facts = Facts(version=3)
    archive = Archive(tmp_path / "history")
    archive.record("source", dict(fact_version=3, assignment_mode="external_plan.v1"), 150)
    assignment = dict(symbol="SMALL", assigned_at=150, state="focus", previous="scout", at=150, score=10)
    facts.apply(archive.record("assignment", assignment, 150))
    price, tid = 100.0, 0
    for t in range(150, 201):
        # 30 bps of buying-led travel up to t=185, then buyers keep hitting a price that no longer moves.
        price = 100 * (1 + 0.003 * min(t - 155, 30) / 30) if t >= 155 else 100.0
        tid += 1
        trade = dict(coin="SMALL", time=t * 1000, px=str(price), sz="100", tid=tid, side="B")
        facts.apply(archive.record("raw", dict(message=dict(channel="trades", data=[trade])), t, epoch="e"))
    book = dict(coin="SMALL", time=now * 1000,
                levels=[[dict(px="100.29", sz="10")], [dict(px="100.31", sz="10"), dict(px="100.32", sz="900")]])
    facts.apply(archive.record("raw", dict(message=dict(channel="l2Book", data=book)), now, epoch="e"))
    card = facts.card("SMALL", now)
    archive.record("facts", card, now)
    archive.close()
    assert replay(archive.path)["verified"]
    packet = build_packet(dict(policy_version=4, last_scout_at=now), {**assignment, "facts": card}, now)
    state = summarize(packet)
    assert state["tape"]["kind"] == "absorption"
    assert state["tape_change"] == pytest.approx(30, abs=0.5)
    text, _ = operational(packet, state, "flow_price_conflict", "situation")
    assert text.startswith("<b>SMALL ▲ 0% за 15с</b>\n⚡ Вероятно поглощение покупок")
    json.dumps(state)  # stored as JSON in telegram_ux_events
