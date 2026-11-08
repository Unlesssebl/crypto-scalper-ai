"""Levels bot: densities relative to their own instrument, appear/approach once per level."""

import json
import time
from types import SimpleNamespace

import pytest

from extensions.scalper.channels import load_channels
from extensions.scalper.fact_packet import build_packet
from extensions.scalper.levels import LevelWatcher, zones_of
from extensions.scalper.signal_outcomes import HORIZON, SETTLE
from tests.test_signal_outcomes import Hub, bars
from tests.test_telegram_product import service as service


def zone(price=99.8, size=1000, rel=12, age=40, distance=20, eat=0.6, side="bid"):
    return dict(id=f"density.{side}.{price}", side=side, price=price, size=size, relative_size=rel,
                observed_age_seconds=age, distance_bps=distance, size_to_observed_turnover=eat,
                notional_usd=price * size)


def test_significance_is_relative_to_the_instrument():
    w = LevelWatcher()
    assert w.significant(zone(), natr=None)
    assert not w.significant(zone(rel=4), natr=None)  # ordinary for this book
    assert not w.significant(zone(eat=0.2), natr=None)  # 12s of this instrument's own turnover: eaten at once
    assert not w.significant(zone(age=8), natr=None)  # not standing yet
    assert w.significant(zone(distance=90), natr=0.5)  # 0.9% is near for a 0.5% minute
    assert not w.significant(zone(distance=30), natr=0.1)  # 0.3% is far for a 0.1% minute
    # No dollar floor: a $300 level in a thin book is a level.
    assert w.significant(dict(zone(), notional_usd=300), natr=None)


def test_adjacent_levels_on_one_side_are_one_wall():
    # TRUMP 08.11 18:04: bids at 1.1858 and 1.1859 produced two identical cards.
    w = LevelWatcher()
    events = w.process("TRUMP", [zone(price=1.1858, rel=11), zone(price=1.1859, rel=12),
                                 zone(price=1.1950, rel=9, side="ask")], None, 1_000_000.0)
    assert [(e["zone"]["side"], e["zone"]["price"]) for e in events] == [("bid", 1.1859), ("ask", 1.1950)]


def test_each_level_fires_appeared_then_approach_once():
    w = LevelWatcher()
    now = 1_000_000.0
    assert [e["kind"] for e in w.process("X", [zone()], None, now)] == ["appeared"]
    assert w.process("X", [zone()], None, now + 5) == []
    [approach] = w.process("X", [zone(distance=6)], None, now + 20)  # within 0.1% without NATR
    assert approach["kind"] == "approach" and approach["since"] == 20
    assert w.process("X", [zone(distance=3)], None, now + 25) == []
    w2 = LevelWatcher()
    w2.process("X", [zone()], None, now)
    [eaten] = w2.process("X", [zone(size=600)], None, now + 30)  # 40% traded through while still far
    assert eaten["eaten_share"] == pytest.approx(0.4)
    w2.process("X", [], None, now + 500)
    assert w2.known == {}  # forgotten after it vanished


def card_packet(now, *, eat=0.6, rel=12, distance_price=99.8, size=1000, mid=100.0):
    card = dict(status="observing", received_at=now, midpoint=mid, spread_bps=2, observed_turnover_60s_usd=200_000,
                observed_trade_imbalance=0.4, observed_trade_count_60s=50, last_trade_received_at=now,
                trade_evidence=["run:trade"], evidence="run:book",
                densities=[dict(side="bid", price=distance_price, size=size, notional_usd=distance_price * size,
                                relative_size=rel, observed_age_seconds=40,
                                distance_bps=abs(distance_price / mid - 1) * 10000,
                                size_to_observed_turnover=eat, evidence="run:book")])
    assignment = dict(symbol="BTC", state="focus", assigned_at=1, score=6, facts=card)
    return build_packet(dict(policy_version=4, archive=dict(error=None)), assignment, now)


def test_zones_of_reads_every_zone_with_relative_measures():
    [z] = zones_of(card_packet(time.time()))
    assert z["side"] == "bid" and z["relative_size"] == 12 and z["size_to_observed_turnover"] == 0.6


def levels_rows(service):
    return list(service.journal.db.execute("SELECT * FROM episode_cards WHERE destination='levels'"))


@pytest.mark.asyncio
async def test_level_cards_go_to_the_levels_bot_and_score_as_hold(service, monkeypatch):
    monkeypatch.setenv("SCALPER_LEVELS_BOT_TOKEN", "222:levels")
    monkeypatch.setenv("SCALPER_LEVELS_CHAT_ID", "42")
    service.channels = load_channels(service.hub.alerts)
    now = time.time()
    service.ux.observe_levels(card_packet(now), now)
    [row] = levels_rows(service)
    assert row["text"].startswith("<b>🔸 T1 · BTC · 🧱 Спрос <code>99.8</code> (-0.20%)</b>")
    assert "Появилась крупная заявка и стоит 40с" in row["text"] and "12× обычного уровня стакана" in row["text"]
    assert row["silent"] == 1  # a new level is notable: no sound
    service.ux.observe_levels(card_packet(now + 20, mid=99.85), now + 20)  # the price came down to the bid
    rows = levels_rows(service)
    assert len(rows) == 2 and "⚡ Цена подошла к заявке" in rows[1]["text"] and rows[1]["silent"] == 0
    state = json.loads(service.journal.db.execute(
        "SELECT state FROM telegram_ux_events WHERE reason='level_appeared'").fetchone()[0])
    assert state["level"]["zone"]["side"] == "bid"
    start = int(now // 60 * 60)
    candles = bars(*[(100.2, 99.7, 100)] + [(100.6, 99.9, 100.4)] * 6, start=start)
    await service.ux.outcomes.once(Hub(candles), now + 20 + HORIZON + SETTLE + 1)
    sides = {r[0] for r in service.journal.db.execute("SELECT side FROM signal_outcomes")}
    assert sides == {1}  # "the bid held": scored upward regardless of the tape arrow


def test_illiquid_levels_stay_quiet(service, monkeypatch):
    service.hub.market.assets["BTC"] = SimpleNamespace(volume_24h=500e3)  # T4
    now = time.time()
    service.ux.observe_levels(card_packet(now), now)
    assert levels_rows(service) == []
