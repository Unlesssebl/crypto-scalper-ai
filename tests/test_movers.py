"""Liquid movers: own slots outside the relative allocator, cards to the movers bot."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.channels import load_channels
from extensions.scalper.movers import MoversSelector
from extensions.scalper.unified_attention import UnifiedAttention
from tests.test_telegram_product import observe, operations
from tests.test_telegram_product import service as service


def asset(symbol, price, volume, change_24h=0.0, at=None):
    return dict(symbol=symbol, price=price, volume_24h=volume, price_change_24h_pct=change_24h,
                observed_at=at if at is not None else time.time())


def test_liquid_day_mover_is_picked_and_illiquid_one_is_not():
    selector = MoversSelector(slots=2)
    now = 1_000_000.0
    picked = selector.choose([
        asset("BINANCE:JCT", 0.0132, 936e6, 2.87, now),  # +287% in 24h, $936M: the JCT case
        asset("BINANCE:DUST", 1.0, 1e6, 5.0, now),  # +500% but $1M a day: T4, never a mover
        asset("BINANCE:BTC", 100_000, 2e9, 0.001, now),  # liquid but not moving
    ], now)
    assert picked == ["BINANCE:JCT"]
    row = selector.rows()[0]
    assert row["state"] == "focus" and row["channel"] == "movers" and row["interest"]["change_24h"] == 2.87


def test_hourly_move_needs_fifty_minutes_of_own_samples():
    selector = MoversSelector(slots=1)
    t0 = 1_000_000.0
    for i in range(0, 3601, 60):
        selector.choose([asset("SOL", 100 * (1 + 0.03 * i / 3600), 60e6, 0.04, t0 + i)], t0 + i)
        if i < 3000:
            assert selector.picked == {}  # +3% so far, but under 50 minutes of history
    assert list(selector.picked) == ["SOL"] and selector.picked["SOL"]["change_1h"] == pytest.approx(0.03, abs=1e-3)


def test_pick_holds_its_slot_and_capacity_is_respected():
    selector = MoversSelector(slots=2, hold_seconds=600)
    now = 1_000_000.0
    selector.choose([asset("A", 1, 50e6, 0.5, now), asset("B", 1, 50e6, 0.3, now), asset("C", 1, 50e6, 0.25, now)], now)
    assert list(selector.picked) == ["A", "B"]
    # A calms down but keeps its slot within the hold; capacity 1 keeps the strongest.
    selector.choose([asset("A", 1, 50e6, 0.0, now + 60), asset("B", 1, 50e6, 0.3, now + 60)], now + 60, capacity=1)
    assert list(selector.picked) == ["B"]
    assert "A" not in selector.choose([asset("A", 1, 50e6, 0.9, now + 61)], now + 61, exclude={"A"})  # anomaly owns A
    assert selector.choose([], now + 400) == []  # nothing fresh in the snapshot: no picks on old numbers


def test_plan_reserves_mover_slots_beyond_anomalies_within_the_native_limit():
    attention = UnifiedAttention(None, slots=30, detail_slots=18, movers_slots=4)
    assert attention.movers.slots == 2  # 18 anomaly slots + 2 = the plan's 20 order books
    now = time.time()
    attention.choose([asset("BINANCE:JCT", 0.0132, 936e6, 2.87, now), asset("BTC", 1e5, 2e9, 0.0, now)], now)
    plan = attention.plan
    assert plan["detail"] == ["BINANCE:JCT"] and plan["selected"][0] == "BINANCE:JCT"
    assert {r["symbol"] for r in plan["active"]} == set(plan["detail"])
    from hyperdata_terminal.market_pulse.control import validate_plan

    validate_plan(plan, int(now * 1000))  # the native hub accepts it


def test_mover_episode_goes_to_the_movers_bot_without_a_desktop_toast(service, monkeypatch):
    monkeypatch.setenv("SCALPER_MOVERS_BOT_TOKEN", "111:movers")
    monkeypatch.setenv("SCALPER_MOVERS_CHAT_ID", "42")
    service.channels = load_channels(service.hub.alerts)
    service.attention.movers = SimpleNamespace(picked={"BTC": dict(change_1h=0.031, change_24h=0.42,
                                                                   volume_24h=1e9, score=2.1,
                                                                   assigned_at=time.time() - 120)})
    service.ux.desktop = SimpleNamespace(notify=AsyncMock(), status=lambda: {})
    observe(service, time.time())
    rows = list(service.journal.db.execute("SELECT * FROM episode_cards WHERE destination='movers'"))
    assert len(rows) == 1 and rows[0]["episode_id"].startswith("ux:mov:")
    assert "🚀 +42.0% за 24ч · +3.10% за 1ч · оборот $1B за 24ч" in rows[0]["text"]
    assert operations(service) == [] and "ликвидное движение" in rows[0]["text"]
    service.ux.desktop.notify.assert_not_called()
    channel = service.journal.db.execute("SELECT channel FROM telegram_ux_events").fetchone()[0]
    assert channel == "movers"


def test_first_mover_card_waits_for_a_warm_tape(service, monkeypatch):
    monkeypatch.setenv("SCALPER_MOVERS_BOT_TOKEN", "111:movers")
    monkeypatch.setenv("SCALPER_MOVERS_CHAT_ID", "42")
    service.channels = load_channels(service.hub.alerts)
    now = time.time()
    service.attention.movers = SimpleNamespace(picked={"BTC": dict(change_24h=0.42, volume_24h=1e9, score=2.1,
                                                                   assigned_at=now - 10)})
    import extensions.scalper.telegram_ux as ux

    real = ux.decide
    monkeypatch.setattr(ux, "decide", lambda packet, samples: (None, "ordinary_context", real(packet, samples)[2]))
    observe(service, now, flow=0, count=1, ratio=0)  # just subscribed: one print, nothing to say yet
    assert not list(service.journal.db.execute("SELECT 1 FROM episode_cards WHERE destination='movers'"))
    observe(service, now + 5, flow=0.8, count=40)  # the tape is warm: the pick itself is the news
    rows = list(service.journal.db.execute("SELECT text FROM episode_cards WHERE destination='movers'"))
    assert len(rows) == 1 and "Покупки 90%" in rows[0]["text"]


def test_hourly_history_survives_a_restart_through_the_journal():
    import sqlite3

    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE scalper_settings(name TEXT PRIMARY KEY, value TEXT)")
    journal = SimpleNamespace(db=db)
    first = UnifiedAttention(journal, slots=20, detail_slots=6)
    t0 = time.time() - 3500
    for i in range(0, 3301, 60):
        first.choose([asset("SOL", 100 * (1 + 0.03 * i / 3600), 60e6, 0.04, t0 + i)], t0 + i)
    assert len(first.movers.history["SOL"]) == 56  # one sample per 30s at most
    # Restart: a new process restores the samples and sees the hourly move on its first pass.
    second = UnifiedAttention(journal, slots=20, detail_slots=6)
    now = time.time()
    second.choose([asset("SOL", 103.2, 60e6, 0.04, now)], now)
    assert second.plan["detail"] == ["SOL"] and second.movers.picked["SOL"]["change_1h"] > 0.02


def test_restore_drops_samples_older_than_the_history_window():
    selector = MoversSelector()
    selector.restore({"SOL": [[1_000.0, 100.0], [5_000.0, 101.0]], "BAD": [[4_900.0, -1]]}, now=5_000.0)
    assert list(selector.history["SOL"]) == [(5_000.0, 101.0)] and "BAD" not in selector.history
