"""Measurements for the bot observatory: episodes, outcomes, controls, stats."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from extensions.scalper.bots.contracts import spec_by_id
from extensions.scalper.bots.observer import BotObservatory, choose_controls
from extensions.scalper.bots.report import bot_card
from extensions.scalper.bots.stats import benjamini_hochberg, status_for, status_for_bot
from extensions.scalper.bots.store import ensure_schema
from extensions.scalper.bots.triggers import flow_continuation
from extensions.scalper.telegram import bots_command


def make_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    ensure_schema(db)
    return db


def make_row(symbol="BTC", direction="BULLISH", rank=3, natr=1.0, price=100.0, window="5m", gate="ready"):
    now = 1_000.0
    return {
        "snapshot": {
            "symbol": symbol,
            "window": window,
            "captured_at": now,
            "metrics": {
                "price": {"value": price, "quality": "ok", "updated_at": now, "unit": "USD"},
                "ofi": {"value": 0.7, "quality": "ok", "updated_at": now, "unit": "ratio"},
                "natr": {"value": natr, "quality": "ok", "updated_at": now, "unit": "%"},
                "spread": {"value": 0.01, "quality": "ok", "updated_at": now, "unit": "USD"},
            },
        },
        "hyperdata": {
            "direction": direction,
            "native_signal": "STRONG_BULL" if direction == "BULLISH" else "STRONG_BEAR",
            "quality": "ok",
        },
        "strength": {"rank": rank, "reasons": ["test"]},
        "analysis_gate": gate,
    }


def market_rows(signal_direction="BULLISH", rank=3, natr=1.0):
    rows = {
        ("BTC", "5m"): make_row("BTC", signal_direction, rank, natr),
        ("ETH", "5m"): make_row("ETH", "NEUTRAL", 1, 1.1),  # selected control, must be excluded
        ("SOL", "5m"): make_row("SOL", "NEUTRAL", 1, 1.1),
        ("XRP", "5m"): make_row("XRP", "NEUTRAL", 1, 1.2),
        ("DOGE", "5m"): make_row("DOGE", "NEUTRAL", 1, 0.9),
        ("ADA", "5m"): make_row("ADA", "NEUTRAL", 1, 1.05),
        ("AVAX", "5m"): make_row("AVAX", "NEUTRAL", 1, 1.15),
    }
    return rows


def quotes(price_map, at):
    return {symbol: (price, at) for symbol, price in price_map.items()}


def test_flow_trigger_requires_fresh_ranked_5m_data():
    assert flow_continuation(make_row())["direction"] == 1
    assert flow_continuation(make_row(direction="BEARISH"))["direction"] == -1
    assert flow_continuation(make_row(rank=0)) is None
    assert flow_continuation(make_row(window="1m")) is None
    assert flow_continuation(make_row(gate="wide_spread")) is None
    broken = make_row()
    broken["hyperdata"]["quality"] = "stale"
    assert flow_continuation(broken) is None


def test_one_episode_one_observation_and_cooldown():
    db = make_db()
    spec = spec_by_id("flow.continuation")
    obs = BotObservatory(db, [spec], now=1_000.0)
    prices = quotes({"BTC": 100.0}, 1_001.2)
    plan = {"selected": ["ETH"]}

    obs.tick(1_000.0, market_rows(), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 1

    obs.tick(1_010.0, market_rows(), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 1

    obs.tick(1_400.0, market_rows(), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 1

    # An opposite direction is a new hypothesis, not a repeat.
    obs.tick(1_500.0, market_rows("BEARISH"), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 2

    # A neutral tick ends the episode after a short grace period; the same
    # direction then waits for cooldown.
    obs.tick(1_510.0, market_rows("NEUTRAL"), prices, plan)
    obs.tick(1_525.0, market_rows("NEUTRAL"), prices, plan)
    obs.tick(1_541.0, market_rows("NEUTRAL"), prices, plan)
    obs.tick(1_550.0, market_rows(), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 2
    obs.tick(1_860.0, market_rows(), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 3


@pytest.mark.asyncio
async def test_outcomes_and_stats_use_marks_and_controls():
    db = make_db()
    spec = spec_by_id("flow.continuation")
    obs = BotObservatory(db, [spec], now=1_000.0)
    plan = {"selected": ["ETH"]}
    control_prices = {"SOL": 10.0, "XRP": 10.0, "DOGE": 10.0, "ADA": 10.0, "AVAX": 10.0}

    obs.tick(1_000.0, market_rows(), quotes({"BTC": 100.0, **control_prices}, 1_001.2), plan)
    obs.tick(1_002.0, market_rows(), quotes({"BTC": 100.0, **control_prices}, 1_001.2), plan)
    obs.tick(1_062.0, market_rows(), quotes({"BTC": 101.0, **control_prices}, 1_061.2), plan)
    await obs.settle(1_063.0)
    obs.tick(1_302.0, market_rows(), quotes({"BTC": 102.0, **control_prices}, 1_301.2), plan)
    await obs.settle(1_303.0)
    obs.tick(1_902.0, market_rows(), quotes({"BTC": 99.0, **control_prices}, 1_901.2), plan)
    await obs.settle(1_903.0)

    rows = db.execute(
        "SELECT horizon_s,status,move_bps,exit_source FROM bot_outcomes ORDER BY horizon_s"
    ).fetchall()
    assert [(row["horizon_s"], row["status"]) for row in rows] == [(60, "scored"), (300, "scored"), (900, "scored")]
    assert round(rows[0]["move_bps"], 1) == 100.0
    assert round(rows[1]["move_bps"], 1) == 200.0
    assert round(rows[2]["move_bps"], 1) == -100.0
    assert all(row["exit_source"] == "mark" for row in rows)

    stats = obs.report("flow.continuation")["bot"]
    assert stats["observations"] == 1
    assert stats["scored"] == 1
    assert stats["coverage"] == 1.0
    assert stats["primary"]["kind"] == "rank_difference"
    assert round(stats["primary"]["strong_mean_bps"], 1) == 200.0
    assert stats["primary"]["n_strong"] == 1
    assert stats["primary"]["n_weak"] == 0
    assert stats["horizons"]["300"]["control_scored"] >= 1
    assert stats["horizons"]["300"]["excess"]["mean_bps"] is not None
    assert "flow.continuation" in bot_card(stats)


@pytest.mark.asyncio
async def test_restart_does_not_duplicate_an_episode():
    db = make_db()
    spec = spec_by_id("flow.continuation")
    plan = {"selected": ["ETH"]}
    prices = quotes({"BTC": 100.0}, 1_001.2)

    first = BotObservatory(db, [spec], now=1_000.0)
    first.tick(1_000.0, market_rows(), prices, plan)
    second = BotObservatory(db, [spec], now=1_001.0)
    second.tick(1_000.0, market_rows(), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_candle_close_is_the_documented_fallback():
    db = make_db()
    spec = spec_by_id("flow.continuation")
    obs = BotObservatory(db, [spec], now=1_000.0)
    plan = {"selected": ["ETH"]}
    obs.tick(1_000.0, market_rows(), quotes({"BTC": 100.0}, 1_001.2), plan)
    obs.tick(1_002.0, market_rows(), quotes({"BTC": 100.0}, 1_001.2), plan)

    async def fetch(symbol, window, bars=50):
        assert symbol == "BTC"
        assert window == "5m"
        return [{"timestamp": 1_300_000, "close": 110.0}]

    hub = SimpleNamespace(market=SimpleNamespace(get_candles=fetch))
    await obs.settle(1_601.0, hub)
    row = db.execute(
        "SELECT status,exit_source,exit_price FROM bot_outcomes WHERE horizon_s=300"
    ).fetchone()
    assert row["status"] == "scored"
    assert row["exit_source"] == "candle_close"
    assert row["exit_price"] == 110.0


def test_controls_are_deterministic_and_exclude_signal_and_selected():
    rows = market_rows()
    first = choose_controls(rows, "5m", "BTC", {"ETH"}, 1.0, seed="fixed")
    second = choose_controls(rows, "5m", "BTC", {"ETH"}, 1.0, seed="fixed")
    assert first == second
    assert "BTC" not in first[0]
    assert "ETH" not in first[0]
    assert len(first[0]) == 5


def test_contract_hash_ignores_notes_but_tracks_semantics():
    spec = spec_by_id("flow.continuation")
    assert replace(spec, note="a different note").contract_hash == spec.contract_hash
    assert replace(spec, cooldown_s=spec.cooldown_s + 1).contract_hash != spec.contract_hash


def test_report_status_thresholds():
    assert status_for(0) == "нет наблюдений"
    assert status_for(29) == "мало данных"
    assert status_for(30) == "предварительно"
    assert status_for(99) == "предварительно"
    assert status_for(100) == "достаточно данных"
    assert status_for_bot(3, 0) == "ожидает исходов"
    assert status_for_bot(0, 0) == "нет наблюдений"


def test_direction_flip_inside_min_episode_is_ignored():
    db = make_db()
    spec = spec_by_id("flow.continuation")
    obs = BotObservatory(db, [spec], now=1_000.0)
    prices = quotes({"BTC": 100.0}, 1_001.2)
    plan = {"selected": ["ETH"]}
    obs.tick(1_000.0, market_rows(), prices, plan)
    obs.tick(1_005.0, market_rows("BEARISH"), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 1
    obs.tick(1_070.0, market_rows("BEARISH"), prices, plan)
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 2


def test_replay_rows_are_not_mixed_into_live_statistics():
    db = make_db()
    spec = spec_by_id("flow.continuation")
    obs = BotObservatory(db, [spec], now=1_000.0)
    obs.tick(1_000.0, market_rows(), quotes({"BTC": 100.0}, 1_001.2), {"selected": ["ETH"]})
    with db:
        db.execute("UPDATE bot_observations SET source='replay'")
    report = obs.report("flow.continuation")["bot"]
    assert report["observations"] == 0
    assert report["status"] == "нет наблюдений"


def test_summary_marks_declared_but_unimplemented_bots():
    obs = BotObservatory(make_db(), now=1_000.0)
    payload = obs.report()
    by_id = {bot["id"]: bot for bot in payload["bots"]}
    assert by_id["flow.continuation"]["implemented"] is True
    assert by_id["attention.relative_move"]["implemented"] is False
    assert "ожидает реализации" in payload["text"]


def test_benjamini_hochberg_marks_only_the_small_p_values():
    rejected, adjusted = benjamini_hochberg([0.001, 0.04, 0.2, 0.8], q=0.1)
    assert rejected == [True, True, False, False]
    assert adjusted == sorted(adjusted)


def test_telegram_bot_commands_are_strict():
    now = time.time()
    def update(text):
        return {"message": {"chat": {"id": 123}, "date": int(now), "text": text}}

    assert bots_command(update("/bots"), "123", "screener_bot", now) == ("summary", None)
    assert bots_command(update("/bots@screener_bot"), "123", "screener_bot", now) == ("summary", None)
    assert bots_command(update("/bot flow.continuation"), "123", "screener_bot", now) == (
        "bot",
        "flow.continuation",
    )
    assert bots_command(update("/bot flow.continuation@other"), "123", "screener_bot", now) is None
    assert bots_command(update("/bots@other"), "123", "screener_bot", now) is None
    assert bots_command(update("/bots"), "999", "screener_bot", now) is None
    stale = update("/bots")
    stale["message"]["date"] = int(now) - 500
    assert bots_command(stale, "123", "screener_bot", now) is None


def _entry(db):
    return db.execute(
        "SELECT entry_price,entry_at,status,unscored_reason FROM bot_observations"
    ).fetchone()


def test_entry_mark_survives_a_market_refresh_slower_than_five_seconds():
    """The native market snapshot refreshes every ~5.8s.

    Decision at 1000 → entry due at 1001. The previous refresh (999.5) is too
    early and the next one (1007.0) fell outside the old 5s window, which left
    the observation unscored with no_entry_mark.
    """
    db = make_db()
    obs = BotObservatory(db, [spec_by_id("flow.continuation")], now=1_000.0)
    plan = {"selected": ["ETH"]}
    obs.tick(1_000.0, market_rows(), quotes({"BTC": 100.0}, 999.5), plan)
    obs.tick(1_005.0, market_rows(), quotes({"BTC": 100.0}, 999.5), plan)
    obs.settle_mark_only(1_005.0)
    obs.tick(1_010.0, market_rows(), quotes({"BTC": 101.0}, 1_007.0), plan)
    obs.settle_mark_only(1_010.0)
    assert tuple(_entry(db)) == (101.0, 1_007.0, "observed", None)


def test_in_window_mark_seen_by_a_late_scan_is_still_captured():
    db = make_db()
    obs = BotObservatory(db, [spec_by_id("flow.continuation")], now=1_000.0)
    plan = {"selected": ["ETH"]}
    obs.tick(1_000.0, market_rows(), {}, plan)
    obs.settle_mark_only(1_015.0)  # due 1001: window ends 1011, scan lag allows until 1021
    obs.tick(1_018.0, market_rows(), quotes({"BTC": 100.0}, 1_010.5), plan)
    assert tuple(_entry(db))[:3] == (100.0, 1_010.5, "observed")


def test_entry_mark_after_the_window_is_not_used():
    db = make_db()
    obs = BotObservatory(db, [spec_by_id("flow.continuation")], now=1_000.0)
    plan = {"selected": ["ETH"]}
    obs.tick(1_000.0, market_rows(), {}, plan)
    obs.tick(1_013.0, market_rows(), quotes({"BTC": 100.0}, 1_012.0), plan)  # due+11s
    obs.settle_mark_only(1_025.0)
    assert tuple(_entry(db)) == (None, None, "unscored", "no_entry_mark")


def test_unified_plan_controls_come_from_overview_rows_not_detail():
    """Unified runtime: every scan row is in plan.selected.

    Excluding the whole overview left every control set empty. Only the
    detail set (live tape) is excluded; overview-only rows are valid controls.
    """
    db = make_db()
    obs = BotObservatory(db, [spec_by_id("flow.continuation")], now=1_000.0)
    rows = market_rows()
    symbols = [symbol for symbol, _ in rows]
    plan = {"selected": symbols, "detail": ["BTC", "ETH"]}
    obs.tick(1_000.0, rows, quotes({s: 10.0 for s in symbols}, 1_001.2), plan)

    chosen = json.loads(db.execute("SELECT symbols FROM bot_control_sets").fetchone()[0])
    assert sorted(chosen) == ["ADA", "AVAX", "DOGE", "SOL", "XRP"]
    points = db.execute("SELECT COUNT(*) FROM bot_control_points").fetchone()[0]
    assert points == 5 * 3
    trigger = json.loads(db.execute("SELECT trigger_json FROM bot_observations").fetchone()[0])
    assert trigger["selected"] is True
