"""Stage 4 observers fed by events: tape readings, level approaches, liquidation cascades."""

import json
import sqlite3
import time
from types import SimpleNamespace

import pytest

from extensions.scalper.bots import BotObservatory
from extensions.scalper.bots.contracts import spec_by_id
from extensions.scalper.bots.triggers import level_break, level_hold, liquidation_cascade, tape_reading
from extensions.scalper.liquidation_context import LiquidationContext
from extensions.scalper.service import Screener


def reading(kind, side=1, prior=40.0, recent=-10.0):
    return dict(kind=kind, side=side, prior=dict(move_bps=prior), recent=dict(move_bps=recent))


@pytest.mark.parametrize(
    "kind, side, implied",
    [("counter_aggression", 1, -1), ("absorption", 1, -1), ("exhaustion", -1, 1), ("thin_move", 1, 1),
     ("continuation", -1, -1)],
)
def test_tape_reading_implies_a_direction(kind, side, implied):
    assert tape_reading(reading(kind, side), 10.0, None)["direction"] == implied


def test_tape_strength_is_impulse_against_the_minute_range_and_slowdown_is_not_a_signal():
    assert tape_reading(reading("absorption", prior=30), 1.0, natr=0.3)["strength"] == 4  # 1 NATR
    assert tape_reading(reading("absorption", prior=10), 1.0, natr=0.3)["strength"] == 2  # 0.33 NATR
    assert tape_reading(reading("absorption", prior=25), 1.0, natr=None)["strength"] == 3  # bps fallback
    assert tape_reading(reading("slowdown"), 1.0, None) is None
    assert tape_reading(None, 1.0, None) is None


def test_level_hold_and_break_read_one_approach_both_ways():
    bid = dict(side="bid", price=99.8, relative_size=25)
    assert level_hold(bid, 99.85, None)["direction"] == 1 and level_break(bid, 99.85, None)["direction"] == -1
    assert level_hold(bid, 99.85, None)["strength"] == 3
    ask = dict(side="ask", price=100.2, relative_size=8)
    assert level_hold(ask, 100.1, None)["direction"] == -1 and level_hold(ask, 100.1, None)["strength"] == 1


def test_cascade_is_sized_against_the_instruments_own_minute():
    # $1.44B a day = $1M an average minute: $600k of forced longs is 0.6 of a minute.
    cont = liquidation_cascade(600_000, 0, 1.44e9, 100.0)
    assert cont["direction"] == -1 and cont["strength"] == 1 and cont["minute_share"] == pytest.approx(0.6)
    assert liquidation_cascade(600_000, 0, 1.44e9, 100.0, exhaustion=True)["direction"] == 1
    assert liquidation_cascade(400_000, 0, 1.44e9, 100.0) is None  # under half a minute for BTC-sized volume
    assert liquidation_cascade(60_000, 0, 14.4e6, 1.0)["strength"] == 3  # 6 minutes of a small coin
    assert liquidation_cascade(40_000, 0, 1e6, 1.0) is None  # under the $50k floor


def observatory(bot_ids, now):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db, BotObservatory(db, [spec_by_id(b) for b in bot_ids], now=now)


def test_recorded_event_opens_one_episode_with_scout_controls():
    now = 1_000.0
    db, obs = observatory(["tape.reading"], now)
    controls = {s: (100.0, now) for s in ("A", "B", "C", "D", "E", "F")}
    obs.tick(now, {}, {"BTC": (100.0, now), **controls}, {"selected": [], "detail": ["BTC"]},
             {s: 1.0 for s in ("BTC", *controls)})
    event = tape_reading(reading("absorption"), 100.0, 0.3)
    assert obs.record("tape.reading", "BTC", event, now + 1)
    assert obs.record("tape.reading", "BTC", event, now + 6)  # same reading 5s later: same episode
    rows = db.execute("SELECT direction, strength, trigger_json FROM bot_observations").fetchall()
    assert len(rows) == 1 and rows[0]["direction"] == -1 and json.loads(rows[0]["trigger_json"])["kind"] == "absorption"
    symbols = json.loads(db.execute("SELECT symbols FROM bot_control_sets").fetchone()[0])
    assert len(symbols) == 5 and "BTC" not in symbols
    assert not obs.record("whale.follow", "BTC", event, now + 7)  # not an active bot here


def test_attention_entries_are_detail_slots_after_a_quiet_spell():
    now = 1_000.0
    db, obs = observatory(["attention.relative_move"], now)
    universe = ("BTC", "ETH", "SOL", "XRP", "A", "B", "C", "D", "E", "F")
    prices = {s: (100.0, now) for s in universe}
    vol = {s: 1.0 for s in universe}
    obs.tick(now, {}, prices, {"selected": ["BTC", "XRP"], "detail": ["XRP"]}, vol)  # baseline
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 0
    rows = {("SOL", "5m"): {"strength": {"rank": 3}}}
    plan = {"selected": ["SOL", "ETH", "XRP", "BTC"], "detail": ["SOL", "ETH", "XRP"], "movers": [{"symbol": "ETH"}]}
    obs.tick(now + 5, rows, prices, plan, vol)
    obs.tick(now + 10, rows, prices, plan, vol)  # still in detail: no new entry
    got = {r["symbol"]: (r["strength"], json.loads(r["trigger_json"])["slot"])
           for r in db.execute("SELECT symbol, strength, trigger_json FROM bot_observations")}
    # BTC is only in the rotating scout tail; XRP held its slot since the baseline.
    assert got == {"ETH": (3, "mover"), "SOL": (4, "detail")}
    controls = json.loads(db.execute("SELECT symbols FROM bot_control_sets").fetchone()[0])
    assert not {"BTC", "ETH", "SOL", "XRP"} & set(controls)  # controls come from outside the plan
    obs.tick(now + 300, rows, prices, {"selected": [], "detail": []}, vol)
    obs.tick(now + 400, rows, prices, {"selected": ["ETH"], "detail": ["ETH"]}, vol)  # back after 6 min
    assert db.execute("SELECT COUNT(*) FROM bot_observations").fetchone()[0] == 2


def whale(symbol="ETH", side="long", usd=1_000_000, price=100.0, entry=100.0, scanned=1.0, address="0xa"):
    return dict(address=address, symbol=symbol, side=side, size_usd=usd, current_price=price, entry_price=entry,
                scanned_at=scanned, leverage=5)


def test_whale_watcher_sees_opened_and_added_positions_but_not_old_ones():
    from extensions.scalper.whales import WhaleWatcher

    w = WhaleWatcher()
    assert w.ingest([whale()], 10) == []  # baseline
    events = w.ingest([
        whale(usd=1_200_000, price=120.0, scanned=20),  # same coins at a higher price: not an addition
        whale(symbol="BTC", entry=100.5, scanned=20),  # new on a re-read address, fresh entry: opened
        whale(symbol="SOL", entry=80.0, scanned=20),  # new to us but held for long: not opened
        whale(symbol="DOGE", address="0xb", scanned=20),  # first read of this address: its baseline
    ], 30)
    assert [(e["symbol"], e["kind"]) for e in events] == [("BTC", "opened")]
    events = w.ingest([whale(usd=1_800_000, price=120.0, scanned=40)], 50)  # 15k coins vs 10k: added
    assert events[0]["kind"] == "added" and events[0]["added_usd"] == pytest.approx(600_000)
    assert ("0xa", "BTC", "long") not in w.known  # its address was re-read without it: closed
    assert len(w.drain()) == 2 and w.drain() == []


def test_whale_position_is_sized_against_the_coins_own_minute():
    from extensions.scalper.bots.triggers import whale_position

    event = dict(kind="opened", side="short", added_usd=600_000, size_usd=600_000)
    assert whale_position(event, 1.44e9, 100.0) is None  # 0.6 of an average BTC-sized minute
    got = whale_position(dict(event, added_usd=2_000_000), 1.44e9, 100.0)  # 2 minutes of BTC
    assert got["direction"] == -1 and got["strength"] == 1
    assert whale_position(event, 14.4e6, 1.0)["strength"] == 4  # 60 minutes of a small coin
    assert whale_position(dict(event, added_usd=200_000), 1e6, 1.0) is None  # under the $250k floor


def test_service_sizes_whales_against_both_venues_and_prices_on_binance():
    now = time.time()
    db, obs = observatory(["whale.follow"], now)
    universe = {"BINANCE:AAVE": dict(symbol="BINANCE:AAVE", price=130.0, volume_24h=40e6),
                "AAVE": dict(symbol="AAVE", price=130.1, volume_24h=3.2e6)}
    whales = SimpleNamespace(drain=lambda: [dict(kind="opened", side="long", symbol="AAVE", added_usd=500_000)])
    service = SimpleNamespace(bots=obs, whales=whales, hub=SimpleNamespace(universe=universe))
    Screener.observe_whales(service, now)
    row = db.execute("SELECT symbol, strength, trigger_json FROM bot_observations").fetchone()
    assert row["symbol"] == "BINANCE:AAVE" and row["strength"] == 3  # $500k = 16.7 minutes of $43.2M a day
    assert json.loads(row["trigger_json"])["minute_share"] == pytest.approx(500_000 / (43.2e6 / 1440))


def test_service_feeds_both_liquidation_observers_from_confirmed_cascades():
    now = time.time()
    db, obs = observatory(["liquidation.continuation", "liquidation.exhaustion"], now)
    context = LiquidationContext()
    context.ingest([dict(timestamp=now - 5, symbol="ETH", side="long", size_usd=900_000, exchange="binance",
                         confirmed=True)], now)
    market = SimpleNamespace(assets={"BINANCE:ETH": SimpleNamespace(symbol="BINANCE:ETH", mark_price=3000.0,
                                                                    price=3000.0, volume_24h=1.44e9)})
    service = SimpleNamespace(bots=obs, liquidations=context, hub=SimpleNamespace(market=market))
    Screener.observe_liquidations(service, now)
    rows = {r["bot_id"]: r["direction"] for r in db.execute("SELECT bot_id, direction FROM bot_observations")}
    assert rows == {"liquidation.continuation": -1, "liquidation.exhaustion": 1}
