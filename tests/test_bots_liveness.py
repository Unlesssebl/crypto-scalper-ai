"""End-to-end liveness checks of the bot observation pipeline."""

from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest

from extensions.scalper.bots.liveness import LivenessPolicy, PipelineLiveness, evaluate, measure_clock_offset
from extensions.scalper.bots.store import ensure_schema

POLICY = LivenessPolicy()
H = 3600.0


def make_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    ensure_schema(db)
    return db


_seq = iter(range(1_000_000))


def add_observation(db, decided_at, *, reason=None, controls="[\"A\",\"B\"]", source="live"):
    key = f"obs{next(_seq)}"
    status = "unscored" if reason else "observed"
    with db:
        db.execute(
            "INSERT INTO bot_observations(id,bot_id,version,trigger_event_id,control_set_id,symbol,window,mode,"
            "hypothesis,strength,direction,decided_at,entry_due_at,trigger_json,status,unscored_reason,source) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (key, "flow.continuation", 2, key, "c" + key, "SYM", "5m", "directional", "h", 1, 1,
             decided_at, decided_at + 1, "{}", status, reason, source),
        )
        db.execute(
            "INSERT INTO bot_control_sets(id,trigger_event_id,direction,mode,symbols,bucket,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            ("c" + key, key, 1, "directional", controls, "", decided_at),
        )


def test_healthy_pipeline_has_no_issues():
    db = make_db()
    now = 10 * H
    for i in range(12):
        add_observation(db, now - 600 - i * 300)
    assert evaluate(db, now, started_at=0, policy=POLICY, clock_offset_s=0.05) == {}


def test_silence_only_after_the_runtime_was_up_that_long():
    db = make_db()
    now = 10 * H
    assert evaluate(db, now, started_at=now - 1 * H, policy=POLICY) == {}
    issues = evaluate(db, now, started_at=now - 3 * H, policy=POLICY)
    assert "observations_silent" in issues and "ни одного" in issues["observations_silent"]
    add_observation(db, now - 2.5 * H)
    assert "observations_silent" in evaluate(db, now, started_at=0, policy=POLICY)
    add_observation(db, now - 0.5 * H)
    assert "observations_silent" not in evaluate(db, now, started_at=0, policy=POLICY)


def test_replay_observations_do_not_count_as_live():
    db = make_db()
    now = 10 * H
    add_observation(db, now - 60, source="replay")
    assert "observations_silent" in evaluate(db, now, started_at=0, policy=POLICY)


def test_missing_entry_marks_need_enough_settled_observations():
    db = make_db()
    now = 10 * H
    for i in range(9):
        add_observation(db, now - 600 - i * 60, reason="no_entry_mark")
    assert "entry_marks_missing" not in evaluate(db, now, started_at=0, policy=POLICY)  # n=9 < 10
    add_observation(db, now - 30, reason="no_entry_mark")  # not settled yet: ignored
    assert "entry_marks_missing" not in evaluate(db, now, started_at=0, policy=POLICY)
    add_observation(db, now - 1200)
    issues = evaluate(db, now, started_at=0, policy=POLICY)
    assert "9 из 10" in issues["entry_marks_missing"]


def test_entry_mark_share_below_threshold_is_fine():
    db = make_db()
    now = 10 * H
    for i in range(10):
        add_observation(db, now - 600 - i * 60, reason="no_entry_mark" if i < 2 else None)
    assert "entry_marks_missing" not in evaluate(db, now, started_at=0, policy=POLICY)


def test_consecutive_empty_control_sets_since_start():
    db = make_db()
    now = 10 * H
    add_observation(db, now - 900)
    for i in range(3):
        add_observation(db, now - 600 + i * 60, controls="[]")
    assert "controls_empty" in evaluate(db, now, started_at=0, policy=POLICY)
    # Sets from before this runtime started (an older code version) are ignored.
    assert "controls_empty" not in evaluate(db, now, started_at=now - 500, policy=POLICY)
    add_observation(db, now - 60)
    assert "controls_empty" not in evaluate(db, now, started_at=0, policy=POLICY)


@pytest.mark.parametrize("offset,expected", [(1.09, "отстают"), (-1.5, "спешат")])
def test_clock_skew_beyond_one_second(offset, expected):
    db = make_db()
    add_observation(db, 10 * H - 60)
    issues = evaluate(db, 10 * H, started_at=0, policy=POLICY, clock_offset_s=offset)
    assert expected in issues["clock_skew"]
    assert "clock_skew" not in evaluate(db, 10 * H, started_at=0, policy=POLICY, clock_offset_s=0.4)
    assert "clock_skew" not in evaluate(db, 10 * H, started_at=0, policy=POLICY, clock_offset_s=None)


class FakeAlerts:
    def __init__(self):
        self.sent = []

    async def send_telegram(self, message, **kwargs):
        self.sent.append(message)


@pytest.mark.asyncio
async def test_alert_is_debounced_repeated_and_recovered():
    db = make_db()
    add_observation(db, 0.0)
    alerts = FakeAlerts()
    # Only the clock issue is under test; the run spans more than silence_s.
    live = PipelineLiveness(db, alerts, started_at=0.0, policy=replace(POLICY, silence_s=1e9))

    assert await live.step(1_000.0, clock_offset_s=1.2) == []  # first sighting only
    assert live.report()["pending"] == ["clock_skew"]
    sent = await live.step(1_060.0)
    assert len(sent) == 1 and "отстают на 1.20" in sent[0]
    assert not live.report()["healthy"]
    assert await live.step(1_120.0) == []  # no repeat inside repeat_s
    assert await live.step(1_060.0 + POLICY.repeat_s - 1, clock_offset_s=1.2) == []
    repeated = await live.step(1_060.0 + POLICY.repeat_s + 1, clock_offset_s=1.2)
    assert repeated and repeated[0].startswith("⚠️ Всё ещё")
    recovered = await live.step(1_060.0 + POLICY.repeat_s + 61, clock_offset_s=0.01)
    assert recovered and recovered[0].startswith("✅") and "clock_skew" in recovered[0]
    assert live.report()["healthy"] and alerts.sent == sent + repeated + recovered


@pytest.mark.asyncio
async def test_one_off_issue_is_never_announced():
    db = make_db()
    add_observation(db, 0.0)
    live = PipelineLiveness(db, FakeAlerts(), started_at=0.0, policy=POLICY)
    assert await live.step(100.0, clock_offset_s=2.0) == []
    assert await live.step(160.0, clock_offset_s=0.0) == []
    assert live.report()["pending"] == [] and live.alerts.sent == []


@pytest.mark.asyncio
async def test_stale_clock_measurement_neither_raises_nor_keeps_an_alarm():
    db = make_db()
    add_observation(db, 0.0)
    live = PipelineLiveness(db, None, started_at=0.0, policy=POLICY)
    await live.step(100.0, clock_offset_s=2.0)
    await live.step(160.0)
    assert "clock_skew" in live.report()["active"]
    later = 100.0 + 3 * POLICY.clock_check_s + 1  # measurements kept failing since
    assert (await live.step(later))[0].startswith("✅")


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status = payload, status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(self.status)

    async def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = responses

    def get(self, url):
        return self.responses[url]


BINANCE, OKX = (url for _, url, _ in __import__("extensions.scalper.bots.liveness", fromlist=["x"]).CLOCK_SOURCES)


@pytest.mark.asyncio
async def test_clock_offset_uses_round_trip_midpoint(monkeypatch):
    stamps = iter([1_000.0, 1_000.2])
    monkeypatch.setattr("extensions.scalper.bots.liveness.time.time", lambda: next(stamps))
    offset, source = await measure_clock_offset(FakeSession({BINANCE: FakeResponse({"serverTime": 1_001_200})}))
    assert offset == pytest.approx(1.1) and source == "Binance"


@pytest.mark.asyncio
async def test_geoblocked_binance_falls_back_to_okx(monkeypatch):
    stamps = iter([1_000.0, 1_000.1, 1_000.2])  # Binance fails before its second read
    monkeypatch.setattr("extensions.scalper.bots.liveness.time.time", lambda: next(stamps))
    session = FakeSession({BINANCE: FakeResponse({}, status=451),
                           OKX: FakeResponse({"code": "0", "data": [{"ts": "1000800"}]})})
    offset, source = await measure_clock_offset(session)
    assert offset == pytest.approx(0.65) and source == "OKX"


@pytest.mark.asyncio
async def test_slow_or_bad_clock_responses_are_discarded(monkeypatch):
    stamps = iter([1_000.0, 1_002.0, 1_003.0, 1_003.1])
    monkeypatch.setattr("extensions.scalper.bots.liveness.time.time", lambda: next(stamps))
    session = FakeSession({BINANCE: FakeResponse({"serverTime": 1_001_000}),  # 2s round trip
                           OKX: FakeResponse({"data": [{"ts": "bad"}]})})
    assert await measure_clock_offset(session) is None
