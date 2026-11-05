"""Opportunity admission, shared spending, dispatch and full-population evaluation."""
import asyncio
import time
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp.test_utils import TestClient, TestServer

from apps.screener_web.__main__ import build_app
from extensions.scalper.analysis import snapshot_key
from extensions.scalper.journal import Journal
from extensions.scalper.market import metric
from extensions.scalper.opportunity import OpportunityPolicy
from extensions.scalper.quality import calculate_quality
from extensions.scalper.service import Screener, alert_text
from extensions.scalper.telegram import analyze_command


def snapshot(now, symbol="BTC", window="1m", **values):
    defaults = dict(price=100, ofi=0.8, cvd=500, spread=0.01, imbalance=0.4, activity_ratio=2.5,
                    trade_count=30, natr=0.2, range_ratio=1, first_trade_price=100, last_trade_price=100)
    defaults.update(values)
    return dict(symbol=symbol, window=window, contract=symbol + "-PERP", exchange="hyperliquid",
                captured_at=now, metrics={k: metric(v, "test", now, "ratio") for k, v in defaults.items()})


def vote(snap, direction="BULLISH"):
    return dict(direction=direction, native_signal="STRONG_BULL" if direction == "BULLISH" else "STRONG_BEAR",
                source="test", evidence=["ofi"], snapshot=snapshot_key(snap))


def row(snap):
    return dict(snapshot=snap, hyperdata=vote(snap), strength={"rank": 2, "label": "Заметная", "icon": "!"},
                facts=[], movement_phase=None, btc_context=None, analysis_gate="ready", context_5m="unavailable",
                other=None)


@pytest.fixture
def journal(tmp_path):
    obj = Journal(tmp_path / "events.sqlite")
    yield obj
    obj.close()


@pytest.mark.parametrize("window", ["1m", "5m"])
def test_early_requires_independent_family(window):
    policy = OpportunityPolicy()
    snap = snapshot(100, window=window, imbalance=0)
    assert policy.classify(snap, vote(snap), 100) == (None, "no_independent_confirmation")
    snap["metrics"]["imbalance"]["value"] = 0.4
    assert policy.classify(snap, vote(snap), 100)[0] == "early"
    snap["metrics"]["imbalance"]["value"] = 0
    snap["metrics"]["last_trade_price"]["value"] = 100.03
    assert policy.classify(snap, vote(snap), 100)[0] == "early"
    assert OpportunityPolicy(mode="economical").classify(snap, vote(snap), 100)[0] is None
    assert OpportunityPolicy(early=False).classify(snap, vote(snap), 100)[0] is None
    snap["metrics"]["last_trade_price"]["value"] = 100.3
    assert OpportunityPolicy(mode="economical").classify(snap, vote(snap), 100)[0] == "confirmed"


@pytest.mark.parametrize("change,reason", [
    ({"ofi": 0.3}, "ordinary_flow"), ({"activity_ratio": 1.2}, "ordinary_flow"),
    ({"imbalance": -0.5}, "no_independent_confirmation"), ({"spread": 0.2}, "wide_spread"),
])
def test_noise_and_quality(change, reason):
    snap = snapshot(100, **change)
    assert OpportunityPolicy().classify(snap, vote(snap), 100) == (None, reason)


def test_stale_and_neutral():
    snap = snapshot(100)
    assert OpportunityPolicy().classify(snap, vote(snap), 111)[1] == "stale_snapshot"
    snap["metrics"]["spread"]["updated_at"] = 70
    assert OpportunityPolicy().classify(snap, vote(snap), 100)[1] == "stale_data"
    snap["metrics"]["spread"]["updated_at"] = 100
    assert OpportunityPolicy().classify(snap, vote(snap, "NEUTRAL"), 100)[1] == "neutral"


def test_persistent_budget_and_cross_window_cooldown(journal):
    policy = OpportunityPolicy(hourly_limit=2)
    assert journal.reserve_request("one", "BTC", 100, policy) is None
    assert journal.reserve_request("two", "BTC", 101, policy) == "cooldown"
    assert journal.reserve_request("control", "BTC", 101, policy, experiment=True) is None
    assert journal.reserve_request("eth", "ETH", 102, policy) == "hourly_limit"
    restored = Screener(None, journal, SimpleNamespace(key="yes"), False)
    assert journal.request_block("BTC", 200, restored.policy) == "cooldown"
    assert journal.reserve_request("eth", "ETH", 3702, policy) is None


def test_dedup_and_all_rejections_have_forward_observations(journal):
    service = Screener(SimpleNamespace(), journal, SimpleNamespace(key="key"), False)
    for frame in ("1m", "5m"):
        snap = snapshot(100, window=frame)
        service.detect(("BTC", frame), snap, vote(snap), 100, "ready", "neutral")
    assert service.jobs.qsize() == 1
    assert journal.db.execute("SELECT reason FROM ai_selection WHERE reason='episode_pending'").fetchone()
    service.detect(("BTC", "1m"), snapshot(105), vote(snapshot(105)), 105, "ready", "neutral")
    assert journal.db.execute("SELECT COUNT(*) FROM finding_candidates").fetchone()[0] == 2
    assert journal.db.execute("SELECT COUNT(*) FROM candidate_observations").fetchone()[0] == 6
    for at in (160, 400, 1000):
        journal.observe(at, {"BTC": (101, at)})
    result = calculate_quality(Path(journal.db.execute("PRAGMA database_list").fetchone()[2]),
                               1001, ("1m", "5m"))["selection"]
    assert result["detected"] == 2 and result["admitted"] == 1 and result["requests"] == 0
    assert result["outcomes"]["15"]["rejected"]["correct"] == 1
    assert result["movements"]["15"]["rejected"]["natr_hits"] == 1


def test_without_key_no_ai_but_events_retained(journal):
    service = Screener(SimpleNamespace(), journal, SimpleNamespace(key=""), False)
    snap = snapshot(100)
    service.detect(("BTC", "1m"), snap, vote(snap), 100, "ready", "neutral")
    assert service.jobs.empty()
    assert journal.ai_reason(journal.latest("BTC", "1m")[0]["id"]) == "unconfigured"
    assert journal.db.execute("SELECT COUNT(*) FROM candidate_observations").fetchone()[0] == 3


async def run_jobs(service):
    task = asyncio.create_task(service.analyze())
    try:
        await asyncio.wait_for(service.jobs.join(), 1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("failure", ["expired", "budget", "error", "ok", "lost_confirmation"])
async def test_dispatch_rechecks_and_charges_once(journal, monkeypatch, failure):
    now = time.time()
    snap = snapshot(now)
    calls = []

    async def analyze(data):
        calls.append(data)
        if failure == "error":
            raise RuntimeError("provider unavailable")
        return dict(status="ok", snapshot=snapshot_key(data), direction="BULLISH",
                    summary="Поток", scenario="", arguments=[{"metric": "ofi"}])

    service = Screener(SimpleNamespace(), journal, SimpleNamespace(key="key", analyze=analyze), False)
    service.rows["BTC", "1m"] = row(snap)
    service.detect(("BTC", "1m"), snap, vote(snap), now, "ready", "neutral")
    if failure == "expired":
        monkeypatch.setattr("extensions.scalper.service.time.time", lambda: now + 46)
    elif failure == "budget":
        service.policy = OpportunityPolicy(hourly_limit=0)
    elif failure == "lost_confirmation":
        snap["metrics"]["imbalance"]["value"] = 0
    await run_jobs(service)
    expected = int(failure in {"ok", "error"})
    assert len(calls) == expected
    assert journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == expected
    ai = journal.latest("BTC", "1m")[0]["ai"]
    assert ai["status"] == (failure if failure in {"ok", "error"} else "skipped")
    if failure == "error":
        assert ai["reason"] == "error"


@pytest.mark.parametrize("reason", ["hourly_limit", "unconfigured", "event_expired", "error", "queue_full"])
def test_telegram_does_not_promise_unadmitted_analysis(reason):
    snap = snapshot(100)
    text = alert_text(snap, vote(snap), {"status": "skipped", "reason": reason}, {})
    assert "анализ выполняется" not in text and "пока не готова" not in text
    assert "AI</b> пропущен" in text


def test_manual_parser_and_shared_limits(journal):
    now = time.time()
    command = {"message": {"date": int(now), "chat": {"id": 1}, "text": "/analyze@Bot BTC 1m"}}
    assert analyze_command(command, 1, "Bot", now) == ("BTC", "1m")
    assert analyze_command(command, 2, "Bot", now) is None
    assert analyze_command(command, 1, "AnotherBot", now) is None
    assert analyze_command(command, 1, "Bot", now + 121) is None
    service = Screener(SimpleNamespace(demo=False), journal, SimpleNamespace(key="key"), False)
    service.rows["BTC", "1m"] = row(snapshot(now, ofi=0.2))
    assert "принят" in service.request_analysis("btc", "1m", now)
    assert "пропущен" in service.request_analysis("BTC", "1m", now)
    assert "отсутствует" in service.request_analysis("UNKNOWN", "1m", now)


async def test_policy_api_persists_and_validates(journal):
    service = Screener(SimpleNamespace(), journal, SimpleNamespace(key="key"), False)
    client = TestClient(TestServer(build_app(service.hub, service)))
    await client.start_server()
    try:
        policy = asdict(OpportunityPolicy(mode="economical", hourly_limit=3))
        response = await client.post("/v1/scalper/policy", json=policy)
        assert response.status == 200
        assert Screener(None, journal, None, False).policy.hourly_limit == 3
        response = await client.post("/v1/scalper/policy", json={**policy, "hourly_limit": -1})
        assert response.status == 400
        response = await client.post("/v1/scalper/policy", json=policy, headers={"Origin": "https://other.test"})
        assert response.status == 403
    finally:
        await client.close()


@pytest.mark.parametrize("enabled,limit,requests", [(False, 12, 1), (True, 1, 1), (True, 2, 2)])
async def test_experiments_share_hard_budget(journal, monkeypatch, enabled, limit, requests):
    monkeypatch.setattr("extensions.scalper.service.AI_INTERVAL", 0)
    now = time.time()
    snap = snapshot(now)
    calls = []

    async def analyze(data):
        calls.append(data)
        return dict(status="ok", snapshot=snapshot_key(data), direction="BULLISH", arguments=[])

    service = Screener(SimpleNamespace(), journal, SimpleNamespace(key="key", analyze=analyze), False)
    service.policy = OpportunityPolicy(experiments=enabled, hourly_limit=limit)
    for window in ("1m", "5m"):
        service.rows["BTC", window] = row(snapshot(now, window=window))
    service.candles["BTC", "15m"] = dict(closed_at=now - 10, return_4=0.1, range_4=0.2,
                                         range_position_4=0.5, natr=0.2)
    service.detect(("BTC", "1m"), snap, vote(snap), now, "ready", "neutral")
    await run_jobs(service)
    assert len(calls) == requests
    assert journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == requests


def test_early_queue_precedes_confirmed_and_does_not_starve_older(journal):
    service = Screener(SimpleNamespace(), journal, SimpleNamespace(key="key"), False)
    for symbol, at, change in (("OLD", 50, 100.3), ("CONFIRMED", 100, 100.3), ("EARLY", 100, 100)):
        snap = snapshot(at, symbol=symbol, last_trade_price=change)
        service.detect((symbol, "1m"), snap, vote(snap), at, "ready", "neutral")
    assert [service.jobs.get_nowait()[3][0] for _ in range(3)] == ["OLD", "EARLY", "CONFIRMED"]


def test_restart_preserves_spending_and_ends_pending_telegram_state(tmp_path):
    path = tmp_path / "restart.sqlite"
    journal = Journal(path)
    snap = snapshot(100)
    event = dict(type="manual", direction="BULLISH")
    journal.record_candidate("a", snap, event, "queued", 100)
    journal.register_fact_message("a", snap, event, vote(snap))
    journal.fact_message_sent("a", 42, False)
    journal.delivery("a", "sent", 100)
    journal.select_ai("a", 100, "early", "queued")
    journal.select_ai("a", 101, "early", "analyzing")
    journal.reserve_request("a", "BTC", 101, OpportunityPolicy())
    journal.close()
    journal = Journal(path)
    try:
        assert journal.recover() == ["a"]
        assert journal.fact_message("a")["ai"]["reason"] == "interrupted"
        assert journal.request_block("BTC", 102, OpportunityPolicy()) == "cooldown"
        service = Screener(None, journal, None, True)
        service.queue_hypothesis("a")
        assert service.notifications.get_nowait() == ("edit", "a")
    finally:
        journal.close()


@pytest.mark.parametrize("failure", ["unconfigured", "hourly_limit", "error"])
async def test_telegram_delivery_and_edits_follow_real_admission(journal, failure):
    now = time.time()
    snap = snapshot(now)
    sent = asyncio.Event()
    messages = []

    async def send(text, **kwargs):
        messages.append(text)
        sent.set()
        return 42, False

    async def edit(message_id, text, **kwargs):
        assert message_id == 42
        messages.append(text)
        return True

    async def analyze(data):
        await sent.wait()
        raise RuntimeError("provider failed")

    hub = SimpleNamespace(alerts=SimpleNamespace(send_telegram=send, edit_telegram=edit))
    model = SimpleNamespace(key="" if failure == "unconfigured" else "key", analyze=analyze)
    service = Screener(hub, journal, model, True)
    if failure == "hourly_limit":
        service.policy = OpportunityPolicy(hourly_limit=0)
    current = row(snap)
    service.rows["BTC", "1m"] = current
    service.detect(("BTC", "1m"), snap, current["hyperdata"], now, "ready", "neutral", current)
    workers = [asyncio.create_task(worker()) for worker in (service.analyze, service.deliver)]
    try:
        await asyncio.wait_for(service.jobs.join(), 1)
        await asyncio.wait_for(service.notifications.join(), 1)
        assert messages
        if failure == "error":
            assert "ошибка — ошибка анализа" in messages[-1]
        else:
            assert all("анализ выполняется" not in text for text in messages)
            assert "пропущен" in messages[-1]
        assert "пока не готова" not in messages[-1]
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
