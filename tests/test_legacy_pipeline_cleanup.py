"""Unified runtime must not start or feed the legacy directional pipeline."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from apps.screener_web.__main__ import service_workers
from extensions.scalper.analysis import snapshot_key
from extensions.scalper.journal import Journal
from extensions.scalper.market import metric
from extensions.scalper.service import Screener


def snapshot(now, symbol="BTC", window="1m"):
    values = dict(
        price=100,
        ofi=0.8,
        cvd=500,
        spread=0.01,
        imbalance=0.4,
        activity_ratio=2.5,
        trade_count=30,
        natr=0.2,
        range_ratio=1,
        first_trade_price=100,
        last_trade_price=100,
    )
    return dict(
        symbol=symbol,
        window=window,
        contract=symbol + "-PERP",
        exchange="hyperliquid",
        captured_at=now,
        metrics={key: metric(value, "test", now, "ratio") for key, value in values.items()},
    )


def vote(snap):
    return dict(
        direction="BULLISH",
        native_signal="STRONG_BULL",
        source="test",
        evidence=["ofi"],
        snapshot=snapshot_key(snap),
    )


def analyzer(calls):
    async def analyze(data):
        calls.append(data)
        return dict(status="ok", snapshot=snapshot_key(data))

    return SimpleNamespace(
        session=AsyncMock(), key="key", provider="test", model="test/model", proxy=None, analyze=analyze
    )


def make_service(journal, calls, *, demo=False):
    return Screener(
        SimpleNamespace(attention={}, demo=demo, alerts=SimpleNamespace()),
        journal,
        analyzer(calls),
        False,
    )


@pytest.fixture
def journal(tmp_path):
    obj = Journal(tmp_path / "legacy.sqlite")
    yield obj
    obj.close()


def test_unified_mode_starts_only_the_scan_worker(journal, monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
    calls = []
    service = make_service(journal, calls)

    assert [worker.__name__ for worker in service_workers(service)] == ["run"]


def test_explicit_legacy_mode_keeps_analysis_and_delivery_workers(journal, monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "0")
    calls = []
    service = make_service(journal, calls)

    assert [worker.__name__ for worker in service_workers(service)] == ["run", "analyze", "deliver"]


def test_unified_detection_creates_no_legacy_candidate_or_job(journal, monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
    now = time.time()
    calls = []
    service = make_service(journal, calls)
    snap = snapshot(now)

    service.detect(("BTC", "1m"), snap, vote(snap), now, "ready", "neutral")

    assert service.jobs.empty()
    assert service.candidate_seen == {}
    assert journal.latest("BTC", "1m") == []
    assert calls == []


def test_unified_queue_hypothesis_is_inert(journal, monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
    service = make_service(journal, [])

    service.notify = True
    service.queue_hypothesis("old")

    assert service.notifications.empty()


def test_unified_recovery_retires_old_pending_work_without_replaying_it(journal, monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
    now = time.time()
    snap = snapshot(now)
    event = dict(type="manual", direction="BULLISH", detected_at=now)
    journal.record_candidate("old", snap, event, "queued", now)
    journal.register_fact_message("old", snap, event, vote(snap))
    journal.fact_message_sent("old", 42, False)
    journal.select_ai("old", now, "early", "queued")
    with journal.db:
        journal.db.execute("INSERT INTO pulse_card_edits VALUES(?,?,?)", ("old", "queued", now))

    calls = []
    service = make_service(journal, calls)

    assert service.recover_pending() == ["old"]
    assert service.jobs.empty()
    assert service.notifications.empty()
    assert journal.pending_pulse_edits() == []
    assert journal.fact_message("old")["ai"]["reason"] == "interrupted"
    assert calls == []
