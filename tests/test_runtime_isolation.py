"""Unified mode stays isolated from native API state and legacy directional AI."""

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.analysis import snapshot_key
from extensions.scalper.journal import Journal
from extensions.scalper.market import metric
from extensions.scalper.service import Screener
from hyperdata_terminal.runtime_mode import (
    LEGACY,
    UNIFIED,
    RuntimeModeError,
    compatibility_error,
    require_compatible,
)


def snapshot(now, symbol="BTC", window="1m", **values):
    defaults = dict(
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
    defaults.update(values)
    return dict(
        symbol=symbol,
        window=window,
        contract=symbol + "-PERP",
        exchange="hyperliquid",
        captured_at=now,
        metrics={key: metric(value, "test", now, "ratio") for key, value in defaults.items()},
    )


def vote(snap, direction="BULLISH"):
    return dict(
        direction=direction,
        native_signal="STRONG_BULL" if direction == "BULLISH" else "STRONG_BEAR",
        source="test",
        evidence=["ofi"],
        snapshot=snapshot_key(snap),
    )


def row(snap):
    return dict(
        snapshot=snap,
        hyperdata=vote(snap),
        strength={"rank": 2, "label": "Заметная", "icon": "!"},
        facts=[],
        movement_phase=None,
        btc_context=None,
        analysis_gate="ready",
        context_5m="unavailable",
        other=None,
    )


def analyzer(calls):
    async def analyze(data):
        calls.append(data)
        return dict(
            status="ok",
            snapshot=snapshot_key(data),
            direction="BULLISH",
            summary="Поток",
            scenario="",
            arguments=[{"metric": "ofi"}],
        )

    return SimpleNamespace(
        session=AsyncMock(), key="key", provider="test", model="test/model", proxy=None, analyze=analyze
    )


@pytest.fixture
def journal(tmp_path):
    obj = Journal(tmp_path / "runtime.sqlite")
    yield obj
    obj.close()


def test_mode_mismatch_is_explicit():
    assert "native mode: missing" in compatibility_error(UNIFIED, {"enabled": True})
    assert "native mode: legacy" in compatibility_error(
        UNIFIED, {"enabled": True, "runtime_mode": LEGACY}
    )
    assert "unified native attention" in compatibility_error(
        LEGACY, {"enabled": True, "runtime_mode": UNIFIED}
    )
    assert compatibility_error(LEGACY, {"enabled": False}) is None
    assert compatibility_error(LEGACY, {"enabled": True, "runtime_mode": LEGACY}) is None
    with pytest.raises(RuntimeModeError, match="native mode: legacy"):
        require_compatible(UNIFIED, {"enabled": True, "runtime_mode": LEGACY})


def test_unified_without_attention_blocks_automatic_and_manual_legacy_ai(journal, monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
    now = time.time()
    calls = []
    service = Screener(
        SimpleNamespace(attention={}, demo=False, alerts=SimpleNamespace()),
        journal,
        analyzer(calls),
        False,
    )

    assert service.runtime_mode == UNIFIED
    assert service.legacy_ai_enabled is False
    assert "native mode: missing" in service.runtime_diagnostic()
    with pytest.raises(RuntimeModeError):
        service.validate_runtime()

    snap = snapshot(now)
    service.rows["BTC", "1m"] = row(snap)
    assert "Нет свежей" in service.request_analysis("BTC", "1m", now)
    service.detect(("BTC", "1m"), snap, vote(snap), now, "ready", "neutral")

    assert service.jobs.empty()
    assert journal.latest("BTC", "1m") == []
    assert service.candidate_seen == {}
    assert calls == []


@pytest.mark.asyncio
async def test_unified_worker_discards_queued_legacy_event(journal, monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
    calls = []
    service = Screener(
        SimpleNamespace(attention={}, demo=False, alerts=SimpleNamespace()),
        journal,
        analyzer(calls),
        False,
    )
    event = {"candidate_id": "legacy", "detected_at": time.time()}
    service.jobs.put_nowait((0, 0, 1, ("BTC", "1m"), event))
    task = asyncio.create_task(service.analyze())
    try:
        await asyncio.wait_for(service.jobs.join(), 1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert calls == []
    assert service.notifications.empty()
    assert journal.ai_reason("legacy") == "context_only"


def test_explicit_legacy_mode_keeps_manual_path_working(journal, monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "0")
    now = time.time()
    calls = []
    service = Screener(
        SimpleNamespace(attention={"enabled": False}, demo=False), journal, analyzer(calls), False
    )
    assert service.legacy_ai_enabled is True
    assert service.runtime_diagnostic() is None
    service.rows["BTC", "1m"] = row(snapshot(now))
    assert "принят" in service.request_analysis("BTC", "1m", now)
    assert service.jobs.qsize() == 1 and calls == []


@pytest.mark.asyncio
async def test_native_attention_declares_its_process_mode(monkeypatch):
    from hyperdata_terminal.api_server import HyperDataAPI

    monkeypatch.setenv("PULSE_UNIFIED", "1")
    api = HyperDataAPI(SimpleNamespace(observation=SimpleNamespace(report=lambda: {"enabled": True})))
    body = json.loads((await api.handle_attention(None)).text)
    assert body["runtime_mode"] == UNIFIED

    monkeypatch.setenv("PULSE_UNIFIED", "0")
    api = HyperDataAPI(SimpleNamespace())
    body = json.loads((await api.handle_attention(None)).text)
    assert body == {"enabled": False, "runtime_mode": LEGACY}
