"""Unified runtime must fail explicitly instead of falling back to a Top-20 scan."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest

from extensions.scalper.history.pulse_runtime import PulseObservation
from extensions.scalper.history.runtime import configure
from extensions.scalper.journal import Journal
from extensions.scalper.remote import RemoteHub
from extensions.scalper.service import Screener
from hyperdata_terminal.runtime_mode import UNIFIED_ENV, RuntimeModeError, compatibility_error


def _attention_error(status):
    return aiohttp.ClientResponseError(request_info=None, history=(), status=status)


def test_runtime_contract_requires_unified_attention():
    assert compatibility_error({"enabled": True, "runtime_mode": "unified"}) is None
    assert "missing" in compatibility_error({"enabled": True})
    assert "disabled" in compatibility_error({"enabled": False, "runtime_mode": "unified"})


@pytest.mark.asyncio
async def test_unified_remote_rejects_non_unified_attention(monkeypatch):
    hub = RemoteHub("http://unused")

    async def non_unified(path, **params):
        assert path == "/v1/attention"
        return {"enabled": True, "runtime_mode": "legacy"}

    hub._get = non_unified
    with pytest.raises(RuntimeError, match="unified"):
        await hub.refresh_snapshot()

    assert hub.ready is False
    assert hub.last_error and "unified" in hub.last_error


@pytest.mark.asyncio
async def test_unified_remote_rejects_missing_attention(monkeypatch):
    hub = RemoteHub("http://unused")

    async def missing(path, **params):
        raise _attention_error(404)

    hub._get = missing
    with pytest.raises(RuntimeError, match="unified"):
        await hub.refresh_snapshot()

    assert hub.attention == {"enabled": False}
    assert hub.last_error and "unified" in hub.last_error


@pytest.mark.asyncio
async def test_unified_remote_accepts_unified_attention(monkeypatch):
    hub = RemoteHub("http://unused")
    now = time.time()

    async def get(path, **params):
        if path == "/v1/attention":
            return {"enabled": True, "runtime_mode": "unified", "context_only": True, "active": []}
        if path == "/v1/market":
            return {"assets": [], "as_of": now}
        if path == "/v1/snapshot":
            return {"as_of": now, "market": dict(quality="ok", observed_at=now), "rows": [], "context": {}}
        assert path == "/v1/health"
        return {"status": "ok"}

    hub._get = get
    await hub.refresh_snapshot()

    assert hub.ready is True
    assert hub.attention["runtime_mode"] == "unified"
    assert hub.last_error is None


@pytest.mark.asyncio
async def test_recovered_executions_never_become_live_triggers(monkeypatch, tmp_path):
    async def waiting(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(PulseObservation, "run", waiting)
    accepted = []
    engine = SimpleNamespace(
        reset_external=lambda symbol: None,
        set_attention_symbols=AsyncMock(),
        accept_execution=lambda row: accepted.append(row) or True,
    )
    hub = SimpleNamespace(
        orderbook=SimpleNamespace(set_symbols=AsyncMock()),
        orderflow=engine,
    )
    observation = PulseObservation(hub, tmp_path)
    now = time.time()
    observation.plan = dict(
        schema_version="attention.v1",
        generated_at_ms=int(now * 1000),
        expires_at_ms=int((now + 60) * 1000),
        selected=["SMALL"],
        detail=["SMALL"],
        controls=[],
        policy_version=4,
        active=[dict(symbol="SMALL", state="focus", assigned_at=now - 10)],
    )

    def row(sequence, delivery_kind):
        return dict(
            event_seq=sequence,
            venue="hyperliquid",
            native_market_id="SMALL",
            delivery_kind=delivery_kind,
            side="buy",
            price="2",
            base_amount="3",
            quote_amount="6",
            event_time_ms=int(now * 1000),
            source_epoch="one",
            trade_id=str(sequence),
            raw_ref=f"raw:{sequence}",
        )

    replies = [
        dict(head_cursor=10, next_cursor=None, events=[]),
        dict(head_cursor=11, next_cursor=11, events=[row(11, "recovery")]),
        dict(head_cursor=12, next_cursor=12, events=[row(12, "live")]),
    ]

    async def get(path, **params):
        if path == "/attention":
            return dict(plan=observation.plan)
        return replies.pop(0)

    observation.get = get
    try:
        for _ in range(3):
            observation.last_control = 0
            await observation.cycle()
    finally:
        await observation.close()

    assert [item["event_seq"] for item in accepted] == [12]
    assert observation.cursor == 12


def _analyst():
    return SimpleNamespace(key="", provider="test", model="test/model")


@pytest.mark.asyncio
async def test_api_loss_after_success_keeps_history_but_blocks_new_reviews(tmp_path):
    hub = RemoteHub("http://unused")
    now = time.time()

    async def ok(path, **params):
        if path == "/v1/attention":
            return {"enabled": True, "runtime_mode": "unified", "context_only": True, "active": []}
        if path == "/v1/market":
            return {"assets": [], "as_of": now}
        if path == "/v1/snapshot":
            return {"as_of": now, "market": dict(quality="ok", observed_at=now), "rows": [], "context": {}}
        assert path == "/v1/health"
        return {"status": "ok"}

    hub._get = ok
    await hub.refresh_snapshot()
    assert hub.ready is True and hub.last_error is None

    async def down(path, **params):
        raise aiohttp.ClientConnectionError("native api dropped")

    hub._get = down
    with pytest.raises(aiohttp.ClientConnectionError):
        await hub.refresh_snapshot()
    # The cache is allowed to remain for history, but the failure is visible.
    assert hub.ready is True
    assert hub.last_error and "dropped" in hub.last_error
    assert hub.attention["enabled"] is True

    journal = Journal(tmp_path / "journal.sqlite")
    service = Screener(hub, journal, _analyst(), False)
    try:
        assert await service.review_context("SMALL") == {"status": "skipped", "reason": "native_unavailable"}
        assert await service.auto_context_once() == []
    finally:
        journal.close()


@pytest.mark.asyncio
async def test_transient_api_outage_waits_instead_of_restarting_the_screener():
    hub = RemoteHub("http://unused")

    async def down(path, **params):
        raise aiohttp.ClientConnectionError("native api refused the connection")

    hub._get = down
    with pytest.raises(aiohttp.ClientConnectionError):
        await hub.refresh_snapshot()

    service = Screener(hub, None, _analyst(), False)
    # Availability is not incompatibility: startup must keep waiting.
    assert service.validate_runtime() is False
    assert service.runtime_diagnostic() is None


@pytest.mark.asyncio
async def test_confirmed_legacy_attention_still_blocks_startup():
    hub = RemoteHub("http://unused")

    async def legacy(path, **params):
        return {"enabled": True, "runtime_mode": "legacy"}

    hub._get = legacy
    with pytest.raises(RuntimeModeError):
        await hub.refresh_snapshot()

    service = Screener(hub, None, _analyst(), False)
    with pytest.raises(RuntimeModeError, match="unified"):
        service.validate_runtime()
    assert service.runtime_diagnostic() and "unified" in service.runtime_diagnostic()


def test_standalone_live_hub_keeps_native_collectors(monkeypatch):
    monkeypatch.delenv(UNIFIED_ENV, raising=False)
    hub = SimpleNamespace(
        demo=False,
        orderbook=SimpleNamespace(symbols=["BTC"], books={"BTC": {}}, snapshots={"BTC": object()}),
        orderflow=SimpleNamespace(selective=False, pulse_external=False, include_binance=True, symbols=["BTC"]),
    )

    assert configure(hub) is None
    assert hub.orderflow.pulse_external is False
    assert hub.orderflow.include_binance is True
    assert hub.orderflow.symbols == ["BTC"]
    assert hub.orderbook.symbols == ["BTC"]


@pytest.mark.asyncio
async def test_unified_flag_creates_the_external_tape_owner(monkeypatch, tmp_path):
    async def waiting(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(PulseObservation, "run", waiting)
    monkeypatch.setenv(UNIFIED_ENV, "1")
    monkeypatch.setenv("HYPERDATA_HISTORY_DIR", str(tmp_path / "history"))
    hub = SimpleNamespace(
        demo=False,
        orderbook=SimpleNamespace(symbols=["BTC"], books={"BTC": {}}, snapshots={"BTC": object()}, raw_callback=None),
        orderflow=SimpleNamespace(selective=False, pulse_external=False, include_binance=True, symbols=["BTC"]),
    )

    observation = configure(hub)
    try:
        assert isinstance(observation, PulseObservation)
        assert hub.orderflow.pulse_external is True
        assert hub.orderflow.include_binance is False
        assert hub.orderflow.symbols == []
        assert hub.orderbook.symbols == []
        assert hub.orderbook.raw_callback == observation.raw
    finally:
        await observation.close()
