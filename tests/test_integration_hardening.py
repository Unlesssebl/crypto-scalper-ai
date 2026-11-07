"""M5: unified mode must fail explicitly instead of falling back to legacy scan."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest

from extensions.scalper.history.pulse_runtime import PulseObservation
from extensions.scalper.remote import RemoteHub


def _attention_error(status):
    return aiohttp.ClientResponseError(request_info=None, history=(), status=status)


@pytest.mark.asyncio
async def test_unified_remote_rejects_legacy_attention(monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
    hub = RemoteHub("http://unused")

    async def legacy(path, **params):
        assert path == "/v1/attention"
        return {"enabled": True, "runtime_mode": "legacy"}

    hub._get = legacy
    with pytest.raises(RuntimeError, match="unified"):
        await hub.refresh_snapshot()

    assert hub.ready is False
    assert hub.last_error and "unified" in hub.last_error


@pytest.mark.asyncio
async def test_unified_remote_rejects_missing_attention(monkeypatch):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
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
    monkeypatch.setenv("PULSE_UNIFIED", "1")
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
