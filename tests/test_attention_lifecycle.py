"""Attention lease lifecycle: outage, expiry, release, recovery and reassignment."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.history.pulse_runtime import PulseObservation


def make_plan(now, *, symbols=("ETH",), expires_in=120, assigned_at=None):
    stamp = now - 5 if assigned_at is None else assigned_at
    return dict(
        schema_version="attention.v1",
        generated_at_ms=int(now * 1000),
        expires_at_ms=int((now + expires_in) * 1000),
        selected=list(symbols),
        detail=list(symbols),
        controls=[],
        policy_version=4,
        active=[dict(symbol=s, state="focus", reason="relative", assigned_at=stamp) for s in symbols],
    )


class Flow:
    def __init__(self):
        self.resets = []
        self.attention = []

    def reset_external(self, symbol):
        self.resets.append(symbol)

    async def set_attention_symbols(self, symbols):
        self.attention.append(list(symbols))


async def make_observer(monkeypatch, tmp_path):
    async def waiting(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(PulseObservation, "run", waiting)
    hub = SimpleNamespace(
        orderbook=SimpleNamespace(set_symbols=AsyncMock()),
        orderflow=Flow(),
    )
    return PulseObservation(hub, tmp_path), hub


async def idle_events(path, **params):
    assert path == "/events"
    return dict(head_cursor=0, next_cursor=None, events=[])


@pytest.mark.asyncio
async def test_malformed_plan_is_rejected_without_crashing(monkeypatch, tmp_path):
    observer, _hub = await make_observer(monkeypatch, tmp_path)

    async def malformed(path, **params):
        return {"plan": {"schema_version": "attention.v1", "detail": "not-a-list", "active": []}}

    observer.get = malformed
    observer.last_control = 0
    assert await observer._refresh_plan(time.time()) is False

    assert observer.plan is None
    assert observer.plan_error and "invalid attention" in observer.plan_error
    await observer.close()


@pytest.mark.asyncio
async def test_attention_outage_keeps_the_last_still_valid_plan(monkeypatch, tmp_path):
    observer, hub = await make_observer(monkeypatch, tmp_path)
    now = time.time()
    plan = make_plan(now)
    state = {"plan": plan, "outage": False}

    async def api(path, **params):
        if path == "/attention":
            if state["outage"]:
                raise RuntimeError("pulse unavailable")
            return {"plan": state["plan"]}
        return await idle_events(path, **params)

    observer.get = api
    observer.last_control = 0
    await observer.cycle()
    assert set(observer.applied) == {"ETH"}

    state["outage"] = True
    observer.last_control = 0
    hub.orderbook.set_symbols.reset_mock()
    hub.orderflow.resets.clear()
    await observer.cycle()

    assert set(observer.applied) == {"ETH"}
    hub.orderbook.set_symbols.assert_not_awaited()
    assert hub.orderflow.resets == ["ETH"]
    assert observer.plan_status == "cached_valid"
    assert observer.plan_error and "pulse unavailable" in observer.plan_error
    assert observer.report()["active"][0]["symbol"] == "ETH"
    assert observer.report()["degraded"] is True
    await observer.close()


@pytest.mark.asyncio
async def test_expired_lease_releases_detail_subscriptions_during_an_outage(monkeypatch, tmp_path):
    observer, hub = await make_observer(monkeypatch, tmp_path)
    now = time.time()
    plan = make_plan(now)
    state = {"plan": plan, "outage": False}

    async def api(path, **params):
        if path == "/attention":
            if state["outage"]:
                raise RuntimeError("pulse unavailable")
            return {"plan": state["plan"]}
        return await idle_events(path, **params)

    observer.get = api
    observer.last_control = 0
    await observer.cycle()
    assert set(observer.applied) == {"ETH"}

    # The lease expires while the API is down; no successful refresh may keep it alive.
    state["outage"] = True
    observer.plan = {**plan, "expires_at_ms": int(time.time() * 1000) - 1}
    state["plan"] = {**plan, "expires_at_ms": int(time.time() * 1000) - 1}
    observer.last_control = 0
    hub.orderbook.set_symbols.reset_mock()
    hub.orderflow.resets.clear()
    await observer.cycle()

    assert observer.applied == {}
    assert hub.orderbook.set_symbols.await_args.args[0] == []
    assert "ETH" in hub.orderflow.resets
    report = observer.report()
    assert report["plan_status"] == "expired"
    assert report["plan_error"]
    assert report["active"] == []
    assert report["degraded"] is True
    await observer.close()


@pytest.mark.asyncio
async def test_recovery_after_expiry_resubscribes_only_the_new_plan(monkeypatch, tmp_path):
    observer, hub = await make_observer(monkeypatch, tmp_path)
    now = time.time()
    plan = make_plan(now)
    state = {"plan": plan, "outage": False}

    async def api(path, **params):
        if path == "/attention":
            if state["outage"]:
                raise RuntimeError("pulse unavailable")
            return {"plan": state["plan"]}
        return await idle_events(path, **params)

    observer.get = api
    observer.last_control = 0
    await observer.cycle()

    state["outage"] = True
    observer.plan = {**plan, "expires_at_ms": int(time.time() * 1000) - 1}
    state["plan"] = {**plan, "expires_at_ms": int(time.time() * 1000) - 1}
    observer.last_control = 0
    await observer.cycle()
    assert observer.applied == {}

    # The same symbol with the same allocator episode must be re-added after release.
    recovered = make_plan(
        time.time(), symbols=("ETH",), assigned_at=plan["active"][0]["assigned_at"]
    )
    state.update(plan=recovered, outage=False)
    observer.last_control = 0
    hub.orderbook.set_symbols.reset_mock()
    hub.orderflow.attention.clear()
    await observer.cycle()

    assert set(observer.applied) == {"ETH"}
    assert hub.orderbook.set_symbols.await_args.args[0] == ["ETH"]
    assert hub.orderflow.attention[-1] == ["ETH"]
    report = observer.report()
    assert report["plan_status"] == "valid"
    assert report["plan_error"] is None
    assert report["active"][0]["symbol"] == "ETH"
    await observer.close()


@pytest.mark.asyncio
async def test_reassignment_invalidates_rolling_windows(monkeypatch, tmp_path):
    observer, hub = await make_observer(monkeypatch, tmp_path)
    now = time.time()
    assigned = now - 10
    plan = make_plan(now, assigned_at=assigned)
    state = {"plan": plan}

    async def api(path, **params):
        if path == "/attention":
            return {"plan": state["plan"]}
        return await idle_events(path, **params)

    observer.get = api
    observer.last_control = 0
    await observer.cycle()
    assert observer.applied["ETH"]["assigned_at"] == assigned

    reassigned_at = now + 1
    state["plan"] = make_plan(time.time(), assigned_at=reassigned_at)
    observer.last_control = 0
    hub.orderflow.resets.clear()
    await observer.cycle()

    assert observer.applied["ETH"]["assigned_at"] == reassigned_at
    assert hub.orderflow.resets == ["ETH"]
    assert hub.orderbook.set_symbols.await_args.kwargs["reset"] == ["ETH"]
    assert observer.report()["plan_status"] == "valid"
    await observer.close()
