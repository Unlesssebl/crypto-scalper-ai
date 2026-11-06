"""Source quality and lifecycle regression checks for the unified project."""
from __future__ import annotations

import json
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from extensions.scalper.pulse import PulseContext, pulse_text
from hyperdata_terminal.market_pulse.api import create_app
from hyperdata_terminal.market_pulse.gaps import GapTracker
from hyperdata_terminal.market_pulse.storage import InMemoryMarketPulseStore
from hyperdata_terminal.market_pulse.windows import WindowService

NOW = 1_700_000_170.0
START = (int(NOW * 1000) - 5_000) // 60_000 * 60_000 - 60_000


def source():
    pulse = PulseContext("http://source")
    pulse.status, pulse.updated_at = "connected", NOW
    for venue, market, channel in (("hyperliquid", "BTC", "trades"),
                                    ("okx", "BTC-USDT-SWAP", "trades-all")):
        pulse.channels.append({"venue": venue, "native_market_id": market, "channel": channel, "stale": False})
        pulse.windows.append({"venue": venue, "native_market_id": market, "channel": channel,
                              "aggregation_kind": "execution", "window_start_ms": START,
                              "quality": "observed_no_detected_gap", "provisional": False,
                              "version": 2, "execution_count": 10, "buy_quote": "80", "sell_quote": "20"})
    return pulse


def test_same_minute_comparison_keeps_currencies_and_versions_separate():
    facts = source().facts("BTC", NOW)
    assert facts["status"] == "ready" and facts["flow"] == "aligned_buy"
    assert [v["currency"] for v in facts["venues"]] == ["USDC", "USDT"]
    assert facts["activity"] == "threshold_not_calibrated"
    assert len(facts["evidence_id"]) == 64
    assert "USDC" in pulse_text(facts) and "USDT" in pulse_text(facts)
    revised = source()
    revised.windows[0]["version"] += 1
    assert revised.facts("BTC", NOW)["evidence_id"] != facts["evidence_id"]


@pytest.mark.parametrize("change", ["stale", "gap", "provisional", "older", "units", "negative", "nan"])
def test_degradation_never_confirms_two_venues(change):
    pulse = source()
    if change == "stale":
        pulse.channels[1]["stale"] = True
    elif change == "gap":
        pulse.windows[1]["quality"] = "gap_detected"
    elif change == "provisional":
        pulse.windows[1]["provisional"] = True
    elif change == "older":
        pulse.windows[1]["window_start_ms"] -= 60_000
    elif change == "units":
        pulse.windows[1]["coverage"] = {"notional_blocked_events": 1}
    else:
        pulse.windows[1]["buy_quote"] = "-1" if change == "negative" else "NaN"
    facts = pulse.facts("BTC", NOW)
    assert facts["status"] == "partial"
    assert facts["flow"] == "unavailable"


def test_expired_adapter_and_uncovered_symbols_are_explicit():
    pulse = source()
    assert pulse.facts("ETH", NOW)["status"] == "not_covered"
    assert pulse.facts("BTC", NOW + 16)["status"] == "stale"
    assert pulse.facts("BTC", NOW + 16)["venues"] == []


async def test_read_only_adapter_consumes_actual_api_and_survives_outage():
    store = InMemoryMarketPulseStore()
    server = TestServer(create_app(store))
    await server.start_server()
    pulse = PulseContext(str(server.make_url("")))
    async with aiohttp.ClientSession() as session:
        await pulse.refresh(session, NOW)
        assert pulse.status == "connected"
        assert pulse.facts("BTC", NOW)["status"] == "unavailable"
        await server.close()
        await pulse.refresh(session, NOW + 1)
        assert pulse.status == "unavailable"
        assert pulse.windows == [] and pulse.channels == []


async def test_invalid_schema_fails_closed():
    app = web.Application()

    async def invalid(request):
        return web.json_response({"schema_version": "wrong", "channels": [], "windows": []})

    app.router.add_get("/coverage", invalid)
    app.router.add_get("/windows", invalid)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        pulse = source()
        pulse.url = str(server.make_url(""))
        await pulse.refresh(session, NOW)
        assert pulse.status == "unavailable"
        assert pulse.facts("BTC", NOW)["venues"] == []


async def test_new_observation_does_not_erase_old_gap_or_poison_future_minutes():
    store = InMemoryMarketPulseStore()
    gaps = GapTracker(store)
    await gaps.on_connected("hyperliquid", "BTC", "trades", "e1", START)
    old = await gaps.on_disconnected("hyperliquid", "BTC", "trades", "e1", START + 1_000, reason="outage")
    await gaps.on_connected("hyperliquid", "BTC", "trades", "e2", START + 60_000)
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", START + 125_000)
    stored = (await store.list_gaps())[0]
    assert stored.gap_id == old.gap_id and stored.state == "gap_detected"
    assert stored.end_ms == START + 60_000
    window = await WindowService(store).rebuild(
        "hyperliquid", "BTC", "trades", "execution", START + 60_000, now_ms=START + 125_001,
    )
    assert window.quality == "observed_no_detected_gap"
    await gaps.on_disconnected("hyperliquid", "BTC", "trades", "e2", START + 130_000, reason="another outage")
    assert len(await store.list_gaps()) == 2


def test_control_rejects_foreign_project_and_handles_stale_state(tmp_path, monkeypatch):
    from scripts import project

    state = tmp_path / "project.json"
    state.write_text(json.dumps({"url": "http://unreachable"}))
    monkeypatch.setattr(project, "STATE", state)

    def unavailable(*args):
        raise OSError("offline")

    monkeypatch.setattr(project, "fetch", unavailable)
    assert project.control() is None
    monkeypatch.setattr(project, "fetch", lambda *a: {"data": {"root": "other-project"}})
    with pytest.raises(RuntimeError, match="different project"):
        project.control()
    state.write_text("{")
    assert project.control() is None


def test_launchers_share_single_entry_point():
    root = Path(__file__).resolve().parent.parent
    for script in ("start.ps1", "start.sh"):
        assert "scripts/project.py" in (root / script).read_text(encoding="utf-8")


def test_old_supervisor_cannot_delete_successors_state(tmp_path, monkeypatch):
    from scripts import project

    state = tmp_path / "project.json"
    state.write_text(json.dumps({"token": "successor"}))
    monkeypatch.setattr(project, "STATE", state)
    project.remove_owned_state("old-owner")
    assert state.exists()
    project.remove_owned_state("successor")
    assert not state.exists()


def test_restart_preserves_notification_flags(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from scripts import project

    state = tmp_path / "project.json"
    state.write_text(json.dumps({"forwarded": ["--notify"], "api_port": 8420, "web_port": 8421}))
    monkeypatch.setattr(project, "STATE", state)
    monkeypatch.setattr(project, "RUNTIME", tmp_path)
    monkeypatch.setattr(project, "load_dotenv", lambda *a: None)
    monkeypatch.setattr(project.sys, "argv", ["project.py", "--restart"])
    monkeypatch.setattr(project, "request_stop", lambda: None)
    results = iter([None, {"phase": "running"}])
    monkeypatch.setattr(project, "control", lambda: next(results))
    commands = []

    def spawn(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(poll=lambda: None)

    monkeypatch.setattr(project.subprocess, "Popen", spawn)
    project.main()
    assert len(commands) == 1 and "--notify" in commands[0]


def test_failed_stack_start_cleans_up_partial_stack_and_management_state(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from scripts import project

    (tmp_path / "deploy/market-pulse").mkdir(parents=True)
    state = tmp_path / "project.json"
    monkeypatch.setattr(project, "ROOT", tmp_path)
    monkeypatch.setattr(project, "STATE", state)
    monkeypatch.setattr(project.signal, "signal", lambda *a: None)

    class Server:
        def __init__(self, *args):
            pass

        def serve_forever(self):
            pass

        def shutdown(self):
            pass

        def server_close(self):
            pass

    monkeypatch.setattr(project, "ThreadingHTTPServer", Server)

    def unavailable(*args):
        raise OSError("not listening")

    monkeypatch.setattr(project, "fetch", unavailable)
    commands = []

    def compose(*args):
        commands.append(args)
        if args[0] == "up":
            raise RuntimeError("partial startup failure")

    monkeypatch.setattr(project, "compose", compose)
    args = SimpleNamespace(api_port=8420, web_port=8421, without_pulse=False, no_build=True, demo=False)
    with pytest.raises(RuntimeError, match="partial startup failure"):
        project.worker(args, [])
    assert commands == [("up", "-d"), ("stop",)]
    assert not state.exists()
