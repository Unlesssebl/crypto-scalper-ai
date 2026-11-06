"""Regression cases from live attention, delayed replies and Windows shutdown audit."""
import asyncio
import json
import os
import subprocess
import sys
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from extensions.scalper.attention import Attention, AttentionPolicy
from extensions.scalper.history.archive import Archive, read_archive
from extensions.scalper.history.recover import recover
from hyperdata_terminal.data_layer.market_data import MarketData
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine
from hyperdata_terminal.lifecycle import run_managed


def allocated(state="focus", assigned_at=0, version=2):
    attention = Attention(AttentionPolicy(slots=1, version=version))
    for symbol in ("OLD", "NEW"):
        attention.previous[symbol] = (95, 1, 100)
        attention.baselines[symbol] = deque([.2] * 12, maxlen=120)
        attention.movement_baselines[symbol] = deque([0.] * 12, maxlen=120)
    attention.active["OLD"] = dict(symbol="OLD", state=state, since=95,
                                   assigned_at=assigned_at, score=10)
    return attention


@pytest.mark.parametrize("state,new_volume", [("focus", 120), ("cooling", 104)])
def test_relative_candidate_can_replace_mature_focus_or_cooling(state, new_volume):
    attention = allocated(state)
    changes = attention.update({"OLD": dict(price=1, volume_24h=101),
                                "NEW": dict(price=1, volume_24h=new_volume)}, 100)
    assert set(attention.active) == {"NEW"}
    assert attention.active["NEW"]["assigned_at"] == 100
    assert any(c["reason"] == "preempted" for c in changes)


def test_minimum_residency_prevents_switch_churn_and_capacity_counts_episodes():
    attention = allocated(assigned_at=90)
    for index, now in enumerate((100, 105), start=1):
        attention.update({"OLD": dict(price=1, volume_24h=100 + index),
                          "NEW": dict(price=1, volume_24h=100 + index * 20)}, now)
    assert set(attention.active) == {"OLD"}
    report = attention.report()
    assert report["capacity_rejections"] == 1
    assert report["capacity_checks"] == 2
    assert report["capacity_rejected_symbols"] == 1


def test_old_attention_policy_retains_replay_semantics():
    attention = allocated(version=1)
    attention.update({"OLD": dict(price=1, volume_24h=101),
                      "NEW": dict(price=1, volume_24h=120)}, 100)
    assert set(attention.active) == {"OLD"}


def test_context_lane_survives_full_five_minute_window_while_search_lane_rotates():
    from hyperdata_terminal.data_layer.orderflow_engine import TimeframeBucket, Trade
    attention = Attention(AttentionPolicy(slots=2, context_slots=1, context_seconds=330, baseline_samples=3))
    observations = {s: dict(price=1, volume_24h=100) for s in ("A", "B", "C")}
    for now in range(0, 25, 5):
        for row in observations.values():
            row["volume_24h"] = 100 + now / 5
        attention.update(observations, now)
    observations = {s: dict(price=1, volume_24h=120) for s in observations}
    attention.update(observations, 25)
    protected = next(a for a in attention.active.values() if a.get("context_until", 0) > 25)
    symbol = protected["symbol"]
    protected_since = protected["assigned_at"]
    bucket = TimeframeBucket(symbol, "5m", 300)
    preempted = False
    for now in range(30, 356, 5):
        for name in observations:
            observations[name]["volume_24h"] += 1000 if name == "C" and now == 85 else .001
        changes = attention.update(observations, now)
        preempted |= any(c["reason"] == "preempted" for c in changes)
        bucket.add_trade(Trade(now, symbol, "buy", 1, 1, 1))
        if now < 355:
            assert attention.active[symbol]["assigned_at"] == protected_since
            assert len(attention.active) <= 2
    assert preempted  # The unprotected lane still admitted new activity.
    snapshot = bucket.get_snapshot(now=355)
    assert snapshot.coverage == 1 and not snapshot.warming_up
    assert attention.active.get(symbol, {}).get("context_until", 0) <= 355


def test_context_protection_is_bounded_and_stale_scout_can_release_it():
    attention = allocated(version=3)
    attention._reserve_context(95)
    attention.update({}, 126)
    assert not attention.active


@pytest.mark.parametrize("slots,protected", [(1, 1), (2, 1), (4, 2)])
def test_context_reservations_leave_search_capacity_when_possible(slots, protected):
    attention = Attention(AttentionPolicy(slots=slots))
    attention.active = {str(i): dict(symbol=str(i), state="focus", assigned_at=0, since=0, score=10)
                        for i in range(slots)}
    attention._reserve_context(0)
    assert sum(a.get("context_until", 0) > 0 for a in attention.active.values()) == protected
    assert max(a.get("context_until", 0) for a in attention.active.values()) == 600
    original = {s: a["context_until"] for s, a in attention.active.items() if "context_until" in a}
    attention._reserve_context(601)
    assert all(attention.active[s]["context_until"] == deadline for s, deadline in original.items())


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["stop", "error"])
async def test_native_startup_cleanup_finalizes_partial_archive(tmp_path, monkeypatch, failure):
    from hyperdata_terminal.cli import _run_api
    marker = tmp_path / "stop"
    state = SimpleNamespace(stops=0, archive=None)
    class PartialHub:
        def __init__(self, **kwargs):
            pass
        async def start(self):
            state.archive = Archive(tmp_path / "capture")
            state.archive.record("policy", {}, 1)
            if failure == "error":
                raise RuntimeError("partial startup failure")
            marker.touch()
            await asyncio.Future()
        async def stop(self):
            state.stops += 1
            state.archive.close()
    monkeypatch.setenv("HYPERDATA_STOP_FILE", str(marker))
    monkeypatch.setattr("hyperdata_terminal.cli._api_universe", AsyncMock(return_value=None))
    monkeypatch.setattr("hyperdata_terminal.data_layer.hub.HyperDataHub", PartialHub)
    if failure == "error":
        with pytest.raises(RuntimeError, match="partial startup"):
            await run_managed(_run_api(0))
    else:
        await run_managed(_run_api(0))
    assert state.stops == 1
    assert len(list(read_archive(state.archive.path))) == 1


def test_supervisor_closes_native_when_screener_cannot_start(tmp_path, monkeypatch):
    from scripts import serve
    native = Mock()
    stopped = Mock()
    monkeypatch.setattr(serve, "health_ok", lambda port: False)
    monkeypatch.setattr(serve, "start_native", lambda *args: native)
    monkeypatch.setattr(serve, "wait_for_health", lambda *args: True)
    monkeypatch.setattr(serve, "start_screener", Mock(side_effect=OSError("spawn failed")))
    monkeypatch.setattr(serve, "stop", stopped)
    with pytest.raises(OSError, match="spawn failed"):
        serve.run(SimpleNamespace(api_port=1, web_port=2, demo=True, forwarded=[], stop_file=tmp_path / "stop"))
    stopped.assert_any_call(native, "native API")


def test_recovery_preserves_prefix_and_original_but_never_claims_completeness(tmp_path):
    archive = Archive(tmp_path / "capture")
    archive.record("policy", {}, 1)
    archive.close()
    (archive.path / "manifest.json").unlink()
    partial = archive.path / "000001.partial"
    partial.write_bytes(b'{"broken":')
    original = {p.name: p.read_bytes() for p in archive.path.iterdir()}
    destination = tmp_path / "recovered"
    result = recover(archive.path, destination)
    assert result["recovered"] and not result["complete"] and result["written"] == 1
    assert original == {p.name: p.read_bytes() for p in archive.path.iterdir()}
    with pytest.raises(ValueError, match="incomplete"):
        list(read_archive(destination))
    assert len(list(read_archive(destination, allow_incomplete=True))) == 1
    with pytest.raises(FileExistsError):
        recover(archive.path, destination)


def test_recovery_stops_at_sequence_gap(tmp_path):
    source = tmp_path / "old-run"
    source.mkdir()
    records = [dict(schema_version=1, run=source.name, sequence=i, received_at=i,
                    kind="policy", payload={}, epoch="") for i in (1, 3, 4)]
    (source / "000000.partial").write_text("".join(json.dumps(r) + "\n" for r in records))
    result = recover(source, tmp_path / "copy")
    assert result["written"] == 1
    assert "missing" in result["limitation"]


@pytest.mark.asyncio
async def test_stop_marker_awaits_cleanup(tmp_path, monkeypatch):
    marker = tmp_path / "stop"
    monkeypatch.setenv("HYPERDATA_STOP_FILE", str(marker))
    finished = []
    async def worker():
        try:
            marker.touch()
            await asyncio.Future()
        finally:
            await asyncio.sleep(.01)
            finished.append(True)
    await run_managed(worker())
    assert finished == [True]


def test_real_child_shutdown_finalizes_archive(tmp_path):
    from scripts import serve
    marker = tmp_path / "stop"
    root = tmp_path / "capture"
    program = '''import asyncio
from extensions.scalper.history.archive import Archive
from hyperdata_terminal.lifecycle import run_managed
async def worker():
    archive = Archive(ROOT)
    archive.record("policy", {}, 1)
    READY.write_text(str(archive.path))
    try:
        await asyncio.Future()
    finally:
        archive.close()
asyncio.run(run_managed(worker()))
'''
    ready = tmp_path / "ready"
    program = ("from pathlib import Path\nROOT=Path(" + repr(str(root)) + ")\nREADY=Path("
               + repr(str(ready)) + ")\n" + program)
    process = subprocess.Popen([sys.executable, "-c", program],
                               env={**os.environ, "HYPERDATA_STOP_FILE": str(marker)})
    serve.STOP_FILES[process.pid] = marker
    try:
        import time
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        assert ready.exists()
        serve.stop(process, "test child")
        assert process.returncode == 0
        assert len(list(read_archive(Path(ready.read_text())))) == 1
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_disabled_binance_is_not_an_outage():
    engine = OrderFlowEngine([], include_binance=False)
    engine.selective = True
    assert engine.venue_status("binance")[0] == "disabled"
    assert engine.venue_status("hyperliquid")[0] == "disabled"


def test_idle_health_does_not_report_disabled_streams_as_broken():
    from hyperdata_terminal.data_layer.health_monitor import DataHealthMonitor
    engine = OrderFlowEngine([], include_binance=False)
    engine.selective = True
    hub = SimpleNamespace(orderflow=engine, orderbook=SimpleNamespace(is_stale=lambda: True),
                          positions=SimpleNamespace(last_scan_at=0), status=SimpleNamespace(last_market_refresh=0),
                          deribit=SimpleNamespace(get_latest=lambda symbol: None))
    checks = {c.name: c for c in DataHealthMonitor(hub)._check_freshness()}
    assert checks["order_flow"].status == checks["orderbook"].status == "pass"
    assert "not evaluated" in checks["order_flow"].detail
    assert "order_flow_binance" not in checks


def test_premium_warning_deduplicates_but_preserves_significant_changes(monkeypatch):
    market = MarketData()
    logger = Mock()
    monkeypatch.setattr("hyperdata_terminal.data_layer.market_data.logger", logger)
    now = SimpleNamespace(value=0)
    monkeypatch.setattr("hyperdata_terminal.data_layer.market_data.time", SimpleNamespace(monotonic=lambda: now.value))
    market._warn_premium("X", 1, 1, 3, 1e6)
    market._warn_premium("X", 1, 1, 3.01, 1e6)
    assert logger.warning.call_count == 1
    market._warn_premium("X", 1, 1, 3.6, 1e6)
    market._warn_premium("X", 1, 1, -3.6, 1e6)
    assert logger.warning.call_count == 3
    now.value = 301
    market._warn_premium("X", 1, 1, -3.6, 1e6)
    assert logger.warning.call_count == 4
