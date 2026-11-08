"""Operational probes and temporally comparable attention outcomes."""

import csv
import json
import threading
import urllib.request
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.history.archive import Archive
from extensions.scalper.history.evaluate import evaluate
from extensions.scalper.relative_attention import Attention, AttentionPolicy
from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
from hyperdata_terminal.market_pulse.collectors.hyperliquid import HyperliquidCollector
from hyperdata_terminal.market_pulse.raw_archive import RawArchive
from scripts import project, serve


def test_local_probes_bypass_environment_proxy(monkeypatch):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.send_response(503 if self.path == "/degraded" else 200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')

        def do_POST(self):
            assert self.headers["Authorization"] == "Bearer local-only-secret"
            self.do_GET()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # Simulate both a system proxy and an environment with no loopback exception.
    monkeypatch.setattr(urllib.request, "getproxies", lambda: {"http": "http://127.0.0.1:1"})
    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: False)
    port = server.server_port
    try:
        assert serve.health_ok(port)
        assert project.fetch(f"http://127.0.0.1:{port}/status", "local-only-secret")["http_status"] == 200
        assert project.fetch(f"http://127.0.0.1:{port}/degraded")["http_status"] == 503
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def capture(root, *, missing_control=False, controls=True):
    archive = Archive(root)
    policy = AttentionPolicy(version=4, slots=2)
    attention = Attention(policy)
    archive.record("policy", asdict(policy), 0)
    for at in [*range(0, 60, 5), 61, 66]:
        observations = {s: dict(price=10 * (1 + at * slope / 10000), volume_24h=0)
                        for s, slope in {"A": 1, "B": 1, **({"C": .1, "D": 0} if controls else {})}.items()}
        if at == 0:
            for symbol in ("A", "B"):
                observations[symbol]["candle_ratio"] = 8
        if missing_control and at > 20:
            observations.pop("C", None)
        archive.record("scout", observations, at)
        for change in attention.update(observations, at):
            archive.record("assignment", change, at)
    archive.close()
    return archive.path


def test_same_time_controls_exclude_all_batch_selections_and_include_endpoint(tmp_path):
    source = capture(tmp_path / "capture")
    output = tmp_path / "result"
    report = evaluate(source, output)
    pairs = list(csv.DictReader((output / "pairs.csv").open(encoding="utf-8")))
    observed = [p for p in pairs if p["status"] == "observed"]
    assert len(observed) == 2
    assert all(set(json.loads(p["control_symbols"])) == {"C", "D"} for p in pairs)
    # First endpoint is at t=61: disclose the actual horizon and include that
    # quote in extrema. Previously max_abs could be smaller than abs(end).
    assert report["time_matched"]["60s"]["mean_abs_end_lift_bps"] == pytest.approx(61 * .95)
    assert report["time_matched"]["60s"]["mean_max_abs_lift_bps"] == pytest.approx(61 * .95)
    assert report["time_matched"]["300s"]["coverage"] == 0
    with (output / "outcomes.csv").open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["status"] == "observed":
                assert float(row["elapsed_seconds"]) == 61
                assert max(float(row["max_up_bps"]), -float(row["max_down_bps"])) >= abs(float(row["end_bps"]))
    again = evaluate(source, tmp_path / "again")
    assert again == report
    assert (output / "pairs.csv").read_bytes() == (tmp_path / "again/pairs.csv").read_bytes()


@pytest.mark.parametrize("kwargs", [{"missing_control": True}, {"controls": False}])
def test_missing_controls_never_silently_improve_comparison_coverage(tmp_path, kwargs):
    report = evaluate(capture(tmp_path / "capture", **kwargs), tmp_path / "result")
    assert report["groups"]["selected/60s"]["observed"] == 2
    comparison = report["time_matched"]["60s"]
    assert comparison["total"] == comparison["unavailable"] == 2
    assert comparison["observed"] == 0
    assert comparison["mean_abs_end_lift_bps"] is None


def test_control_capacity_must_be_explicitly_bounded(tmp_path):
    with pytest.raises(ValueError, match="control_count"):
        evaluate(tmp_path, tmp_path / "output", control_count=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [{"t": 1}, {"n": -1}, {"c": "NaN"}, {"s": []}])
async def test_invalid_summary_cannot_interrupt_execution_tape(tmp_path, monkeypatch, bad):
    stamp = 1_800_000_000_000
    monkeypatch.setattr("hyperdata_terminal.market_pulse.collectors.base.now_ms", lambda: stamp)
    sink, gaps = AsyncMock(), AsyncMock()
    archive = RawArchive(tmp_path, "hyperliquid")
    collector = HyperliquidCollector(CollectorConfig("hyperliquid", ("BTC",), str(tmp_path), ""),
                                     archive, sink, gaps)
    collector.overview_markets = {"BTC"}
    collector._overview_epoch_start = stamp - 60_000
    candle = dict(t=stamp, T=stamp + 59_999, s="BTC", i="1m", n=20, v="10", c="100")
    try:
        # Reject a newer invalid summary after accepting an earlier update.
        await collector._handle_payload(json.dumps(dict(channel="candle", data=candle)), "epoch")
        await collector._handle_payload(json.dumps(dict(channel="candle", data={**candle, **bad})), "epoch")
        assert collector.overview_rejected == 1
        assert collector.overview_last_error["raw_ref"]
        # The same processing loop is still usable for canonical trades.
        trade = dict(coin="BTC", time=stamp, side="B", px="100", sz="1", tid=1)
        await collector._handle_payload(json.dumps(dict(channel="trades", data=[trade])), "epoch")
        await collector.flush(force=True)
        sink.append_batch.assert_awaited_once()
        assert len(sink.append_batch.call_args.args[0]) == 1
        gaps.disconnected.assert_not_awaited()
        if bad in ({"n": -1}, {"c": "NaN"}):
            assert not collector._overview_pending
    finally:
        archive.close()
