"""Tests for the shadow-run recorder and parity report."""
from __future__ import annotations

from extensions.scalper.parity import Recorder, compare, load


def _row(quality: str = "ok", cvd: float = 100.0) -> dict:
    return {
        "snapshot": {"symbol": "BTC", "window": "1m"},
        "hyperdata": {"direction": "BULLISH", "quality": quality},
        "market": {"quality": quality, "price": 70000.0},
        "flow": {
            "quality": quality, "cvd": cvd, "ofi": 0.2, "volume": 200.0,
            "trade_count": 2, "trades_per_second": 0.03, "coverage": 0.98,
            "signal": "BULLISH", "first_trade_price": 69900.0, "last_trade_price": 70100.0,
        },
        "book": {"quality": quality, "best_bid": 69999.0, "best_ask": 70001.0,
                 "spread": 2.0, "imbalance": 0.1},
        "candles": {"quality": quality, "natr": 0.5, "range_ratio": 1.2},
    }


def _write(path, rows, as_of: float = 1000.0) -> None:
    recorder = Recorder(path)
    recorder.record(as_of, {("BTC", "1m"): rows})
    recorder.close()


def test_recorder_writes_symbol_window_and_parity_passes(tmp_path) -> None:
    baseline, candidate = tmp_path / "base.jsonl", tmp_path / "cand.jsonl"
    _write(baseline, _row())
    _write(candidate, _row())

    records = load(baseline)
    assert records[0]["rows"][0]["symbol"] == "BTC"
    assert records[0]["rows"][0]["window"] == "1m"

    report = compare(load(baseline), load(candidate))
    assert report["passed"] is True
    assert report["rows_compared"] == 1


def test_parity_reports_quality_and_value_mismatches(tmp_path) -> None:
    baseline, candidate = tmp_path / "base.jsonl", tmp_path / "cand.jsonl"
    _write(baseline, _row())
    _write(candidate, _row(quality="stale", cvd=500.0))

    report = compare(load(baseline), load(candidate))
    assert report["passed"] is False
    fields = {detail["field"] for detail in report["details"]}
    assert "flow.quality" in fields
    assert "flow.cvd" in fields


def test_parity_requires_aligned_scans(tmp_path) -> None:
    baseline, candidate = tmp_path / "base.jsonl", tmp_path / "cand.jsonl"
    _write(baseline, _row(), as_of=1000.0)
    _write(candidate, _row(), as_of=2000.0)

    report = compare(load(baseline), load(candidate))
    assert report["passed"] is False
    assert report["aligned_scans"] == 0
