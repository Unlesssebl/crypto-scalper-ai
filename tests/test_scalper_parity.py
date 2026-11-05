"""Tests for the shadow-run recorder and parity report."""
from __future__ import annotations

from extensions.scalper.parity import Recorder, compare, load


def _metric(value, quality: str = "ok") -> dict:
    return {"value": value, "quality": quality}


def _row(quality: str = "ok", cvd: float = 100.0) -> dict:
    metrics = {
        "price": _metric(70000.0, quality),
        "volume_24h": _metric(5_000_000.0, quality),
        "open_interest": _metric(1_000_000.0, quality),
        "funding_rate": _metric(0.00001, quality),
        "cvd": _metric(cvd, quality),
        "ofi": _metric(0.2, quality),
        "volume": _metric(200.0, quality),
        "trade_count": _metric(2, quality),
        "trade_rate": _metric(0.03, quality),
        "first_trade_price": _metric(69900.0, quality),
        "last_trade_price": _metric(70100.0, quality),
        "spread": _metric(2.0, quality),
        "imbalance": _metric(0.1, quality),
        "best_bid": _metric(69999.0, quality),
        "best_ask": _metric(70001.0, quality),
        "natr": _metric(0.5, quality),
        "range_ratio": _metric(1.2, quality),
    }
    return {
        "snapshot": {"symbol": "BTC", "window": "1m", "metrics": metrics},
        "hyperdata": {"direction": "BULLISH", "native_signal": "BULLISH", "quality": quality},
    }


def _flat_row(quality: str = "ok", cvd: float = 100.0) -> dict:
    return {
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


def _write(path, row, as_of: float = 1000.0) -> None:
    recorder = Recorder(path)
    recorder.record(as_of, {("BTC", "1m"): row})
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
    _write(candidate, _row(quality="clock_skew", cvd=500.0))

    report = compare(load(baseline), load(candidate))
    assert report["passed"] is False
    fields = {detail["field"] for detail in report["details"]}
    assert "flow.cvd" in fields
    assert "flow.cvd.quality" in fields


def test_parity_accepts_flat_api_rows(tmp_path) -> None:
    baseline, candidate = tmp_path / "base.jsonl", tmp_path / "cand.jsonl"
    _write(baseline, _flat_row())
    _write(candidate, _flat_row())

    report = compare(load(baseline), load(candidate))
    assert report["passed"] is True


def test_parity_aligns_scans_within_tolerance(tmp_path) -> None:
    baseline, candidate = tmp_path / "base.jsonl", tmp_path / "cand.jsonl"
    _write(baseline, _row(), as_of=1001.0)
    _write(candidate, _row(), as_of=1003.0)

    report = compare(load(baseline), load(candidate))
    assert report["aligned_scans"] == 1
    assert report["passed"] is True


def test_parity_requires_aligned_scans(tmp_path) -> None:
    baseline, candidate = tmp_path / "base.jsonl", tmp_path / "cand.jsonl"
    _write(baseline, _row(), as_of=1000.0)
    _write(candidate, _row(), as_of=2000.0)

    report = compare(load(baseline), load(candidate))
    assert report["passed"] is False
    assert report["aligned_scans"] == 0
