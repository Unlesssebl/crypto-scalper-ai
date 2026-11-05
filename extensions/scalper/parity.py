"""Scan recorder and parity report for the native-migration shadow run.

Both the in-process fork and the external screener can write one JSONL line
per scan with `--record`. `compare` aligns those lines by scan time and checks
the fields that matter for parity: per-metric quality labels, metric values and
the native signal direction.

Two row shapes are understood:

* the screener's internal `{"snapshot": {"metrics": ...}, "hyperdata": ...}`;
* the flat `/v1/snapshot` shape (`{"market": ..., "flow": ...}`), useful when
  comparing raw API payloads.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

GROUPS = {
    "market": ("price", "volume_24h", "open_interest", "funding_rate"),
    "flow": ("cvd", "ofi", "volume", "trade_count", "trade_rate",
             "first_trade_price", "last_trade_price"),
    "book": ("spread", "imbalance", "best_bid", "best_ask"),
    "candles": ("natr", "range_ratio"),
}
FIELD_ALIASES = {
    "trade_rate": ("trade_rate", "trades_per_second"),
    "trades_per_second": ("trades_per_second", "trade_rate"),
}
VALUE_TOLERANCE = {
    "cvd": ("rel", 0.005),
    "ofi": ("abs", 0.001),
    "volume": ("rel", 0.005),
    "trade_count": ("abs", 0.0),
    "trade_rate": ("abs", 1e-6),
    "trades_per_second": ("abs", 1e-6),
    "price": ("abs", 1e-8),
    "best_bid": ("abs", 1e-8),
    "best_ask": ("abs", 1e-8),
    "spread": ("abs", 1e-8),
    "imbalance": ("abs", 0.001),
    "first_trade_price": ("abs", 1e-8),
    "last_trade_price": ("abs", 1e-8),
    "natr": ("abs", 0.01),
    "range_ratio": ("abs", 0.01),
}


class Recorder:
    """Append-only JSONL recorder; one line per screener scan."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def record(self, now: float, rows, **extra) -> None:
        if isinstance(rows, dict):
            rows = [
                {"symbol": symbol, "window": window, **row}
                for (symbol, window), row in rows.items()
            ]
        payload = {"as_of": now, "rows": rows, **extra}
        self._fh.write(json.dumps(payload, default=str, ensure_ascii=False) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


def load(path: Path | str) -> list[dict]:
    records = []
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _rows(record: dict) -> dict[tuple[str, str], dict]:
    rows = record.get("rows") or []
    if isinstance(rows, dict):
        rows = rows.values()
    return {
        (row["symbol"], row["window"]): row
        for row in rows
        if isinstance(row, dict) and "symbol" in row and "window" in row
    }


def _metric(row: dict, group: str, field: str):
    metrics = (row.get("snapshot") or {}).get("metrics")
    names = FIELD_ALIASES.get(field, (field,))
    if metrics is not None:
        for name in names:
            if name in metrics:
                return metrics[name]
        return None
    flat = row.get(group) or {}
    for name in names:
        if name in flat:
            return flat[name]
    return None


def _value(row: dict, group: str, field: str):
    metric = _metric(row, group, field)
    return metric.get("value") if isinstance(metric, dict) else metric


def _quality(row: dict, group: str, field: str):
    metric = _metric(row, group, field)
    if isinstance(metric, dict):
        return metric.get("quality")
    return (row.get(group) or {}).get("quality")


def _signal(row: dict):
    hyperdata = row.get("hyperdata") or {}
    if "native_signal" in hyperdata:
        return hyperdata.get("native_signal")
    return (row.get("flow") or {}).get("signal")


def _direction(row: dict):
    return (row.get("hyperdata") or {}).get("direction")


def _matches(name: str, left, right) -> bool:
    if left is None or right is None:
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        mode, tolerance = VALUE_TOLERANCE.get(name, ("abs", 1e-8))
        if mode == "rel":
            scale = max(abs(left), abs(right), 1e-12)
            return abs(left - right) / scale <= tolerance
        return abs(left - right) <= tolerance
    return left == right


def _align(baseline: list[dict], candidate: list[dict], tolerance: float = 2.5):
    """Greedy nearest-neighbour alignment of two scan series.

    The fork and the external screener start independently, so their 5-second
    scans land on different grids; rounding to a shared grid would misalign
    half the scans. Match each baseline record to the closest unused candidate
    within `tolerance` seconds instead.
    """
    base = sorted(baseline, key=lambda record: record["as_of"])
    cand = sorted(candidate, key=lambda record: record["as_of"])
    used: set[int] = set()
    pairs: list[tuple[dict, dict]] = []
    for left in base:
        best: int | None = None
        best_dt: float | None = None
        for index, right in enumerate(cand):
            if index in used:
                continue
            dt = abs(left["as_of"] - right["as_of"])
            if dt <= tolerance and (best_dt is None or dt < best_dt):
                best, best_dt = index, dt
        if best is not None:
            used.add(best)
            pairs.append((left, cand[best]))
    return pairs


def compare(baseline: list[dict], candidate: list[dict]) -> dict:
    pairs = _align(baseline, candidate)
    mismatches: list[dict] = []
    by_field: dict[str, int] = {}
    compared = 0
    for left, right in pairs:
        base_rows, cand_rows = _rows(left), _rows(right)
        for key in sorted(base_rows.keys() & cand_rows.keys()):
            compared += 1
            base_row, cand_row = base_rows[key], cand_rows[key]
            stamp = left["as_of"]
            for group, fields in GROUPS.items():
                for field in fields:
                    lv, rv = _value(base_row, group, field), _value(cand_row, group, field)
                    if not _matches(field, lv, rv):
                        name = f"{group}.{field}"
                        mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                           "field": name, "baseline": lv, "candidate": rv})
                        by_field[name] = by_field.get(name, 0) + 1
                    lq, rq = _quality(base_row, group, field), _quality(cand_row, group, field)
                    if lq != rq:
                        name = f"{group}.{field}.quality"
                        mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                           "field": name, "baseline": lq, "candidate": rq})
                        by_field[name] = by_field.get(name, 0) + 1
            ls, rs = _signal(base_row), _signal(cand_row)
            if ls != rs:
                mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                   "field": "signal", "baseline": ls, "candidate": rs})
                by_field["signal"] = by_field.get("signal", 0) + 1
            ld, rd = _direction(base_row), _direction(cand_row)
            if ld != rd:
                mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                   "field": "direction", "baseline": ld, "candidate": rd})
                by_field["direction"] = by_field.get("direction", 0) + 1
    return {
        "passed": not mismatches and bool(pairs),
        "baseline_records": len(baseline),
        "candidate_records": len(candidate),
        "aligned_scans": len(pairs),
        "rows_compared": compared,
        "mismatches": len(mismatches),
        "by_field": dict(sorted(by_field.items(), key=lambda item: item[1], reverse=True)),
        "details": mismatches[:200],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare two screener JSONL recordings.")
    parser.add_argument("baseline")
    parser.add_argument("candidate")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    report = compare(load(args.baseline), load(args.candidate))
    text = json.dumps(report, indent=2, ensure_ascii=False)
    if args.report:
        args.report.write_text(text, encoding="utf-8")
    print(text)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
