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


def _bucket(as_of: float) -> int:
    return int(round(as_of / 5.0) * 5)


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


def compare(baseline: list[dict], candidate: list[dict]) -> dict:
    base = {_bucket(record["as_of"]): record for record in baseline}
    cand = {_bucket(record["as_of"]): record for record in candidate}
    shared = sorted(base.keys() & cand.keys())
    mismatches: list[dict] = []
    compared = 0
    for stamp in shared:
        base_rows, cand_rows = _rows(base[stamp]), _rows(cand[stamp])
        for key in sorted(base_rows.keys() & cand_rows.keys()):
            compared += 1
            left, right = base_rows[key], cand_rows[key]
            for group, fields in GROUPS.items():
                for field in fields:
                    lv, rv = _value(left, group, field), _value(right, group, field)
                    if not _matches(field, lv, rv):
                        mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                           "field": f"{group}.{field}",
                                           "baseline": lv, "candidate": rv})
                    lq, rq = _quality(left, group, field), _quality(right, group, field)
                    if lq != rq:
                        mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                           "field": f"{group}.{field}.quality",
                                           "baseline": lq, "candidate": rq})
            ls, rs = _signal(left), _signal(right)
            if ls != rs:
                mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                   "field": "signal", "baseline": ls, "candidate": rs})
            ld, rd = _direction(left), _direction(right)
            if ld != rd:
                mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                   "field": "direction", "baseline": ld, "candidate": rd})
    return {
        "passed": not mismatches and bool(shared),
        "baseline_records": len(baseline),
        "candidate_records": len(candidate),
        "aligned_scans": len(shared),
        "rows_compared": compared,
        "mismatches": len(mismatches),
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
