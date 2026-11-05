"""Scan recorder and parity report for the native-migration shadow run.

Both the in-process fork and the external screener can write one JSONL line
per scan with `--record`. `compare` aligns those lines by scan time and checks
the fields that matter for parity: quality labels, flow/market/book values and
signal direction.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

QUALITY_EXACT = ("market", "flow", "book", "candles")
VALUE_TOLERANCE = {
    "cvd": ("rel", 0.005),
    "ofi": ("abs", 0.001),
    "volume": ("rel", 0.005),
    "trade_count": ("abs", 0.0),
    "trades_per_second": ("abs", 1e-6),
    "coverage": ("abs", 0.01),
    "price": ("abs", 1e-8),
    "best_bid": ("abs", 1e-8),
    "best_ask": ("abs", 1e-8),
    "spread": ("abs", 1e-8),
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


def _value(row: dict, group: str, field: str):
    return (row.get(group) or {}).get(field)


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
            for group in QUALITY_EXACT:
                lq, rq = _value(left, group, "quality"), _value(right, group, "quality")
                if lq != rq:
                    mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                       "field": f"{group}.quality", "baseline": lq, "candidate": rq})
            for group, fields in (
                ("flow", ("cvd", "ofi", "volume", "trade_count", "trades_per_second",
                          "coverage", "signal", "first_trade_price", "last_trade_price")),
                ("market", ("price", "best_bid", "best_ask", "spread", "natr", "range_ratio")),
                ("book", ("best_bid", "best_ask", "spread", "imbalance")),
            ):
                for field in fields:
                    lv, rv = _value(left, group, field), _value(right, group, field)
                    if not _matches(field, lv, rv):
                        mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                           "field": f"{group}.{field}", "baseline": lv, "candidate": rv})
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
