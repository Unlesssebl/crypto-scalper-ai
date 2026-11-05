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
# Two independent live feeds cannot match tick-for-tick: prices are polled at
# different instants and a 1m window includes slightly different boundary
# trades. These are the tolerances used by the shadow report; the plan's
# original exact-price numbers were not achievable in practice.
VALUE_TOLERANCE = {
    "cvd": ("rel", 0.05),
    "ofi": ("abs", 0.02),
    "volume": ("rel", 0.05),
    "volume_24h": ("rel", 0.02),
    "open_interest": ("rel", 0.02),
    "funding_rate": ("abs", 1e-6),
    "trade_count": ("abs", 2.0),
    "trade_rate": ("rel", 0.10),
    "trades_per_second": ("rel", 0.10),
    "price": ("rel", 0.002),
    "best_bid": ("rel", 0.002),
    "best_ask": ("rel", 0.002),
    "spread": ("rel", 0.50),
    "imbalance": ("abs", 0.20),
    "first_trade_price": ("rel", 0.002),
    "last_trade_price": ("rel", 0.002),
    "natr": ("abs", 0.50),
    "range_ratio": ("abs", 0.50),
}
# Transient warmup/staleness is a startup/feed artifact, not a parity defect.
# 'unavailable' is NOT skipped: that is a real coverage gap.
SKIP_QUALITIES = {"warming_up", "stale"}
# Core = the signal path. Book/candle values are timing-sensitive snapshots and
# are allowed a higher rate; flow/signal mismatches are the ones that matter.
CORE_PREFIXES = ("flow.",)
CORE_NAMES = {"signal", "direction"}
# Rates are computed against comparison SLOTS actually exercised (a value slot
# plus its quality slot per field), not a hand-tuned field count: the old
# `compared * CORE_FIELDS` denominator diverged from the numerator's slot
# accounting and silently skewed the headline rate.
CORE_RATE = 0.10
SECONDARY_RATE = 0.30
SIGNAL_AGREEMENT = 0.95


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
    warmup_skipped = 0
    stale_skipped = 0
    signal_compared = 0
    signal_mismatches = 0
    core_slots = 0
    secondary_slots = 0
    for left, right in pairs:
        base_rows, cand_rows = _rows(left), _rows(right)
        for key in sorted(base_rows.keys() & cand_rows.keys()):
            compared += 1
            base_row, cand_row = base_rows[key], cand_rows[key]
            stamp = left["as_of"]
            flow_quality_left = _quality(base_row, "flow", "cvd")
            flow_quality_right = _quality(cand_row, "flow", "cvd")
            flow_warming = "warming_up" in (flow_quality_left, flow_quality_right)
            for group, fields in GROUPS.items():
                is_core = group == "flow"
                for field in fields:
                    lq, rq = _quality(base_row, group, field), _quality(cand_row, group, field)
                    if lq == "warming_up" or rq == "warming_up":
                        warmup_skipped += 1
                        continue
                    if lq in SKIP_QUALITIES or rq in SKIP_QUALITIES:
                        stale_skipped += 1
                        continue
                    # One value slot + one quality slot per field, only for
                    # slots actually exercised (not skipped above).
                    if is_core:
                        core_slots += 2
                    else:
                        secondary_slots += 2
                    lv, rv = _value(base_row, group, field), _value(cand_row, group, field)
                    if not _matches(field, lv, rv):
                        name = f"{group}.{field}"
                        mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                           "field": name, "baseline": lv, "candidate": rv})
                        by_field[name] = by_field.get(name, 0) + 1
                    if lq != rq:
                        name = f"{group}.{field}.quality"
                        mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                           "field": name, "baseline": lq, "candidate": rq})
                        by_field[name] = by_field.get(name, 0) + 1
            if flow_warming or flow_quality_left in SKIP_QUALITIES or flow_quality_right in SKIP_QUALITIES:
                warmup_skipped += 1
                continue
            # signal + direction are core slots.
            core_slots += 2
            signal_compared += 1
            ls, rs = _signal(base_row), _signal(cand_row)
            if ls != rs:
                signal_mismatches += 1
                mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                   "field": "signal", "baseline": ls, "candidate": rs})
                by_field["signal"] = by_field.get("signal", 0) + 1
            ld, rd = _direction(base_row), _direction(cand_row)
            if ld != rd:
                mismatches.append({"as_of": stamp, "symbol": key[0], "window": key[1],
                                   "field": "direction", "baseline": ld, "candidate": rd})
                by_field["direction"] = by_field.get("direction", 0) + 1
    core = sum(count for name, count in by_field.items()
               if name.startswith(CORE_PREFIXES) or name in CORE_NAMES)
    secondary = len(mismatches) - core
    core_rate = core / core_slots if core_slots else 1.0
    secondary_rate = secondary / secondary_slots if secondary_slots else 1.0
    signal_agreement = (1 - signal_mismatches / signal_compared) if signal_compared else 1.0
    passed = (
        bool(pairs)
        and core_rate <= CORE_RATE
        and secondary_rate <= SECONDARY_RATE
        and signal_agreement >= SIGNAL_AGREEMENT
    )
    return {
        "passed": passed,
        "baseline_records": len(baseline),
        "candidate_records": len(candidate),
        "aligned_scans": len(pairs),
        "rows_compared": compared,
        "warmup_skipped": warmup_skipped,
        "stale_skipped": stale_skipped,
        "mismatches": len(mismatches),
        "core_mismatches": core,
        "secondary_mismatches": secondary,
        "core_slots": core_slots,
        "secondary_slots": secondary_slots,
        "core_mismatch_rate": round(core_rate, 4),
        "secondary_mismatch_rate": round(secondary_rate, 4),
        "signal_compared": signal_compared,
        "signal_agreement": round(signal_agreement, 4),
        "thresholds": {
            "core": CORE_RATE,
            "secondary": SECONDARY_RATE,
            "signal_agreement": SIGNAL_AGREEMENT,
        },
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
