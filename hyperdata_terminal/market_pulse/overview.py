"""Exchange candle summaries for broad discovery, never execution aggregates.

Only recently updated closed candles and a contiguous, same-epoch reference
qualify. These observations cannot prove absolute exchange completeness.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from statistics import median

MINUTE = 60_000
VERSION = "market-pulse.overview.v1"


def validate_candle(row, now_ms):
    if not isinstance(row, dict) or row.get("schema_version") != VERSION:
        raise ValueError("invalid candle schema")
    if not re.fullmatch(r"[\w.:-]{1,64}", str(row.get("symbol", ""))):
        raise ValueError("invalid candle instrument")
    for field in ("start_ms", "received_at_ms", "epoch_started_ms", "trade_count"):
        if type(row.get(field)) is not int or row[field] < 0:
            raise ValueError("invalid candle " + field)
    if (row["start_ms"] % MINUTE or row["start_ms"] > now_ms + 5_000
            or row["received_at_ms"] > now_ms + 5_000
            or row["received_at_ms"] < row["start_ms"]
            or row["epoch_started_ms"] > row["received_at_ms"]):
        raise ValueError("invalid candle times")
    for field in ("base_volume", "close"):
        try:
            value = Decimal(str(row[field]))
        except (KeyError, InvalidOperation) as exc:
            raise ValueError("invalid candle decimal") from exc
        if not value.is_finite() or value < 0 or (field == "close" and value == 0):
            raise ValueError("invalid candle decimal")
    if not all(isinstance(row.get(k), str) and row[k] for k in ("source_epoch", "raw_ref")):
        raise ValueError("missing candle provenance")
    return {k: row[k] for k in ("schema_version", "symbol", "start_ms", "received_at_ms",
                                "epoch_started_ms", "trade_count", "base_volume", "close",
                                "source_epoch", "raw_ref")}


def activity(rows, now_ms):
    """Past-only 14-minute median ratio; not a rarity or trading threshold."""
    target_start = (now_ms - 5_000) // MINUTE * MINUTE - MINUTE
    grouped = {}
    for row in rows:
        grouped.setdefault(row["symbol"], {})[row["start_ms"]] = row
    result = []
    for symbol, bars in sorted(grouped.items()):
        target = bars.get(target_start)
        item = {"symbol": symbol, "start_ms": target_start, "status": "missing",
                "source": "binance_candle_1m" if symbol.startswith("BINANCE:") else "hyperliquid_candle_1m",
                "reference_minutes": 14}
        if target:
            item.update(status="warming_up", trade_count=target["trade_count"],
                        base_volume=target["base_volume"])
            reference = [bars.get(target_start - i * MINUTE) for i in range(1, 15)]
            candidates = [target, *reference]
            valid = all(r and r["source_epoch"] == target["source_epoch"]
                        and r["epoch_started_ms"] <= r["start_ms"]
                        and r["start_ms"] + MINUTE - 10_000 <= r["received_at_ms"]
                        <= r["start_ms"] + MINUTE + 15_000 for r in candidates)
            if valid:
                ratios = {}
                for field in ("trade_count", "base_volume"):
                    baseline = median(Decimal(str(r[field])) for r in reference)
                    ratios[field] = (float(min(Decimal(1000), Decimal(str(target[field])) / baseline))
                                     if baseline > 0 else None)
                item.update(status="ready", ratios=ratios, reference_policy="previous_14_contiguous.v1",
                            zero_reference=any(v is None for v in ratios.values()))
        result.append(item)
    return {"schema_version": VERSION, "at_ms": now_ms, "items": result,
            "limits": "Exchange summaries; not execution totals, calibrated rarity or proof of completeness."}
