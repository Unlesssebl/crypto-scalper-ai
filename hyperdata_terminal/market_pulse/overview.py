"""Exchange candle summaries for broad discovery, never execution aggregates.

Only closed candles qualify: Binance klines flagged closed (a reference of at least
10 of the previous 14 minutes, across reconnects), Hyperliquid candles updated within
10s of the close with a contiguous, same-epoch reference.
These observations cannot prove absolute exchange completeness.
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


# Binance klines arrive flagged closed (x=true) and the collector keeps only those, so a bar is final
# whichever socket delivered it and however late. Its reference may then span reconnects and tolerate a
# few missing minutes. Hyperliquid has no closed flag: a bar counts as final only when its last update came
# within 10s of the close, and its 14 reference minutes must be contiguous from one connection.
BINANCE_MIN_REFERENCE = 10
BINANCE_MAX_LATE_MS = 120_000


def _final(row, symbol):
    closes = row["start_ms"] + MINUTE
    if symbol.startswith("BINANCE:"):
        return closes - 10_000 <= row["received_at_ms"] <= closes + BINANCE_MAX_LATE_MS
    return row["epoch_started_ms"] <= row["start_ms"] and closes - 10_000 <= row["received_at_ms"] <= closes + 15_000


def activity(rows, now_ms):
    """Past-only 14-minute median ratio; not a rarity or trading threshold."""
    target_start = (now_ms - 5_000) // MINUTE * MINUTE - MINUTE
    grouped = {}
    for row in rows:
        grouped.setdefault(row["symbol"], {})[row["start_ms"]] = row
    result = []
    for symbol, bars in sorted(grouped.items()):
        target = bars.get(target_start)
        binance = symbol.startswith("BINANCE:")
        item = {"symbol": symbol, "start_ms": target_start, "status": "missing",
                "source": "binance_candle_1m" if binance else "hyperliquid_candle_1m",
                "reference_minutes": 14}
        if target:
            item.update(status="warming_up", trade_count=target["trade_count"],
                        base_volume=target["base_volume"])
            window = [bars.get(target_start - i * MINUTE) for i in range(1, 15)]
            if binance:
                reference = [r for r in window if r and _final(r, symbol)]
                valid = _final(target, symbol) and len(reference) >= BINANCE_MIN_REFERENCE
                policy = "previous_14_closed_min10.v2"
            else:
                reference = window
                valid = all(r and r["source_epoch"] == target["source_epoch"] and _final(r, symbol)
                            for r in [target, *window])
                policy = "previous_14_contiguous.v1"
            if valid:
                ratios = {}
                for field in ("trade_count", "base_volume"):
                    baseline = median(Decimal(str(r[field])) for r in reference)
                    ratios[field] = (float(min(Decimal(1000), Decimal(str(target[field])) / baseline))
                                     if baseline > 0 else None)
                item.update(status="ready", ratios=ratios, reference_policy=policy,
                            reference_bars=len(reference),
                            zero_reference=any(v is None for v in ratios.values()))
        result.append(item)
    return {"schema_version": VERSION, "at_ms": now_ms, "items": result,
            "limits": "Exchange summaries; not execution totals, calibrated rarity or proof of completeness."}
