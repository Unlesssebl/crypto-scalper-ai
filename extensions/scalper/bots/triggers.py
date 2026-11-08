"""Pure trigger functions.

A trigger turns one screener row into a trigger event or returns None.  It must
only read data that was observable at that moment; no future prices and no
retrospective rank calculation.
"""

from __future__ import annotations

import math


def _metric(row: dict, name: str):
    snapshot = row.get("snapshot") or {}
    item = (snapshot.get("metrics") or {}).get(name) or {}
    value = item.get("value")
    if item.get("quality") != "ok" or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def flow_continuation(row: dict) -> dict | None:
    """5m order-flow direction with a documented strength rank.

    Rank 0 means "no fresh data" and is not a signal.  Ranks 1-2 are the weak
    contrast group; ranks 3-4 are the strong group used by the primary test.
    """
    snapshot = row.get("snapshot") or {}
    native = row.get("hyperdata") or {}
    if snapshot.get("window") != "5m":
        return None
    if native.get("quality") != "ok":
        return None
    if row.get("analysis_gate", "ready") != "ready":
        return None

    direction = native.get("direction")
    if direction not in {"BULLISH", "BEARISH"}:
        return None

    strength = row.get("strength") or {}
    rank = strength.get("rank")
    if type(rank) is not int or rank < 1:
        return None

    from ..activity import wide_spread

    if wide_spread(snapshot):
        return None

    price = _metric(row, "price")
    ofi = _metric(row, "ofi")
    if price is None or price <= 0 or ofi is None:
        return None

    return {
        "direction": 1 if direction == "BULLISH" else -1,
        "direction_label": direction,
        "strength": rank,
        "price": price,
        "natr": _metric(row, "natr"),
        "ofi": ofi,
        "reasons": list(strength.get("reasons") or []),
        "window": "5m",
    }


TRIGGERS = {
    "flow_continuation": flow_continuation,
}
