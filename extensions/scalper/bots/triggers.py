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


# ---------------------------------------------------------------------------
# Event triggers: fed from the card pipeline instead of screener rows
# (docs/multi-bot-plan.md, stage 4). Same rule: only what was observable then.
# ---------------------------------------------------------------------------

# The side each tape reading implies for the next minutes, relative to the impulse it read.
TAPE_IMPLIED = {
    "counter_aggression": -1,  # the other side took over: the move turns
    "absorption": -1,  # aggression is absorbed: it does not carry on
    "exhaustion": -1,  # aggression faded: the move turns
    "thin_move": 1,  # price travels easily: it carries on
    "continuation": 1,
}


def _rank(value, steps):
    return 1 + sum(value >= step for step in steps)


def tape_reading(reading, price, natr):
    """A tape reading with the direction it implies; strength = impulse size against the 1m range."""
    if not reading or reading.get("kind") not in TAPE_IMPLIED or not price or price <= 0:
        return None
    impulse = abs(reading["prior"]["move_bps"])
    scale = (impulse / (natr * 100)) if natr else None
    strength = _rank(scale, (0.25, 0.5, 1.0)) if scale is not None else _rank(impulse, (10, 20, 40))
    return {
        "direction": TAPE_IMPLIED[reading["kind"]] * reading["side"],
        "strength": strength,
        "price": float(price),
        "natr": natr,
        "kind": reading["kind"],
        "impulse_bps": reading["prior"]["move_bps"],
        "recent_move_bps": reading["recent"].get("move_bps"),
        "window": "5m",
    }


def _level_strength(relative_size):
    return _rank(relative_size, (10, 20, 40))


def level_hold(zone, price, natr):
    """Price came to a significant level: holding means up from a bid, down from an ask."""
    if not zone or not price or price <= 0:
        return None
    return {
        "direction": 1 if zone["side"] == "bid" else -1,
        "strength": _level_strength(zone["relative_size"]),
        "price": float(price),
        "natr": natr,
        "level_price": zone["price"],
        "level_side": zone["side"],
        "relative_size": zone["relative_size"],
        "window": "5m",
    }


def level_break(zone, price, natr):
    """The same approach read as a break: down through a bid, up through an ask."""
    event = level_hold(zone, price, natr)
    return None if event is None else {**event, "direction": -event["direction"]}


LIQ_MIN_USD = 50_000
LIQ_MIN_MINUTE_SHARE = 0.5  # forced closures in 60s >= half of an average minute of this venue's volume


def liquidation_cascade(longs_usd, shorts_usd, volume_24h, price, *, exhaustion=False):
    """Confirmed liquidations in the last 60s, large for this instrument's own average minute.

    Continuation reads the move as carrying on in the forced side's direction (longs force-sold: down);
    exhaustion reads the opposite.
    """
    total = (longs_usd or 0) + (shorts_usd or 0)
    if not price or price <= 0 or not volume_24h or volume_24h <= 0 or total < LIQ_MIN_USD:
        return None
    share = total / (volume_24h / 1440)
    if share < LIQ_MIN_MINUTE_SHARE:
        return None
    direction = -1 if (longs_usd or 0) >= (shorts_usd or 0) else 1
    return {
        "direction": -direction if exhaustion else direction,
        "strength": _rank(share, (1, 3, 10)),
        "price": float(price),
        "longs_usd": longs_usd,
        "shorts_usd": shorts_usd,
        "minute_share": share,
        "window": "5m",
    }


def attention_entry(slot, price, flow_rank=None):
    """A symbol entered the attention plan. Absolute mode: the direction carries no meaning.

    slot: "overview" (selected only), "detail" (anomaly with orderbook detail) or "mover" (reserved
    liquid-mover slot). Rank 4 = a detail slot whose 5m flow is already pronounced (rank >= 3).
    """
    if not price or price <= 0 or slot not in {"overview", "detail", "mover"}:
        return None
    strength = 2 if slot == "overview" else 4 if (flow_rank or 0) >= 3 else 3
    return {"direction": 1, "strength": strength, "price": float(price), "slot": slot,
            "flow_rank": flow_rank, "window": "5m"}


WHALE_MIN_USD = 250_000
WHALE_MIN_MINUTE_SHARE = 1.0  # the added size >= one average minute of this coin's volume


def whale_position(event, volume_24h, price):
    """A new or increased whale position, sized against the coin's own average minute of volume."""
    if not event or not price or price <= 0 or not volume_24h or volume_24h <= 0:
        return None
    added = event.get("added_usd") or 0
    share = added / (volume_24h / 1440)
    if added < WHALE_MIN_USD or share < WHALE_MIN_MINUTE_SHARE:
        return None
    return {
        "direction": 1 if event["side"] == "long" else -1,
        "strength": _rank(share, (3, 10, 30)),
        "price": float(price),
        "kind": event["kind"],
        "added_usd": added,
        "size_usd": event.get("size_usd"),
        "minute_share": share,
        "leverage": event.get("leverage"),
        "lag_s": event.get("lag_s"),
        "address": event.get("address"),
        "window": "5m",
    }


TRIGGERS = {
    "flow_continuation": flow_continuation,
}

# Contracts fed by events (BotObservatory.record or the plan diff in tick), not by screener rows.
EVENT_TRIGGERS = frozenset({
    "tape_reading", "level_hold", "level_break", "liquidation_cascade", "liquidation_exhaustion",
    "attention_relative_move", "whale_follow",
})
