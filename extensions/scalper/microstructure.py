"""Effort vs result on the observed tape: why a move continues, slows or stops.

Deterministic reading of two stretches of the same tape: the last 15s against the 30s
before it. It compares how much aggressor volume was spent with how far trade prices
travelled. Labels are hypotheses ("вероятно"), never entry signals; top-N L2 snapshots
cannot prove hidden liquidity or replenishment, so zone wording stays observational.
Thresholds are initial engineering values, not proven trading edges.
"""

import math

RECENT_SECONDS = 15
PRIOR_SECONDS = 30
MIN_PRIOR_TRADES = 10
MIN_IMPULSE_USD = 10_000  # aggressor volume over the 30s impulse; dust moves are not impulses
MIN_RECENT_USD_FOR_EFFICIENCY = 2_000
MIN_RECENT_TRADES_FOR_ABSORPTION = 10
MIN_IMPULSE_IMBALANCE = 0.2
MIN_IMPULSE_BPS = 3
STALL_MOVE_RATIO = 0.3  # recent travel per second ≤ 30% of the impulse's
HELD_AGGRESSION = 0.8  # aggressor pace kept ≥ 80% → effort held
FADED_AGGRESSION = 0.5  # aggressor pace ≤ 50% → effort faded
COUNTER_IMBALANCE = 0.3
COUNTER_PACE = 0.6  # opposite side's pace vs the impulse aggressor's
ZONE_NEAR_BPS = 15

KINDS = ("absorption", "exhaustion", "counter_aggression", "thin_move", "slowdown", "continuation")


def _value(packet, key):
    fact = next((f for f in packet["facts"] if f["id"] == key), {})
    v = fact.get("value")
    return v if fact.get("quality") == "ok" and type(v) in (int, float) and math.isfinite(v) else None


def _segment(packet, name):
    seg = {k: _value(packet, f"tape.{name}.{k}") for k in ("buy_usd", "sell_usd", "trade_count", "price_change_bps")}
    return seg if None not in (seg["buy_usd"], seg["sell_usd"], seg["trade_count"]) else None


def read_tape(packet, spread_bps=None, zone=None, midpoint=None):
    """Returns None when the tape cannot support a reading, else a dict with kind/side/metrics."""
    prior, recent = _segment(packet, "prior"), _segment(packet, "recent")
    if not prior or not recent or prior["trade_count"] < MIN_PRIOR_TRADES or prior["price_change_bps"] is None:
        return None
    prior_volume = prior["buy_usd"] + prior["sell_usd"]
    if prior_volume <= 0:
        return None
    imbalance = (prior["buy_usd"] - prior["sell_usd"]) / prior_volume
    move = prior["price_change_bps"]
    natr = _value(packet, "context.1m.natr")
    spread_bps = spread_bps if spread_bps is not None else _value(packet, "book.spread_bps")
    threshold = max(MIN_IMPULSE_BPS, 2 * (spread_bps or 0), 0.3 * natr * 100 if natr else 0)
    side = 1 if move > 0 else -1
    # An impulse: prices travelled beyond noise, pushed by the same side's aggression.
    if abs(move) < threshold or imbalance * side < MIN_IMPULSE_IMBALANCE:
        return None

    def pace(usd, seconds):
        return usd / seconds

    aggressor = "buy_usd" if side > 0 else "sell_usd"
    opposite = "sell_usd" if side > 0 else "buy_usd"
    if prior[aggressor] < MIN_IMPULSE_USD:
        return None
    prior_pace = pace(prior[aggressor], PRIOR_SECONDS)
    recent_pace = pace(recent[aggressor], RECENT_SECONDS)
    opposite_pace = pace(recent[opposite], RECENT_SECONDS)
    effort = recent_pace / prior_pace if prior_pace else None
    recent_volume = recent["buy_usd"] + recent["sell_usd"]
    recent_imbalance = (recent["buy_usd"] - recent["sell_usd"]) / recent_volume if recent_volume else 0
    # A silent stretch has no trade travel: treat as zero result, not unknown.
    recent_move = recent["price_change_bps"] if recent["price_change_bps"] is not None else 0.0
    result = (recent_move * side / RECENT_SECONDS) / (abs(move) / PRIOR_SECONDS)

    def efficiency(travel, usd):
        return travel * side / (usd / 100_000) if usd > 0 else None

    if recent_imbalance * side <= -COUNTER_IMBALANCE and opposite_pace >= COUNTER_PACE * prior_pace:
        kind = "counter_aggression"
    elif result <= STALL_MOVE_RATIO:
        busy = recent["trade_count"] >= MIN_RECENT_TRADES_FOR_ABSORPTION
        if effort is not None and effort >= HELD_AGGRESSION and busy:
            kind = "absorption"
        elif effort is not None and effort <= FADED_AGGRESSION:
            kind = "exhaustion"
        else:
            kind = "slowdown"
    elif result >= 1 and effort is not None and effort <= FADED_AGGRESSION:
        kind = "thin_move"
    else:
        kind = "continuation"

    wall = None
    if zone and midpoint and kind in {"absorption", "slowdown", "counter_aggression"}:
        facing = "ask" if side > 0 else "bid"
        distance = abs(zone["price"] / midpoint - 1) * 10000
        if zone["side"] == facing and distance <= ZONE_NEAR_BPS:
            wall = dict(side=facing, price=zone["price"], distance_bps=distance, notional_usd=zone.get("notional_usd"))
    return dict(
        kind=kind,
        side=side,
        prior=dict(aggressor_usd=prior[aggressor], move_bps=move, seconds=PRIOR_SECONDS,
                   efficiency=efficiency(move, prior[aggressor])),
        recent=dict(aggressor_usd=recent[aggressor], opposite_usd=recent[opposite],
                    move_bps=recent["price_change_bps"], seconds=RECENT_SECONDS, trades=recent["trade_count"],
                    efficiency=efficiency(recent_move, recent[aggressor])),
        effort_ratio=effort,
        result_ratio=result,
        # Price travel per unit of aggressor volume, relative to the impulse (scale-free effort vs result).
        efficiency_ratio=result / effort
        if effort and recent[aggressor] >= MIN_RECENT_USD_FOR_EFFICIENCY else None,
        wall=wall,
    )


LABELS = {
    "absorption": ("Вероятно поглощение покупок", "Вероятно поглощение продаж"),
    "exhaustion": ("Вероятно истощение покупателей", "Вероятно истощение продавцов"),
    "counter_aggression": ("Продавцы перехватывают инициативу", "Покупатели перехватывают инициативу"),
    "thin_move": (
        "Рост на слабом объёме — возможно снятие заявок",
        "Падение на слабом объёме — возможно снятие заявок",
    ),
    "slowdown": ("Рост замедляется", "Падение замедляется"),
    "continuation": ("Рост продолжается", "Падение продолжается"),
}
# Readings that explain a stop or a change of character; continuation/slowdown are context.
NOTABLE = {"absorption", "exhaustion", "counter_aggression", "thin_move"}


def label(reading):
    return LABELS[reading["kind"]][0 if reading["side"] > 0 else 1]
