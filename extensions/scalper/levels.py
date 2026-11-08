"""Densities and levels for the levels bot: every threshold is relative to the instrument itself.

A visible level is significant for its own instrument when it is
- much larger than this book's usual level (≥ 6× the median visible level),
- slow to trade through at this instrument's own pace (≥ 20s of its last-60s turnover),
- near the price for this instrument's volatility (within 2 NATR of the 1m range; 0.5% if NATR is unknown),
- and standing (visible for ≥ 20s).
No dollar floor: $25k is dust for BTC and a wall for a small coin.

Events (docs/multi-bot-plan.md, chosen in the interview):
- "appeared": a significant level appeared near the price and stands;
- "approach": the price came to an announced level (within 0.3 NATR; 0.1% without NATR), or the level
  is being traded through (≥ 25% smaller than when announced while still visible).
Each event is sent once per level. Top-N L2 snapshots cannot prove who cancelled or filled what, so the
wording stays observational. Thresholds are initial engineering values.
"""

import math

MIN_RELATIVE = 6
MIN_EAT_SECONDS = 20
MIN_AGE_SECONDS = 20
NEAR_NATR = 2.0
NEAR_FALLBACK_BPS = 50
APPROACH_NATR = 0.3
APPROACH_FALLBACK_BPS = 10
EATEN_SHARE = 0.25
FORGET_SECONDS = 180
NEIGHBOR_BPS = 5  # adjacent price levels on one side are one wall, not several events


def finite(x):
    return type(x) in (int, float) and math.isfinite(x)


def zones_of(packet):
    """Every visible zone in the packet with its relative measures (not only the largest one)."""
    facts = {f["id"]: f for f in packet["facts"]}

    def value(key):
        f = facts.get(key, {})
        v = f.get("value")
        return v if f.get("quality") == "ok" and finite(v) else None

    zones = []
    for key in facts:
        if not key.startswith("density.") or not key.endswith(".relative_size"):
            continue
        prefix = key.removesuffix(".relative_size")
        zone = {n: value(f"{prefix}.{n}") for n in (
            "price", "size", "notional_usd", "relative_size", "observed_age_seconds", "distance_bps",
            "size_to_observed_turnover")}
        if None in (zone["price"], zone["size"], zone["relative_size"], zone["observed_age_seconds"]):
            continue
        zone.update(id=prefix, side=prefix.split(".")[1])
        zones.append(zone)
    return zones


class LevelWatcher:
    def __init__(self):
        self.known = {}  # (symbol, zone id) -> dict(announced_at, size, seen_at, approached)

    def near_bps(self, natr):
        return NEAR_NATR * natr * 100 if natr else NEAR_FALLBACK_BPS

    def approach_bps(self, natr):
        return APPROACH_NATR * natr * 100 if natr else APPROACH_FALLBACK_BPS

    def significant(self, zone, natr):
        eat = zone.get("size_to_observed_turnover")
        return bool(
            zone["relative_size"] >= MIN_RELATIVE
            and eat is not None and eat * 60 >= MIN_EAT_SECONDS
            and zone.get("distance_bps") is not None and zone["distance_bps"] <= self.near_bps(natr)
            and zone["observed_age_seconds"] >= MIN_AGE_SECONDS
        )

    def process(self, symbol, zones, natr, now):
        """Events for this snapshot: [dict(kind, zone, announced_size?)]; each level fires each kind once."""
        events = []
        # Largest first, so a wall is announced by its biggest level and its neighbours fold into it.
        for zone in sorted(zones, key=lambda z: -z["relative_size"]):
            key = (symbol, zone["id"])
            known = self.known.get(key)
            if known is None:
                if self.significant(zone, natr) and not self.neighbour(symbol, zone):
                    self.known[key] = dict(announced_at=now, size=zone["size"], seen_at=now, approached=False,
                                           side=zone["side"], price=zone["price"])
                    events.append(dict(kind="appeared", zone=zone))
                continue
            known["seen_at"] = now
            if known["approached"]:
                continue
            close = zone.get("distance_bps") is not None and zone["distance_bps"] <= self.approach_bps(natr)
            eaten = zone["size"] <= known["size"] * (1 - EATEN_SHARE)
            if close or eaten:
                known["approached"] = True
                events.append(dict(kind="approach", zone=zone, announced_size=known["size"],
                                   eaten_share=max(0.0, 1 - zone["size"] / known["size"]) if known["size"] else None,
                                   since=now - known["announced_at"]))
        for key in [k for k, v in self.known.items() if now - v["seen_at"] > FORGET_SECONDS]:
            del self.known[key]
        return events

    def neighbour(self, symbol, zone):
        """True when an announced level of the same side stands within NEIGHBOR_BPS of this one."""
        return any(
            s == symbol and v["side"] == zone["side"] and abs(v["price"] / zone["price"] - 1) * 10000 <= NEIGHBOR_BPS
            for (s, _), v in self.known.items()
        )
