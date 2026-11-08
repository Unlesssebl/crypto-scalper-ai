"""Liquid movers: obviously active, liquid instruments that never compete for attention slots.

The relative allocator ranks anomalies against each instrument's own norm, so a liquid coin in a long,
steady move (JCT on 08.11: $700M+ a day, +287% in 24h) never looks anomalous to itself and loses its
slot to illiquid spikes. This selector picks such instruments separately and reserves their own detail
slots; their cards go to the movers bot (docs/multi-bot-plan.md).

Eligible: T1–T2 by the venue's 24h volume. Moving: |1h| ≥ 2% from this process's own price samples
(needs ~50 minutes of history), or |24h| ≥ 20%. Thresholds are initial engineering values.
`price_change_24h_pct` from /v1/market is a fraction (2.87 = +287%), not percent.
"""

import math
from collections import deque

MIN_VOLUME_24H = 20e6  # the T2 floor
HOUR_MOVE = 0.02
DAY_MOVE = 0.20
HISTORY_SECONDS = 3900
MIN_HOUR_SPAN = 3000  # an "hourly" change needs at least 50 minutes of samples
HOLD_SECONDS = 600  # a pick keeps its slot this long unless it stops being liquid
STALE_SECONDS = 120


def finite(x):
    return type(x) in (int, float) and math.isfinite(x)


class MoversSelector:
    def __init__(self, slots=2, hold_seconds=HOLD_SECONDS):
        self.slots = slots
        self.hold_seconds = hold_seconds
        self.history = {}
        self.latest = {}
        self.picked = {}

    def observe(self, assets, now):
        for a in assets:
            symbol, price, volume = a.get("symbol"), a.get("price", a.get("mark_price")), a.get("volume_24h")
            if not isinstance(symbol, str) or not finite(price) or price <= 0 or not finite(volume):
                continue
            if volume < MIN_VOLUME_24H:
                self.history.pop(symbol, None)
                self.latest.pop(symbol, None)
                continue
            rows = self.history.setdefault(symbol, deque())
            rows.append((now, float(price)))
            while rows and now - rows[0][0] > HISTORY_SECONDS:
                rows.popleft()
            change = a.get("price_change_24h_pct")
            self.latest[symbol] = dict(price=float(price), volume_24h=float(volume), t=now,
                                       change_24h=float(change) if finite(change) else None)

    def change_1h(self, symbol, now):
        rows = self.history.get(symbol)
        if not rows or now - rows[0][0] < MIN_HOUR_SPAN:
            return None
        anchor = next((p for t, p in rows if t >= now - 3600), rows[0][1])
        return rows[-1][1] / anchor - 1

    def score(self, symbol, now):
        info = self.latest.get(symbol)
        if not info or now - info["t"] > STALE_SECONDS:
            return None  # gone from the market snapshot: not a mover on old numbers
        hour, day = self.change_1h(symbol, now), info["change_24h"]
        parts = [abs(hour) / HOUR_MOVE if hour is not None else 0, abs(day) / DAY_MOVE if day is not None else 0]
        best = max(parts)
        return dict(score=best, change_1h=hour, change_24h=day, volume_24h=info["volume_24h"]) if best >= 1 else None

    def choose(self, assets, now, exclude=(), capacity=None):
        """Symbols holding mover slots now; anomalies' own detail symbols are excluded, never doubled."""
        self.observe(assets, now)
        limit = self.slots if capacity is None else max(0, min(self.slots, capacity))
        keep = {}
        for symbol, pick in self.picked.items():
            if symbol in exclude or symbol not in self.latest or now - self.latest[symbol]["t"] > STALE_SECONDS:
                continue  # the hold protects a pick from replacement, not from vanishing from the market
            fresh = self.score(symbol, now)
            if fresh or now - pick["assigned_at"] < self.hold_seconds:
                # live = how strongly it moves now; a calmed pick only holds while there is room.
                keep[symbol] = {**pick, **(fresh or {}), "at": now, "live": (fresh or {}).get("score", 0)}
        ranked = sorted(
            ((s, self.score(s, now)) for s in self.latest if s not in exclude and s not in keep),
            key=lambda item: -(item[1] or {}).get("score", 0),
        )
        for symbol, info in ranked:
            if len(keep) >= limit or not info:
                break
            keep[symbol] = {**info, "assigned_at": now, "at": now, "live": info["score"]}
        # Moving now first when trimming to capacity.
        self.picked = dict(sorted(keep.items(), key=lambda kv: -kv[1]["live"])[:limit])
        return list(self.picked)

    def rows(self):
        """Plan assignments for the native hub: same shape as attention's active rows."""
        return [
            dict(symbol=s, assigned_at=p["assigned_at"], state="focus", score=round(p.get("score", 0), 3),
                 channel="movers", interest=dict(kind="mover", at=p["at"], change_1h=p.get("change_1h"),
                                                 change_24h=p.get("change_24h"), volume_24h=p.get("volume_24h")))
            for s, p in self.picked.items()
        ]
