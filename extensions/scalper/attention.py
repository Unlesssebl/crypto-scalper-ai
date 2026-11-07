"""Broad discovery and bounded, persistent attention. Never an anomaly verdict."""

from __future__ import annotations

import json
import math

from .relative_attention import Attention as RelativeAttention
from .relative_attention import AttentionPolicy

VERSION = "attention.v1"


class PulseAttention:
    def __init__(self, journal, *, slots=20, detail_slots=6, hold_seconds=300):
        if not 1 <= detail_slots <= slots <= 20 or hold_seconds < 60:
            raise ValueError("invalid attention limits")
        self.journal, self.slots, self.detail_slots, self.hold = journal, slots, detail_slots, hold_seconds
        self.selected = {}
        self.anchors = {}
        self.plan = {"schema_version": VERSION, "selected": [], "detail": [], "controls": []}
        self.last_at = 0
        self.activity = {}
        self.detail_state = {}
        self.last_prune_minute = None
        if journal:
            row = journal.db.execute("SELECT value FROM scalper_settings WHERE name='attention'").fetchone()
            if row:
                saved = json.loads(row[0])
                self.selected = saved.get("selected_state", {})
                self.anchors = saved.get("anchors", {})
                self.plan["detail"] = saved.get("detail", [])
                self.detail_state = saved.get("detail_state", {})

    def allocate(self, ranking, states, limit, valid, now):
        chosen = {s: state for s, state in states.items() if s in ranking and 0 <= now - state["since"] < self.hold}
        for symbol in ranking:
            if len(chosen) >= limit:
                break
            chosen.setdefault(symbol, {"since": now})
        for symbol in ranking:
            if symbol in chosen:
                continue
            replaceable = [s for s in chosen if now - chosen[s]["since"] >= 60]
            if not replaceable:
                break
            weakest = min(replaceable, key=lambda s: valid[s]["score"])
            if valid[symbol]["score"] <= valid[weakest]["score"] * 1.5:
                break
            del chosen[weakest]
            chosen[symbol] = {"since": now}
        return chosen

    def choose(self, assets, now):
        if not math.isfinite(now) or now <= self.last_at:
            return [s for s in self.selected if s in {a["symbol"] for a in assets}]
        self.last_at = now
        valid = {}
        for asset in assets:
            symbol = asset.get("symbol")
            price, volume = asset.get("mark_price"), asset.get("volume_24h")
            if (
                not isinstance(symbol, str)
                or not symbol
                or not isinstance(price, (float, int))
                or not math.isfinite(price)
                or price <= 0
                or not isinstance(volume, (float, int))
                or not math.isfinite(volume)
                or volume <= 0
            ):
                continue
            anchor = self.anchors.get(symbol)
            change = abs(math.log(price / anchor[1])) * 10_000 if anchor and 30 <= now - anchor[0] <= 180 else 0
            # Liquidity is a bootstrap/tie-breaker, short-horizon movement drives discovery.
            candle = self.activity.get(symbol, {})
            ratios = candle.get("ratios", {})
            # Discovery preference only; this is not calibrated rarity.
            activity_score = 0
            if candle.get("status") == "ready" and int((now * 1000 - 5000) // 60_000 * 60_000 - 60_000) == candle.get(
                "start_ms"
            ):
                activity_score = max(
                    (
                        min(1000, v)
                        for v in ratios.values()
                        if isinstance(v, (int, float)) and math.isfinite(v) and v >= 0
                    ),
                    default=0,
                )
                activity_score = max(0, math.log2(max(1, activity_score)) * 10)
            score = change + math.log1p(volume) / 10 + activity_score
            valid[symbol] = {
                "score": score,
                "reason": "candle_activity"
                if activity_score > change
                else "recent_price_movement"
                if change
                else "liquidity_bootstrap",
            }
            if anchor is None or now - anchor[0] >= 60:
                self.anchors[symbol] = [now, price]
        self.anchors = {s: a for s, a in self.anchors.items() if s in valid}
        ranking = sorted(valid, key=lambda s: (-valid[s]["score"], s))
        retained = self.allocate(ranking, self.selected, self.slots, valid, now)
        # Detail leases have their own hold; ordering cannot rotate the expensive feeds every poll.
        detail_rank = [s for s in ranking if s in retained]
        self.detail_state = self.allocate(detail_rank, self.detail_state, self.detail_slots, valid, now)
        detail = list(self.detail_state)
        self.selected = retained
        self.plan = {
            "schema_version": VERSION,
            "generated_at_ms": int(now * 1000),
            "expires_at_ms": int((now + self.hold) * 1000),
            "selected": list(retained),
            "detail": detail,
            "controls": [],
            "universe_size": len(valid),
            "slots": self.slots,
            "detail_slots": self.detail_slots,
            "reasons": {s: valid[s] for s in retained},
            "hold_seconds": self.hold,
        }
        if self.journal:
            with self.journal.db:
                self.journal.db.execute(
                    "INSERT OR REPLACE INTO scalper_settings VALUES('attention',?)",
                    (
                        json.dumps(
                            {
                                "selected_state": retained,
                                "anchors": self.anchors,
                                "detail": detail,
                                "detail_state": self.detail_state,
                            }
                        ),
                    ),
                )
                slot = int(now // 60)
                self.journal.db.execute("""CREATE TABLE IF NOT EXISTS market_overview_samples (
                    symbol TEXT NOT NULL, minute INTEGER NOT NULL, observed_at REAL NOT NULL,
                    payload TEXT NOT NULL, PRIMARY KEY(symbol,minute))""")
                self.journal.db.execute(
                    "CREATE INDEX IF NOT EXISTS market_overview_minute ON market_overview_samples(minute)"
                )
                self.journal.db.executemany(
                    "INSERT OR IGNORE INTO market_overview_samples VALUES(?,?,?,?)",
                    [(a["symbol"], slot, now, json.dumps(a)) for a in assets if a.get("symbol") in valid],
                )
                if self.last_prune_minute != slot:
                    self.journal.db.execute("DELETE FROM market_overview_samples WHERE minute<?", (slot - 10_080,))
                    self.last_prune_minute = slot
        return list(retained)


class Attention:
    def __new__(cls, *args, **kwargs):
        if (
            not args
            and not kwargs
            or isinstance(kwargs.get("policy"), AttentionPolicy)
            or args
            and isinstance(args[0], AttentionPolicy)
        ):
            return RelativeAttention(*args, **kwargs)
        return PulseAttention(*args, **kwargs)
