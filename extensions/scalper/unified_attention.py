"""One persistent relative allocator, independent of absolute liquidity."""

import json
import math
from collections import deque

from .relative_attention import Attention, AttentionPolicy


class UnifiedAttention:
    def __init__(self, journal, *, slots=20, detail_slots=6, hold_seconds=600):
        if (type(slots) is not int or type(detail_slots) is not int
                or not 1 <= detail_slots <= min(slots, 8) or slots > 20
                or type(hold_seconds) is not int or not 60 <= hold_seconds <= 3600):
            raise ValueError("attention requires 1..20 overview slots, 1..8 detail slots, and 60..3600s context hold")
        self.requested_hold_seconds = hold_seconds
        self.journal, self.slots = journal, slots
        self.engine = Attention(
            AttentionPolicy(version=6, slots=detail_slots, context_slots=min(2, detail_slots), probe_seconds=90,
                            context_seconds=max(330, hold_seconds))
        )
        self.activity, self.selected, self.feedback = {}, {}, {}
        self.last_at = 0
        self.plan = dict(schema_version="attention.v1", selected=[], detail=[], controls=[])
        self.archive = None
        if journal:
            saved = journal.db.execute(
                "SELECT value FROM scalper_settings WHERE name='unified_attention.v6'"
            ).fetchone()
            if saved:
                self.restore(json.loads(saved[0]))

    def restore(self, state):
        for name in ("previous", "scouts", "active"):
            setattr(self.engine, name, state.get(name, {}))
        for name in ("baselines", "movement_baselines"):
            setattr(
                self.engine,
                name,
                {s: deque(v, maxlen=self.engine.policy.baseline_size) for s, v in state.get(name, {}).items()},
            )

    def state(self):
        return {
            **{n: getattr(self.engine, n) for n in ("previous", "scouts", "active")},
            **{
                n: {s: list(v) for s, v in getattr(self.engine, n).items()} for n in ("baselines", "movement_baselines")
            },
        }

    def choose(self, assets, now):
        if not math.isfinite(now) or now <= self.last_at:
            return list(self.selected)
        self.last_at = now
        observations = {}
        target = int((now * 1000 - 5000) // 60000 * 60000 - 60000)
        for a in assets:
            symbol, price = a.get("symbol"), a.get("price", a.get("mark_price"))
            stamp = a.get("observed_at", now)
            if (
                not isinstance(symbol, str)
                or not symbol
                or type(price) not in (int, float)
                or not math.isfinite(price)
                or price <= 0
                or type(stamp) not in (int, float) or not math.isfinite(stamp) or not -5 <= now - stamp <= 20
            ):
                continue
            row = dict(price=price, volume_24h=0, price_source="last" if "price" in a else "mark")
            if symbol in self.feedback:
                row["detail"] = self.feedback[symbol]
            candle = self.activity.get(symbol, {})
            if candle.get("status") == "ready" and candle.get("start_ms") == target:
                ratios = [
                    v
                    for v in candle.get("ratios", {}).values()
                    if type(v) in (int, float) and math.isfinite(v) and v >= 0
                ]
                if ratios:
                    row["candle_ratio"] = max(ratios)
                elif candle.get("zero_reference") and candle.get("trade_count", 0) > 0:
                    row["candle_ratio"] = self.engine.policy.probe_ratio
            observations[symbol] = row
        dynamic = observations
        changes = self.engine.update(dynamic, now)
        if self.archive:
            self.archive.record("scout", dynamic, now)
            for change in changes:
                self.archive.record("assignment", change, now)
        ranking = sorted(observations, key=lambda s: (-self.engine.scouts.get(s, {}).get("score", 0), s))
        detail = list(self.engine.active)
        chosen = list(dict.fromkeys([*detail, *ranking]))[: self.slots]
        self.selected = {s: {} for s in chosen}
        self.plan = dict(
            schema_version="attention.v1",
            generated_at_ms=int(now * 1000),
            expires_at_ms=int((now + 60) * 1000),
            selected=chosen,
            detail=detail,
            controls=[],
            universe_size=len(observations),
            warming=sum(r["state"] == "warming" for r in self.engine.scouts.values()),
            slots=self.slots,
            detail_slots=self.engine.policy.slots,
            decision_archive=self.archive.report() if self.archive else None,
            policy_version=6,
            hold_seconds=self.engine.policy.context_seconds,
            requested_hold_seconds=self.requested_hold_seconds,
            active=list(self.engine.active.values()),
            reasons={s: self.engine.scouts.get(s, {}) for s in chosen},
            transitions=list(self.engine.transitions)[-50:],
        )
        if self.journal:
            with self.journal.db:
                self.journal.db.execute(
                    "INSERT OR REPLACE INTO scalper_settings VALUES(?,?)",
                    ("unified_attention.v6", json.dumps(self.state())),
                )
        return chosen

    def close(self):
        if self.archive:
            self.archive.close()
