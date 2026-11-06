"""Deterministic allocation of detailed observation; no model or trading decisions."""

import math
from collections import deque
from dataclasses import asdict, dataclass
from statistics import median


@dataclass(frozen=True)
class AttentionPolicy:
    version: int = 3
    slots: int = 4
    baseline_samples: int = 12
    baseline_size: int = 120
    min_activity_usd: float = 0  # Optional operator floor; never a ranking input.
    probe_ratio: float = 3
    focus_ratio: float = 6
    release_ratio: float = 1.5
    severe_move_ratio: float = 6
    min_probe_seconds: float = 15
    probe_seconds: float = 45
    min_focus_seconds: float = 60
    cooling_seconds: float = 60
    stale_seconds: float = 30
    context_slots: int = 2
    context_seconds: float = 600

    def __post_init__(self):
        if type(self.version) is not int or self.version not in (1, 2, 3, 4):
            raise ValueError("unsupported attention policy version")
        if any(
            isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
            for v in asdict(self).values()
        ):
            raise ValueError("finite numeric policy fields required")
        if any(type(v) is not int for v in (self.slots, self.context_slots, self.baseline_samples, self.baseline_size)):
            raise ValueError("integer capacities required")
        if not 1 <= self.slots <= 8:
            raise ValueError("attention slots must be 1..8")
        if not 1 <= self.context_slots <= 8 or self.context_seconds < 330:
            raise ValueError("context requires at least 330 seconds and 1..8 slots")
        if not 3 <= self.baseline_samples <= self.baseline_size:
            raise ValueError("invalid baseline size")
        if not 0 < self.release_ratio < self.probe_ratio < self.focus_ratio:
            raise ValueError("attention thresholds must have hysteresis")
        if self.min_activity_usd < 0 or self.severe_move_ratio <= self.probe_ratio:
            raise ValueError("invalid optional floor or movement threshold")
        if min(self.min_probe_seconds, self.min_focus_seconds, self.cooling_seconds, self.stale_seconds) <= 0:
            raise ValueError("positive attention thresholds required")
        if self.probe_seconds < self.min_probe_seconds:
            raise ValueError("probe deadline precedes minimum observation")


class Attention:
    def __init__(self, policy=None):
        self.policy = policy or AttentionPolicy()
        self.previous = {}
        self.baselines = {}
        self.movement_baselines = {}
        self.scouts = {}
        self.active = {}
        self.transitions = deque(maxlen=500)
        self.denied = 0
        self.denied_checks = 0
        self.denied_symbols = set()
        self.denied_now = set()
        self.activations = 0
        self.last_at = None

    def _transition(self, symbol, state, reason, now, score=0):
        old = self.active.get(symbol, {}).get("state", "scout")
        row = dict(symbol=symbol, previous=old, state=state, reason=reason, at=now, score=score)
        self.transitions.append(row)
        if state == "scout":
            self.active.pop(symbol, None)
        else:
            prior = self.active.get(symbol, {})
            self.active[symbol] = dict(
                symbol=symbol,
                state=state,
                since=now,
                assigned_at=prior.get("assigned_at", now),
                score=score,
                reason=reason,
            )
            if "context_until" in prior:
                self.active[symbol]["context_until"] = prior["context_until"]
        return row

    def _reserve_context(self, now):
        if self.policy.version < 3:
            return
        capacity = min(self.policy.context_slots, max(1, self.policy.slots - 1))
        protected = sum(a.get("context_until", 0) > now for a in self.active.values())
        eligible = sorted(
            (a for a in self.active.values() if a["state"] == "focus" and "context_until" not in a),
            key=lambda a: (-a["score"], a["assigned_at"], a["symbol"]),
        )
        for row in eligible[: max(0, capacity - protected)]:
            row["context_until"] = now + self.policy.context_seconds

    def update(self, observations, now):
        """Observations are current price and rolling day volume, not a full tape.

        A positive change of rolling 24h volume is a LOWER BOUND on interval
        turnover, not the exact volume. Negative changes invalidate that sample.
        The trigger is deliberately labelled as a proxy everywhere downstream.
        """
        if not math.isfinite(now):
            raise ValueError("finite scout time required")
        if self.last_at is not None and now <= self.last_at:
            raise ValueError("scout time must increase")
        self.last_at = now
        changes = []
        p = self.policy
        for symbol, observation in observations.items():
            price, volume = observation["price"], observation["volume_24h"]
            if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (price, volume)):
                continue
            if price <= 0 or volume < 0:
                continue
            previous = self.previous.get(symbol)
            self.previous[symbol] = (now, price, volume)
            baseline = self.baselines.setdefault(symbol, deque(maxlen=p.baseline_size))
            movements = self.movement_baselines.setdefault(symbol, deque(maxlen=p.baseline_size))
            valid = previous is not None and 0 < now - previous[0] <= p.stale_seconds
            if not valid:
                baseline.clear()
                movements.clear()
                rate, move_rate = None, None
            else:
                elapsed = now - previous[0]
                rate = (volume - previous[2]) / elapsed if volume >= previous[2] else None
                move_rate = abs(price / previous[1] - 1) * 10000 / elapsed
                rate = rate if rate is not None and math.isfinite(rate) else None
                move_rate = move_rate if math.isfinite(move_rate) else None
            move = move_rate * (now - previous[0]) if move_rate is not None else 0
            warm = len(baseline) >= p.baseline_samples
            typical = median(baseline) if warm else None

            def relative(value, reference):
                if value is None or reference is None:
                    return None
                # First activity after an observed quiet baseline gets a probe,
                # never infinite priority or a hidden absolute-dollar denominator.
                return min(value / reference, 1e6) if reference > 0 else p.probe_ratio if value > 0 else 0

            ratio = relative(rate, typical)
            move_ratio = relative(move_rate, median(movements) if len(movements) >= p.baseline_samples else None)
            interval_usd = rate * (now - previous[0]) if rate is not None else None
            meaningful = interval_usd is not None and interval_usd > 0 and interval_usd >= p.min_activity_usd
            score = max((ratio or 0) if meaningful else 0, move_ratio or 0)
            trigger = (
                meaningful
                and ratio is not None
                and ratio >= p.probe_ratio
                or move_ratio is not None
                and move_ratio >= p.probe_ratio
            )
            severe = (
                meaningful
                and ratio is not None
                and ratio >= p.focus_ratio
                or move_ratio is not None
                and move_ratio >= p.severe_move_ratio
            )
            if p.version == 4:
                external = observation.get("candle_ratio")
                if isinstance(external, (int, float)) and math.isfinite(external) and external >= 0:
                    score = max(score, min(external, 1000))
                    trigger = trigger or external >= p.probe_ratio
                    severe = severe or external >= p.focus_ratio
            ready = warm or len(movements) >= p.baseline_samples
            self.scouts[symbol] = dict(
                symbol=symbol,
                at=now,
                price=price,
                state="ready" if ready else "warming",
                score=score,
                trigger=trigger or severe,
                severe=severe,
                activity_ratio=ratio,
                activity_usd_lower_bound=interval_usd,
                move_bps=move,
                movement_ratio=move_ratio,
                source="relative_price_and_candle.v4" if p.version == 4 else "rolling_24h_volume_change_proxy",
                baseline_samples=len(baseline),
            )
            # Compare to the old baseline before adding the current observation.
            if rate is not None:
                baseline.append(rate)
            if move_rate is not None:
                movements.append(move_rate)
        for symbol, current in list(self.active.items()):
            scout = self.scouts.get(symbol, {})
            if now - scout.get("at", 0) > p.stale_seconds:
                changes.append(self._transition(symbol, "scout", "scout_stale", now))
                continue
            score, age = scout.get("score", 0), now - current["since"]
            current["score"] = score
            if current.get("context_until", 0) > now:
                continue
            if current["state"] == "probe":
                if age >= p.min_probe_seconds and scout.get("trigger"):
                    changes.append(self._transition(symbol, "focus", "sustained_activity", now, score))
                elif age >= p.probe_seconds:
                    changes.append(self._transition(symbol, "cooling", "probe_not_confirmed", now, score))
            elif current["state"] == "focus" and age >= p.min_focus_seconds and score < p.release_ratio:
                changes.append(self._transition(symbol, "cooling", "activity_faded", now, score))
            elif current["state"] == "cooling":
                if scout.get("trigger"):
                    changes.append(self._transition(symbol, "focus", "activity_resumed", now, score))
                elif age >= p.cooling_seconds:
                    changes.append(self._transition(symbol, "scout", "observation_finished", now, score))
        self._reserve_context(now)
        candidates = sorted(
            (s for s in self.scouts.values() if s["trigger"] and s["at"] == now and s["symbol"] not in self.active),
            key=lambda s: (-s["score"], s["symbol"]),
        )
        denied_now = set()
        for s in candidates:
            if len(self.active) >= p.slots:

                def replaceable(a):
                    if a.get("context_until", 0) > now:
                        return False
                    if a["state"] == "probe":
                        mature = now - a["since"] >= p.min_probe_seconds
                    else:
                        mature = p.version >= 2 and now - a["assigned_at"] >= p.min_focus_seconds
                    cooling = a["state"] == "cooling" and p.version >= 2
                    threshold = p.probe_ratio if cooling else p.focus_ratio
                    enough = s["score"] >= threshold if p.version >= 2 else s["score"] > threshold
                    return mature and (s["severe"] or cooling) and enough and s["score"] > a["score"] * 2

                weak = min(
                    (a for a in self.active.values() if replaceable(a)),
                    key=lambda a: (a["score"], a["symbol"]),
                    default=None,
                )
                if weak:
                    changes.append(self._transition(weak["symbol"], "scout", "preempted", now))
                else:
                    self.denied_checks += 1
                    self.denied_symbols.add(s["symbol"])
                    denied_now.add(s["symbol"])
                    if p.version == 1 or s["symbol"] not in self.denied_now:
                        self.denied += 1
                        changes.append(
                            dict(
                                symbol=s["symbol"],
                                state="scout",
                                reason="capacity",
                                at=now,
                                score=s["score"],
                                previous="scout",
                            )
                        )
                    continue
            state = "focus" if s["severe"] else "probe"
            reason = (
                "relative_price_activity"
                if (s["movement_ratio"] or 0) >= p.probe_ratio
                else "relative_activity_proxy_burst"
            )
            changes.append(self._transition(s["symbol"], state, reason, now, s["score"]))
            self.activations += 1
        self._reserve_context(now)
        self.denied_now = denied_now
        # A bounded universe history: delisted/disappeared instruments cannot grow RAM forever.
        for symbol, prior in list(self.previous.items()):
            if now - prior[0] > 3600 and symbol not in self.active:
                self.previous.pop(symbol, None)
                self.baselines.pop(symbol, None)
                self.movement_baselines.pop(symbol, None)
                self.scouts.pop(symbol, None)
        return changes

    def report(self):
        return dict(
            enabled=True,
            policy=asdict(self.policy),
            active=list(self.active.values()),
            scout_count=len(self.scouts),
            warming=sum(s["state"] == "warming" for s in self.scouts.values()),
            activations=self.activations,
            capacity_rejections=self.denied,
            capacity_checks=self.denied_checks,
            capacity_rejected_symbols=len(self.denied_symbols),
            limitation="Activity uses positive rolling 24h volume changes, not full-market trades.",
        )
