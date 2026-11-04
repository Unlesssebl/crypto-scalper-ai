"""Run fixed detector alternatives without publishing alerts."""
from .activity import active_anomalies, finding_strength
from .analysis import DIRECTIONAL
from .audit import STRONG_RANK
from .profile import fresh

BASELINE = "Текущие пороги"
MINUTE_CONTROL = "1m · Контроль для строгого порога"
STRICT_MINUTE = "1m · Цена от 0,25% и 1,5 NATR"
MOVE_THRESHOLD_PCT = 0.5
VARIANTS = {
    BASELINE: {},
    "Лента от 1,5×": {"tape_ratio": 1.5},
    "Цена от 0,15% и 1,25 NATR": {"price_floor": 0.15, "price_natr_multiple": 1.25},
    MINUTE_CONTROL: {},
    STRICT_MINUTE: {"price_floor": 0.25, "price_natr_multiple": 1.5},
}
SHADOW_COOLDOWN = 60


class ShadowDetector:
    def __init__(self):
        self.states = {}
        self.last_hit = {}

    def keep(self, rows):
        self.states = {key: value for key, value in self.states.items() if key[:2] in rows}
        self.last_hit = {key: value for key, value in self.last_hit.items() if key[:2] in rows}

    def evaluate(self, key, snapshot, native):
        now = snapshot["captured_at"]
        metrics = snapshot["metrics"]
        target = fresh(metrics, "natr")
        direction = native["direction"]
        results = []
        for variant, rules in VARIANTS.items():
            if variant in (MINUTE_CONTROL, STRICT_MINUTE) and key[1] != "1m":
                continue
            variant_key = (*key, variant)
            rank = finding_strength(snapshot, native, **rules)["rank"]
            anomalies = active_anomalies(snapshot, tape_ratio=rules.get("tape_ratio", 2.0))
            previous, was_eligible, previous_anomalies = self.states.get(variant_key, (None, False, frozenset()))
            eligible = rank >= STRONG_RANK and bool(target and target > 0)
            changed = (previous in DIRECTIONAL and direction != previous) or bool(anomalies - previous_anomalies)
            self.states[variant_key] = (direction, eligible, anomalies)
            if (eligible and (not was_eligible or changed)
                    and now - self.last_hit.get(variant_key, float("-inf")) >= SHADOW_COOLDOWN):
                self.last_hit[variant_key] = now
                results.append((variant, key[0], key[1], now, rank, target, snapshot))
        return results
