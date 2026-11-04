"""Observed phase of a market impulse, independent of its strength and AI vote."""
from dataclasses import dataclass

from .activity import active_anomalies
from .analysis import DIRECTIONAL
from .profile import fresh

CONFIRM_NATR = 0.5
EXTENDED_NATR = 2.0
RETRACE_NATR = 0.5
MIN_OBSERVATIONS = 3
RATE_FLOOR = 0.7
MAX_GAP_SECONDS = 15

PHASES = {
    "unavailable": "Нет свежих данных",
    "untracked": "Начало не наблюдалось",
    "early": "Ранний импульс",
    "confirmed": "Движение подтвердилось",
    "extended": "Движение уже далеко ушло",
    "weakening": "Импульс слабеет",
}


@dataclass
class Impulse:
    direction: str
    started_at: float
    start_price: float
    natr_pct: float | None
    peak_rate: float | None
    last_observed_at: float
    last_progress_at: float
    peak_natr: float = 0.0
    last_progress_natr: float = 0.0
    was_confirmed: bool = False
    confirmed: int = 0
    slowing: int = 0


class PhaseTracker:
    def __init__(self):
        self.previous = {}
        self.impulses = {}

    def keep(self, keys):
        self.previous = {key: value for key, value in self.previous.items() if key in keys}
        self.impulses = {key: value for key, value in self.impulses.items() if key in keys}

    def observe(self, key, snapshot, native, seconds):
        now = snapshot["captured_at"]
        metrics = snapshot["metrics"]
        mark, ofi, rate, natr = (fresh(metrics, name) for name in ("price", "ofi", "trade_rate", "natr"))
        if mark is None or mark <= 0 or ofi is None:
            self.previous.pop(key, None)
            self.impulses.pop(key, None)
            return self.result("unavailable")
        direction = native["direction"]
        anomalies = active_anomalies(snapshot)
        previous = self.previous.get(key)
        self.previous[key] = (now, direction, anomalies)
        if previous is None or now - previous[0] > MAX_GAP_SECONDS:
            self.impulses.pop(key, None)
            return self.result("untracked")

        impulse = self.impulses.get(key)
        reversed_flow = impulse and direction in DIRECTIONAL and direction != impulse.direction
        if reversed_flow:
            self.impulses.pop(key)
            impulse = None
        new_impulse = direction in DIRECTIONAL and (
            direction != previous[1] or bool(anomalies - previous[2])
        )
        observed_at = metrics["price"].get("updated_at")
        if observed_at is None:
            return self.result("unavailable")
        if impulse is None and new_impulse:
            impulse = Impulse(direction, now, mark, natr if natr and natr > 0 else None,
                              rate, observed_at, now)
            self.impulses[key] = impulse
        if impulse is None:
            return self.result("untracked")
        if now - impulse.last_progress_at > seconds * 3:
            self.impulses.pop(key)
            return self.result("untracked")

        progress = self.progress(impulse, mark)
        if observed_at > impulse.last_observed_at:
            impulse.last_observed_at = observed_at
            if rate is not None:
                impulse.peak_rate = max(impulse.peak_rate or 0, rate)
            slowing = rate is not None and bool(impulse.peak_rate) and rate < RATE_FLOOR * impulse.peak_rate
            if progress is not None and progress >= impulse.last_progress_natr + 0.1:
                impulse.last_progress_at = now
                impulse.last_progress_natr = progress
            if progress is not None:
                impulse.peak_natr = max(progress, impulse.peak_natr)
            confirms = progress is not None and progress >= CONFIRM_NATR and direction == impulse.direction
            impulse.confirmed = impulse.confirmed + 1 if confirms and rate and not slowing else 0
            impulse.was_confirmed = impulse.was_confirmed or impulse.confirmed >= MIN_OBSERVATIONS
            fading = direction == "NEUTRAL" or slowing
            impulse.slowing = impulse.slowing + 1 if fading else 0

        phase = "early"
        if progress is not None:
            if (impulse.peak_natr >= CONFIRM_NATR and impulse.peak_natr - progress >= RETRACE_NATR
                    and impulse.slowing >= MIN_OBSERVATIONS):
                phase = "weakening"
            elif progress >= EXTENDED_NATR:
                phase = "extended"
            elif impulse.was_confirmed:
                phase = "confirmed"
        return self.result(phase, impulse, progress, now)

    @staticmethod
    def progress(impulse, mark):
        if impulse.natr_pct is None:
            return None
        signed_change = (mark / impulse.start_price - 1) * 100
        return signed_change * (1 if impulse.direction == "BULLISH" else -1) / impulse.natr_pct

    @staticmethod
    def result(phase, impulse=None, progress=None, now=None):
        result = {"phase": phase, "label": PHASES[phase]}
        if impulse:
            result.update(direction=impulse.direction, started_at=impulse.started_at,
                          start_price=impulse.start_price, natr_at_start=impulse.natr_pct,
                          distance_natr=progress,
                          retracement_natr=max(0.0, impulse.peak_natr - progress) if progress is not None else None,
                          age_seconds=int(now - impulse.started_at))
        return result
