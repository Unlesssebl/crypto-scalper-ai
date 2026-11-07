"""Instrument profiles from regularly sampled, quality-checked market windows."""

import math
import sqlite3
from collections import defaultdict
from contextlib import closing
from statistics import median

DAY = 86400
PROFILE_DAYS = (7, 30)
MIN_SAMPLES = 20
FIELDS = ("trades", "natr", "spread_bps", "return_pct", "imbalance")


def fresh(metrics, name):
    item = metrics.get(name)
    if not item or item["quality"] != "ok":
        return None
    value = item["value"]
    return value if isinstance(value, (int, float)) and math.isfinite(value) else None


def sample_window(snapshot, seconds):
    now = snapshot["captured_at"]
    slot = int(now // seconds) * seconds
    if now - slot > 10:
        return None
    m = snapshot["metrics"]
    trades, mark, spread = (fresh(m, key) for key in ("trade_count", "price", "spread"))
    start, end = (fresh(m, key) for key in ("first_trade_price", "last_trade_price"))
    spread_bps = spread * 10000 / mark if mark and spread is not None else None
    change = (end / start - 1) * 100 if start and end is not None else None
    values = (trades, fresh(m, "natr"), spread_bps, change, fresh(m, "imbalance"))
    if not any(value is not None for value in values):
        return None
    return snapshot["symbol"], snapshot["window"], slot, *values


def calculate_profiles(path, now, symbols):
    if not symbols:
        return {}
    groups = defaultdict(list)
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
        placeholders = ",".join("?" for _ in symbols)
        for row in db.execute(
            f"SELECT * FROM instrument_samples WHERE slot BETWEEN ? AND ? AND symbol IN ({placeholders})",
            (now - 30 * DAY, now, *symbols),
        ):
            groups[row[0], row[1]].append(row[2:])
    profiles = {}
    for key, rows in groups.items():
        seconds = {"1m": 60, "5m": 300}.get(key[1], 300)
        # Exclude any historical rolling sample overlapping the current interval.
        rows = [row for row in rows if row[0] <= now - seconds]
        if not rows:
            continue
        periods = {}
        for days in PROFILE_DAYS:
            recent = [row for row in rows if row[0] >= now - days * DAY]
            metrics = {}
            for index, name in enumerate(FIELDS, 1):
                values = sorted(row[index] for row in recent if row[index] is not None)
                if values:
                    metrics[name] = {
                        "samples": len(values),
                        "median": median(values) if len(values) >= MIN_SAMPLES else None,
                        "p90": values[math.ceil(0.9 * len(values)) - 1] if len(values) >= MIN_SAMPLES else None,
                    }
            periods[days] = {"windows": len(recent), "metrics": metrics}
        profiles[key] = {
            "updated_at": max(row[0] for row in rows),
            "calculated_at": now,
            "baseline_policy": "strictly_prior_rolling_intervals.v2",
            "periods": periods,
        }
    return profiles


def profile_context(profile):
    if not profile:
        return None
    periods = {}
    for days, period in profile["periods"].items():
        metrics = {name: data for name, data in period["metrics"].items() if data["median"] is not None}
        if metrics:
            periods[days] = metrics
    return {"updated_at": profile["updated_at"], "periods": periods} if periods else None


def situation_facts(snapshot, profile=None):
    metrics = snapshot["metrics"]
    facts = []
    trades, rate, relative = (fresh(metrics, name) for name in ("trade_count", "trade_rate", "activity_ratio"))
    if trades is not None:
        tape = f"Сделок {trades:g}"
        if rate is not None:
            tape += f" · {rate:.1f}/с"
        if relative is not None:
            tape += f" · {relative:.1f}× обычного темпа"
        facts.append(tape)
    ofi = fresh(metrics, "ofi")
    if ofi is not None:
        flow = "преобладают покупки" if ofi > 0.15 else "преобладают продажи" if ofi < -0.15 else "близок к балансу"
        facts.append(f"Поток: {flow}")
    start, end = (fresh(metrics, key) for key in ("first_trade_price", "last_trade_price"))
    if start and end is not None:
        facts.append(f"Цена за окно: {(end / start - 1) * 100:+.2f}%")
    spread, mark = (fresh(metrics, key) for key in ("spread", "price"))
    if mark and spread is not None:
        facts.append(f"Спред: {100 * spread / mark:.3f}%")
    if profile:
        baseline = profile["periods"].get(7, {}).get("metrics", {})
        typical = baseline.get("trades", {})
        if trades is not None and typical.get("median"):
            facts.append(f"Темп к медиане за 7 дней: {trades / typical['median']:.1f}× (n={typical['samples']})")
    return facts
