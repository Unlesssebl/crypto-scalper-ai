"""Deterministic exploratory models. Scores are not calibrated trading signals."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from statistics import median

MINUTE = 60_000
VERSION = "pulse-models.v1"
USABLE = {"observed_no_detected_gap", "reconciled"}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class Parameters:
    lookback_minutes: int = 10_080
    min_samples: int = 2_000
    min_coverage: float = 0.99
    quantile: float = 0.99
    min_tail: int = 20
    local_minutes: int = 14
    local_ratio: float = 3.0
    mad_threshold: float = 6.0
    cusum_k: float = 0.5
    cusum_h: float = 5.0
    cooldown_minutes: int = 5

    def __post_init__(self):
        for name in ("lookback_minutes", "min_samples", "min_tail", "local_minutes", "cooldown_minutes"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(name)
        if self.min_samples > self.lookback_minutes:
            raise ValueError("min_samples exceeds lookback")
        if not 0 < self.quantile < 1 or not 0 < self.min_coverage <= 1:
            raise ValueError("quantile/coverage")
        for name in ("local_ratio", "mad_threshold", "cusum_k", "cusum_h"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(name)


def stream(row):
    return tuple(row.get(k) for k in ("venue", "native_market_id", "channel", "aggregation_kind"))


def specification(row):
    versions = row.get("coverage", {}).get("instrument_spec_versions", [])
    return versions[0] if isinstance(versions, list) and len(versions) == 1 else None


def value(row, metric):
    if metric == "execution_count":
        raw = row.get("execution_count")
        if type(raw) is not int or raw < 0:
            raise ValueError("invalid count")
        return float(raw)
    if metric != "quote_volume":
        raise ValueError("unknown metric")
    total = Decimal(0)
    for name in ("buy_quote", "sell_quote"):
        raw = row.get(name)
        # A missing side is zero only when normalization is known and trades exist.
        if raw is None and not row.get("event_count", row.get("execution_count", 0)):
            raise ValueError("unknown empty-window amount")
        part = Decimal(str(raw)) if raw is not None else Decimal(0)
        if not part.is_finite() or part < 0:
            raise ValueError("invalid amount")
        total += part
    result = float(total)
    if not math.isfinite(result):
        raise ValueError("amount overflow")
    return result


def eligible(row, metric):
    try:
        return (row.get("quality") in USABLE and row.get("provisional") is False
                and row.get("aggregation_kind") == "execution" and specification(row) is not None
                and not row.get("coverage", {}).get("notional_blocked_events", 0)
                and row["window_start_ms"] % MINUTE == 0
                and row["window_end_ms"] == row["window_start_ms"] + MINUTE
                and value(row, metric) >= 0)
    except (ValueError, TypeError, KeyError, InvalidOperation, OverflowError):
        return False


def latest_rows(rows):
    """One revision per stream/minute; conflicting equal revisions fail closed."""
    found = {}
    for row in rows:
        key = (*stream(row), row["window_start_ms"])
        old = found.get(key)
        if old is None or row["version"] > old["version"]:
            found[key] = row
        elif row["version"] == old["version"] and digest(row) != digest(old):
            raise ValueError("conflicting aggregate revision")
    return sorted(found.values(), key=lambda r: (stream(r), r["window_start_ms"]))


def fit(rows, target, metric="execution_count", params=None):
    """Fit only on strictly preceding closed minutes of the same specification."""
    p = params or Parameters()
    end = target["window_start_ms"]
    start = end - p.lookback_minutes * MINUTE
    past = [r for r in latest_rows(rows) if stream(r) == stream(target)
            and start <= r["window_start_ms"] < end]
    clean = [r for r in past if eligible(r, metric) and specification(r) == specification(target)]
    # The requested horizon, not the available prefix, is the coverage denominator.
    coverage = len(clean) / p.lookback_minutes
    values = sorted(value(r, metric) for r in clean)
    tail = math.floor(len(values) * (1 - p.quantile) + 1e-9)
    status = ("unknown_specification" if specification(target) is None else
              "insufficient_history" if len(values) < p.min_samples else
              "insufficient_coverage" if coverage < p.min_coverage else
              "insufficient_tail" if tail < p.min_tail else "exploratory")
    logs = [math.log1p(v) for v in values]
    center = median(logs) if logs else None
    scale = median([abs(v - center) for v in logs]) * 1.4826 if logs else None
    profile = {
        "algorithm_version": VERSION, "metric": metric, "stream": list(stream(target)),
        "specification": specification(target), "parameters": asdict(p),
        "from_ms": start, "to_ms": end, "samples": len(values), "coverage": coverage,
        "tail_samples": tail, "status": status, "calibrated": False,
        "median": median(values) if values else None,
        "quantile_threshold": values[max(0, math.ceil(p.quantile * len(values)) - 1)] if values else None,
        "log_median": center, "log_mad_scale": scale,
        "members": [[r["window_start_ms"], r["version"], digest(r)] for r in clean],
        "values": [value(r, metric) for r in clean],
    }
    profile["baseline_version"] = digest(profile)
    return profile


def score(profile, target, past=(), *, cusum=0.0, contiguous=True):
    """Score first, then the caller may update its baseline/CUSUM state."""
    p = Parameters(**profile["parameters"])
    metric = profile["metric"]
    result = {"status": "unavailable", "local_ratio": None, "mad_score": None,
              "cusum": 0.0, "candidates": {k: False for k in ("local", "quantile", "mad", "cusum")}}
    if (not eligible(target, metric) or list(stream(target)) != profile["stream"]
            or specification(target) != profile["specification"]
            or target["window_start_ms"] < profile["to_ms"]):
        return result
    result["status"] = profile["status"]
    x = value(target, metric)
    result["value"] = x
    end = target["window_start_ms"]
    recent = {r["window_start_ms"]: r for r in latest_rows(past)
              if stream(r) == stream(target) and specification(r) == specification(target)
              and eligible(r, metric)}
    local = [recent.get(end - i * MINUTE) for i in range(1, p.local_minutes + 1)]
    if all(local):
        base = median(value(r, metric) for r in local)
        result["local_status"] = "ready" if base else "zero_median"
        if base:
            result["local_ratio"] = x / base
            result["candidates"]["local"] = x / base >= p.local_ratio
    else:
        result["local_status"] = "insufficient_contiguous_minutes"
    # Insufficient history never becomes a rarity claim, including zero MAD.
    if profile["status"] != "exploratory":
        return result
    result["candidates"]["quantile"] = x > profile["quantile_threshold"]
    result["empirical_percentile"] = sum(v <= x for v in profile["values"]) / profile["samples"]
    scale = profile["log_mad_scale"]
    if not scale:
        result["mad_status"] = "zero_mad"
        return result
    z = (math.log1p(x) - profile["log_median"]) / scale
    result["mad_score"] = z
    result["mad_status"] = "ready"
    result["cusum"] = max(0.0, (cusum if contiguous else 0.0) + z - p.cusum_k)
    result["candidates"]["mad"] = z >= p.mad_threshold
    result["candidates"]["cusum"] = result["cusum"] >= p.cusum_h
    return result


def research(rows, start_ms, end_ms, params=None):
    """Frozen chronological 70/30 holdout; no threshold selection on holdout."""
    p = params or Parameters()
    if start_ms % MINUTE or end_ms % MINUTE or end_ms <= start_ms:
        raise ValueError("research boundaries must be ordered UTC minutes")
    split = start_ms + max(1, int((end_ms - start_ms) // MINUTE * .7)) * MINUTE
    if split >= end_ms:
        raise ValueError("need at least two minutes")
    rows = [r for r in latest_rows(rows) if start_ms <= r["window_start_ms"] < end_ms]
    cohorts = {(*stream(r), specification(r) or "unknown") for r in rows}
    for symbol in ("BTC", "XRP"):
        for key in (("hyperliquid", symbol, "trades", "execution"),
                    ("okx", symbol + "-USDT-SWAP", "trades-all", "execution")):
            if not any(c[:4] == key for c in cohorts):
                cohorts.add((*key, "unknown"))
    cohorts = sorted(cohorts)
    reports = []
    for cohort in cohorts:
        subset = [r for r in rows if (*stream(r), specification(r) or "unknown") == cohort]
        for metric in ("execution_count", "quote_volume"):
            template = subset[-1] if subset else dict(zip(
                ("venue", "native_market_id", "channel", "aggregation_kind"), cohort[:4], strict=True))
            target = {**template, "window_start_ms": split}
            profile = fit(subset, target, metric, p)
            state, previous = 0.0, None
            counts = {k: {"candidates": 0, "episodes": 0, "suppressed": 0, "last": None}
                      for k in ("local", "quantile", "mad", "cusum")}
            scores, history = [], [r for r in subset if r["window_start_ms"] < split]
            for row in (r for r in subset if r["window_start_ms"] >= split):
                t = row["window_start_ms"]
                assessment = score(profile, row, history[-p.local_minutes:], cusum=state,
                                   contiguous=previous is not None and t == previous + MINUTE)
                state, previous = assessment["cusum"], t
                for method, candidate in assessment["candidates"].items():
                    count = counts[method]
                    if candidate:
                        count["candidates"] += 1
                        suppressed = count["last"] is not None and t - count["last"] < p.cooldown_minutes * MINUTE
                        count["suppressed" if suppressed else "episodes"] += 1
                        if not suppressed:
                            count["last"] = t
                    if method == "cusum" and candidate:
                        state = 0.0  # New accumulation after every threshold crossing.
                scores.append({"at_ms": t, "version": row["version"], **assessment})
                history.append(row)
            expected = (end_ms - split) // MINUTE
            usable = sum(eligible(r, metric) for r in subset if r["window_start_ms"] >= split)
            for count in counts.values():
                count.pop("last")
                count["episodes_per_calendar_day"] = count["episodes"] * 1440 / expected
            segments = []
            for i in range(3):
                lo = split + (end_ms - split) // (3 * MINUTE) * MINUTE * i
                hi = end_ms if i == 2 else split + (end_ms - split) // (3 * MINUTE) * MINUTE * (i + 1)
                selected = [s for s in scores if lo <= s["at_ms"] < hi]
                segments.append({"from_ms": lo, "to_ms": hi,
                                 "expected_minutes": (hi - lo) // MINUTE,
                                 "usable_minutes": sum(s["status"] != "unavailable" for s in selected),
                                 "candidates": {k: sum(s["candidates"][k] for s in selected) for k in counts}})
            reports.append({"cohort": list(cohort), "metric": metric, "profile": profile,
                            "holdout_expected_minutes": expected, "holdout_usable_minutes": usable,
                            "holdout_coverage": usable / expected, "methods": counts, "scores": scores,
                            "holdout_segments": segments})
    return {"version": VERSION, "from_ms": start_ms, "to_ms": end_ms, "split_ms": split,
            "parameters": asdict(p), "reports": reports, "live_approved": False,
            "limitations": ["Exploratory fixed thresholds; no profitability claim",
                            "No seasonal model; adjacent minutes are dependent",
                            "Latest aggregate revisions, not a point-in-time arrival replay",
                            "Live approval requires P3 and independent threshold review"]}
