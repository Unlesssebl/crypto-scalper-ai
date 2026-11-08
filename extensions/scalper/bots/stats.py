"""Per-bot aggregation with explicit uncertainty.

No PnL is computed here.  A directional bot reports signed basis points; an
absolute bot reports absolute movement.  Every primary metric carries a block
bootstrap interval so a handful of episodes cannot masquerade as a result.
"""

from __future__ import annotations

import math
import random
import sqlite3
import statistics

BLOCK_SECONDS = 900
BOOTSTRAP_ITERATIONS = 1200


def wilson_interval(successes: int, n: int, z: float = 1.96):
    if n <= 0:
        return None
    p = successes / n
    denominator = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denominator
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominator
    return max(0.0, centre - half), min(1.0, centre + half)


def _blocks(values, times):
    grouped: dict[int, list[float]] = {}
    for value, stamp in zip(values, times):
        grouped.setdefault(int(stamp // BLOCK_SECONDS), []).append(value)
    return list(grouped.values())


def _bootstrap_means(values, times, *, seed: int = 17):
    blocks = _blocks(values, times)
    if len(blocks) < 3:
        return None
    rng = random.Random(seed)
    means = []
    for _ in range(BOOTSTRAP_ITERATIONS):
        sample = []
        for _ in blocks:
            sample.extend(blocks[rng.randrange(len(blocks))])
        means.append(statistics.fmean(sample))
    return means


def _quantile(sorted_values, q):
    if not sorted_values:
        return None
    position = (len(sorted_values) - 1) * q
    low = int(math.floor(position))
    high = int(math.ceil(position))
    if low == high:
        return sorted_values[low]
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (position - low)


def mean_interval(values, times, *, seed: int = 17):
    means = _bootstrap_means(values, times, seed=seed)
    if not means:
        return None
    means.sort()
    return _quantile(means, 0.025), _quantile(means, 0.975)


def mean_test(values, times, *, seed: int = 17):
    """Two-sided bootstrap p-value for a mean different from zero."""
    means = _bootstrap_means(values, times, seed=seed)
    if not means:
        return None
    nonpositive = sum(1 for value in means if value <= 0) / len(means)
    nonnegative = sum(1 for value in means if value >= 0) / len(means)
    return min(1.0, 2 * min(nonpositive, nonnegative))


def difference_interval(strong, weak, *, seed: int = 17):
    """Bootstrap the difference of two independent block samples."""
    strong_means = _bootstrap_means(
        [value for value, _ in strong], [stamp for _, stamp in strong], seed=seed
    )
    weak_means = _bootstrap_means(
        [value for value, _ in weak], [stamp for _, stamp in weak], seed=seed + 1
    )
    if not strong_means or not weak_means:
        return None
    count = min(len(strong_means), len(weak_means))
    differences = [strong_means[i] - weak_means[i] for i in range(count)]
    differences.sort()
    nonpositive = sum(1 for value in differences if value <= 0) / count
    nonnegative = sum(1 for value in differences if value >= 0) / count
    return _quantile(differences, 0.025), _quantile(differences, 0.975), min(
        1.0, 2 * min(nonpositive, nonnegative)
    )


def benjamini_hochberg(pvalues: list[float], *, q: float = 0.1) -> tuple[list[bool], list[float]]:
    """Benjamini-Hochberg FDR across the declared primary tests.

    Returns (rejected, adjusted).  The caller decides which tests are primary;
    exploratory cells must not enter this list.
    """
    if not pvalues:
        return [], []
    order = sorted(range(len(pvalues)), key=lambda index: pvalues[index])
    count = len(pvalues)
    adjusted = [1.0] * count
    previous = 1.0
    for position in range(count - 1, -1, -1):
        index = order[position]
        value = min(previous, pvalues[index] * count / (position + 1))
        adjusted[index] = value
        previous = value
    rejected = [value <= q for value in adjusted]
    return rejected, adjusted


def status_for(n: int) -> str:
    if n <= 0:
        return "нет наблюдений"
    if n < 30:
        return "мало данных"
    if n < 100:
        return "предварительно"
    return "достаточно данных"


def status_for_bot(observations: int, scored: int) -> str:
    if observations <= 0:
        return "нет наблюдений"
    if scored <= 0:
        return "ожидает исходов"
    return status_for(scored)


def _summary(values, times):
    if not values:
        return {
            "n": 0,
            "mean_bps": None,
            "median_bps": None,
            "ci": None,
            "p": None,
        }
    return {
        "n": len(values),
        "mean_bps": statistics.fmean(values),
        "median_bps": statistics.median(values),
        "ci": mean_interval(values, times),
        "p": mean_test(values, times),
    }


def _control_means(db: sqlite3.Connection, control_set_ids: list[str]) -> dict[tuple[str, int], float]:
    result: dict[tuple[str, int], float] = {}
    for start in range(0, len(control_set_ids), 900):
        chunk = control_set_ids[start:start + 900]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"SELECT control_set_id,horizon_s,AVG(move_bps) AS mean,COUNT(*) AS n "
            f"FROM bot_control_points WHERE status='scored' AND control_set_id IN ({placeholders}) "
            "GROUP BY control_set_id,horizon_s HAVING COUNT(*) >= 3",
            tuple(chunk),
        ).fetchall()
        result.update(
            {(row["control_set_id"], row["horizon_s"]): float(row["mean"]) for row in rows if row["n"] >= 1}
        )
    return result


def summarize(db: sqlite3.Connection, spec, *, implemented: bool = True, source: str = "live") -> dict:
    if source not in {"live", "replay"}:
        raise ValueError("source must be live or replay")
    observations = {
        row["id"]: {**dict(row), "outcomes": {}}
        for row in db.execute(
            "SELECT * FROM bot_observations WHERE bot_id=? AND version=? AND source=? ORDER BY decided_at",
            (spec.id, spec.version, source),
        ).fetchall()
    }
    if observations:
        for row in db.execute(
            "SELECT x.* FROM bot_outcomes x JOIN bot_observations o ON o.id=x.observation_id "
            "WHERE o.bot_id=? AND o.version=? AND o.source=?",
            (spec.id, spec.version, source),
        ).fetchall():
            observation = observations.get(row["observation_id"])
            if observation is not None:
                observation["outcomes"][int(row["horizon_s"])] = dict(row)

    control_ids = sorted({obs["control_set_id"] for obs in observations.values() if obs["control_set_id"]})
    control_means = _control_means(db, control_ids)

    horizons = {}
    horizon_values = sorted({60, 300, 900, spec.primary_horizon_s})
    for horizon in horizon_values:
        scored = []
        control_excess = []
        control_scored = 0
        for observation in observations.values():
            outcome = observation["outcomes"].get(horizon)
            if outcome is None or outcome.get("status") != "scored" or outcome.get("move_bps") is None:
                continue
            scored.append((float(outcome["move_bps"]), float(observation["decided_at"])))
            control = control_means.get((observation["control_set_id"], horizon))
            if control is not None:
                control_scored += 1
                control_excess.append((float(outcome["move_bps"]) - control, float(observation["decided_at"])))

        values = [value for value, _ in scored]
        times = [stamp for _, stamp in scored]
        summary = _summary(values, times)
        hits = sum(1 for value in values if value > 0)
        summary.update(
            {
                "scored": len(scored),
                "coverage": len(scored) / len(observations) if observations else 0.0,
                "hit_rate": hits / len(scored) if scored and spec.mode == "directional" else None,
                "hit_ci": wilson_interval(hits, len(scored)) if scored and spec.mode == "directional" else None,
                "mae_median_bps": statistics.median(
                    [float(obs["outcomes"][horizon]["mae_bps"]) for obs in observations.values()
                     if obs["outcomes"].get(horizon) and obs["outcomes"][horizon].get("mae_bps") is not None]
                ) if any(
                    obs["outcomes"].get(horizon) and obs["outcomes"][horizon].get("mae_bps") is not None
                    for obs in observations.values()
                ) else None,
                "mfe_median_bps": statistics.median(
                    [float(obs["outcomes"][horizon]["mfe_bps"]) for obs in observations.values()
                     if obs["outcomes"].get(horizon) and obs["outcomes"][horizon].get("mfe_bps") is not None]
                ) if any(
                    obs["outcomes"].get(horizon) and obs["outcomes"][horizon].get("mfe_bps") is not None
                    for obs in observations.values()
                ) else None,
                "control_scored": control_scored,
                "excess": _summary([value for value, _ in control_excess], [stamp for _, stamp in control_excess]),
            }
        )
        rank_rows = []
        for rank in (1, 2, 3, 4):
            rank_scored = [
                (float(observation["outcomes"][horizon]["move_bps"]), float(observation["decided_at"]))
                for observation in observations.values()
                if observation["strength"] == rank
                and observation["outcomes"].get(horizon)
                and observation["outcomes"][horizon].get("status") == "scored"
                and observation["outcomes"][horizon].get("move_bps") is not None
            ]
            rank_n = sum(1 for observation in observations.values() if observation["strength"] == rank)
            rank_hits = sum(1 for value, _ in rank_scored if value > 0)
            rank_rows.append(
                {
                    "strength": rank,
                    "label": spec.scale_labels.get(rank, str(rank)),
                    "n": rank_n,
                    "scored": len(rank_scored),
                    "mean_bps": statistics.fmean([value for value, _ in rank_scored]) if rank_scored else None,
                    "hit_rate": rank_hits / len(rank_scored)
                    if rank_scored and spec.mode == "directional"
                    else None,
                }
            )
        summary["ranks"] = rank_rows
        horizons[str(horizon)] = summary

    primary = horizons[str(spec.primary_horizon_s)]
    primary_test = {"kind": "unavailable", "n_strong": 0, "n_weak": 0}
    if observations:
        strong = [
            (float(observation["outcomes"][spec.primary_horizon_s]["move_bps"]), float(observation["decided_at"]))
            for observation in observations.values()
            if observation["strength"] >= 3
            and observation["outcomes"].get(spec.primary_horizon_s)
            and observation["outcomes"][spec.primary_horizon_s].get("status") == "scored"
            and observation["outcomes"][spec.primary_horizon_s].get("move_bps") is not None
        ]
        weak = [
            (float(observation["outcomes"][spec.primary_horizon_s]["move_bps"]), float(observation["decided_at"]))
            for observation in observations.values()
            if observation["strength"] in (1, 2)
            and observation["outcomes"].get(spec.primary_horizon_s)
            and observation["outcomes"][spec.primary_horizon_s].get("status") == "scored"
            and observation["outcomes"][spec.primary_horizon_s].get("move_bps") is not None
        ]
        if spec.mode == "directional":
            diff = (
                statistics.fmean([value for value, _ in strong]) - statistics.fmean([value for value, _ in weak])
                if strong and weak
                else None
            )
            interval = difference_interval(strong, weak) if strong and weak else None
            primary_test = {
                "kind": "rank_difference",
                "n_strong": len(strong),
                "n_weak": len(weak),
                "strong_mean_bps": statistics.fmean([value for value, _ in strong]) if strong else None,
                "weak_mean_bps": statistics.fmean([value for value, _ in weak]) if weak else None,
                "diff_bps": diff,
                "ci": (interval[0], interval[1]) if interval else None,
                "p": interval[2] if interval else None,
            }
        else:
            excess = primary["excess"]
            primary_test = {
                "kind": "excess_absolute",
                "mean_bps": excess["mean_bps"],
                "ci": excess["ci"],
                "p": excess["p"],
                "n_strong": len(strong),
                "n_weak": len(weak),
            }
    primary_test["status"] = status_for_bot(len(observations), primary["scored"])

    return {
        "id": spec.id,
        "version": spec.version,
        "contract_hash": spec.contract_hash,
        "mode": spec.mode,
        "hypothesis": spec.hypothesis,
        "strength": spec.strength,
        "scale": spec.scale_labels,
        "window": spec.window,
        "primary_horizon_s": spec.primary_horizon_s,
        "implemented": implemented,
        "note": spec.note,
        "observations": len(observations),
        "scored": primary["scored"],
        "coverage": primary["coverage"],
        "status": status_for_bot(len(observations), primary["scored"]),
        "horizons": horizons,
        "primary": primary_test,
    }
