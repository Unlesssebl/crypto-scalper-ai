"""Summarize AI direction against subsequent Hyperliquid mark prices."""
import json
import sqlite3
from bisect import bisect_left
from collections import defaultdict
from contextlib import closing
from statistics import median

from .audit import audit_report
from .shadow import MINUTE_CONTROL, MOVE_THRESHOLD_PCT, SHADOW_COOLDOWN, STRICT_MINUTE, VARIANTS

HORIZONS = (1, 5, 15)
PERIOD_SECONDS = 24 * 60 * 60
OUTCOMES = ("correct", "incorrect", "flat", "pending", "missing")
ATTEMPTS = ("directional", "neutral", "insufficient", "error", "skipped", "pending")
TRIGGERS = {
    "tape_burst": "Всплеск ленты",
    "range_burst": "Всплеск диапазона",
    "combined": "Лента и диапазон",
    "orderflow_transition": "Смена Order Flow",
    "other": "Без разметки",
}


def trigger(event):
    anomalies = event.get("anomalies") or ()
    if "tape_burst" in anomalies and "range_burst" in anomalies:
        return "combined"
    for name in ("tape_burst", "range_burst"):
        if name in anomalies:
            return name
    return "orderflow_transition" if event.get("type") == "orderflow_transition" else "other"


def counters():
    return {"attempts": dict.fromkeys(ATTEMPTS, 0),
            "horizons": {minutes: dict.fromkeys(OUTCOMES, 0) for minutes in HORIZONS}}


def positive_price(value):
    return isinstance(value, (int, float)) and 0 < value < float("inf")


def outcome(direction, start, end, status):
    if not positive_price(start):
        return "missing"
    if status == "pending":
        return "pending"
    if status != "observed" or not positive_price(end):
        return "missing"
    if start == end:
        return "flat"
    return "correct" if (end > start) == (direction == "BULLISH") else "incorrect"


def attempt(row):
    if row["status"] == "ok":
        return {"BULLISH": "directional", "BEARISH": "directional",
                "NEUTRAL": "neutral", "INSUFFICIENT_DATA": "insufficient"}.get(row["direction"], "error")
    if row["status"] in ("expired", "unconfigured", "insufficient_market_data", "facts_only", "skipped"):
        return "skipped"
    if row["status"] is None and row["effective_delivery"] == "pending":
        return "pending"
    return "error"


CONTEXT_ARMS = ("hyperdata", "baseline", "senior")


def context_quality(db, since, now, windows):
    results = {window: {
        "attempted": 0, "matched": 0, "changed": 0,
        "horizons": {minutes: {arm: dict.fromkeys((*OUTCOMES, "no_direction"), 0) for arm in CONTEXT_ARMS}
                     for minutes in HORIZONS},
    } for window in windows}
    rows = db.execute("""
        SELECT a.window, a.hyperdata, a.ai, t.baseline_ai, o.minutes, o.price, o.status,
               anchor.price AS anchor_price, anchor.status AS anchor_status
        FROM context_trials t JOIN analyses a ON a.id = t.event_id
        JOIN context_observations o ON o.event_id = t.event_id
        JOIN context_observations anchor ON anchor.event_id = t.event_id AND anchor.minutes = 0
        WHERE t.ready_at BETWEEN ? AND ?
    """, (since, now))
    for row in rows:
        if row["window"] not in results:
            continue
        result = results[row["window"]]
        primary, baseline = json.loads(row["ai"]), json.loads(row["baseline_ai"])
        matched = primary["status"] == baseline["status"] == "ok"
        if row["minutes"] == 0:
            result["attempted"] += 1
            result["matched"] += matched
            result["changed"] += matched and primary["direction"] != baseline["direction"]
            continue
        if not matched:
            continue
        directions = (json.loads(row["hyperdata"])["direction"], baseline["direction"], primary["direction"])
        for arm, direction in zip(CONTEXT_ARMS, directions):
            if direction not in {"BULLISH", "BEARISH"}:
                category = "no_direction"
            elif row["anchor_status"] == "pending":
                category = "pending"
            else:
                category = outcome(direction, row["anchor_price"], row["price"], row["status"])
            result["horizons"][row["minutes"]][arm][category] += 1
    return results


def minute_shadow_comparison(db, since, now):
    rows = db.execute("""
        SELECT variant,symbol,detected_at,status,peak_pct FROM shadow_findings
        WHERE window='1m' AND variant IN (?,?) AND detected_at BETWEEN ? AND ?
        ORDER BY detected_at
    """, (MINUTE_CONTROL, STRICT_MINUTE, since, now)).fetchall()
    strict_times = defaultdict(list)
    for row in rows:
        if row["variant"] == STRICT_MINUTE:
            strict_times[row["symbol"]].append(row["detected_at"])
    result = {"baseline": 0, "strict": 0, "baseline_moves": 0, "missed_moves": 0}
    for row in rows:
        if row["variant"] == STRICT_MINUTE:
            result["strict"] += 1
            continue
        result["baseline"] += 1
        if row["status"] not in ("hit", "quiet") or row["peak_pct"] < MOVE_THRESHOLD_PCT:
            continue
        result["baseline_moves"] += 1
        times = strict_times[row["symbol"]]
        at = row["detected_at"]
        index = bisect_left(times, at - SHADOW_COOLDOWN)
        if index == len(times) or times[index] > at + SHADOW_COOLDOWN:
            result["missed_moves"] += 1
    return result


def calculate_quality(path, now, windows):
    since = now - PERIOD_SECONDS
    results = {window: {**counters(), "triggers": {}} for window in windows}
    uri = path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("""
            SELECT a.id, a.window, a.delivery, a.event, a.analyzed_at,
                   CASE WHEN a.ai IS NULL AND f.event_id IS NOT NULL AND a.captured_at <= ? THEN 'facts_only'
                        ELSE json_extract(a.ai, '$.status') END AS status,
                   CASE WHEN a.ai IS NULL AND f.event_id IS NOT NULL THEN 'pending'
                        ELSE a.delivery END AS effective_delivery,
                   json_extract(a.ai, '$.direction') AS direction,
                   CASE WHEN json_extract(a.snapshot, '$.metrics.price.quality') = 'ok'
                        THEN json_extract(a.snapshot, '$.metrics.price.value') END AS start_price,
                   o.minutes, o.price AS end_price, o.status AS observation
            FROM analyses a LEFT JOIN observations o ON o.event_id = a.id
            LEFT JOIN fact_messages f ON f.event_id = a.id
            WHERE a.captured_at BETWEEN ? AND ?
        """, (now - 150, since, now))
        events = {}
        for row in rows:
            window = results.get(row["window"])
            if window is None:
                continue
            if row["id"] not in events:
                category = attempt(row)
                event = json.loads(row["event"])
                name = trigger(event)
                if name not in window["triggers"]:
                    window["triggers"][name] = {**counters(), "delays": []}
                group = window["triggers"][name]
                events[row["id"]] = category, group
                window["attempts"][category] += 1
                group["attempts"][category] += 1
                detected, analyzed = event.get("detected_at"), row["analyzed_at"]
                if isinstance(detected, (int, float)) and analyzed is not None and analyzed >= detected:
                    group["delays"].append(analyzed - detected)
            category, group = events[row["id"]]
            if category == "directional" and row["minutes"] in HORIZONS:
                result = outcome(row["direction"], row["start_price"], row["end_price"], row["observation"])
                for target in (window, group):
                    target["horizons"][row["minutes"]][result] += 1
        selection = selection_quality(db, since, now)
        experiments = context_quality(db, since, now, windows)
        audit = audit_report(db, since, now, windows)
        minute_shadow = minute_shadow_comparison(db, since, now)
        shadow = {window: {variant: {} for variant in VARIANTS} for window in windows}
        for row in db.execute("""
            SELECT window,variant,status,COUNT(*) AS count FROM shadow_findings
            WHERE detected_at BETWEEN ? AND ? GROUP BY window,variant,status
        """, (since, now)):
            if row["window"] in shadow:
                counts = shadow[row["window"]].setdefault(row["variant"], {})
                counts[row["status"]] = row["count"]
    for window in results.values():
        for target in (window, *window["triggers"].values()):
            for counts in target["horizons"].values():
                counts["missing"] += target["attempts"]["directional"] - sum(counts.values())
        for group in window["triggers"].values():
            delays = group.pop("delays")
            group["median_delay_seconds"] = round(median(delays), 1) if delays else None
            group["delay_samples"] = len(delays)
    return {"selection": selection, "calculated_at": now, "since": since, "windows": results,
            "trigger_labels": TRIGGERS,
            "context_experiment": experiments, "finding_audit": audit, "shadow": shadow,
            "minute_shadow": minute_shadow}



def selection_quality(db, since, now):
    """Evaluate the full detector population, including rejected opportunities."""
    result = {"detected": 0, "admitted": 0, "requests": 0, "reasons": {}, "outcomes": {}, "movements": {}}
    rows = db.execute("""
        SELECT s.*, c.snapshot, c.event FROM ai_selection s
        JOIN finding_candidates c ON c.id=s.candidate_id WHERE s.at BETWEEN ? AND ?
    """, (since, now)).fetchall()
    for row in rows:
        result["detected"] += 1
        result["admitted"] += row["admitted"]
        reason = row["reason"]
        result["reasons"][reason] = result["reasons"].get(reason, 0) + 1
        snapshot, event = json.loads(row["snapshot"]), json.loads(row["event"])
        mark = snapshot["metrics"].get("price", {})
        start = mark.get("value") if mark.get("quality") == "ok" else None
        group = "admitted" if row["admitted"] else "rejected"
        for obs in db.execute("SELECT * FROM candidate_observations WHERE event_id=?", (row["candidate_id"],)):
            horizon = result["outcomes"].setdefault(str(obs["minutes"]), {})
            counts = horizon.setdefault(group, dict.fromkeys((*OUTCOMES, "no_direction"), 0))
            direction = event.get("direction")
            score = (outcome(direction, start, obs["price"], obs["status"])
                     if direction in {"BULLISH", "BEARISH"} else "no_direction")
            counts[score] += 1
            movement = result["movements"].setdefault(str(obs["minutes"]), {}).setdefault(
                group, {"samples": 0, "sum_abs_pct": 0, "max_abs_pct": 0, "natr_samples": 0, "natr_hits": 0},
            )
            if obs["status"] == "observed" and positive_price(start) and positive_price(obs["price"]):
                change = abs(100 * (obs["price"] / start - 1))
                movement["samples"] += 1
                movement["sum_abs_pct"] += change
                movement["max_abs_pct"] = max(movement["max_abs_pct"], change)
                natr = snapshot["metrics"].get("natr", {})
                if natr.get("quality") == "ok" and positive_price(natr.get("value")):
                    movement["natr_samples"] += 1
                    movement["natr_hits"] += change >= natr["value"]
    result["requests"] = db.execute("SELECT COUNT(*) FROM ai_requests WHERE at BETWEEN ? AND ?",
                                     (since, now)).fetchone()[0]
    # Compare independent decisions against their actual input snapshot.
    result["arms"] = {arm: {str(m): dict.fromkeys((*OUTCOMES, "no_direction"), 0) for m in HORIZONS}
                      for arm in ("ai", "rule", "consensus")}
    for row in db.execute("""
        SELECT a.*, o.minutes, o.price AS end_price, o.status AS observation
        FROM analyses a JOIN ai_selection s ON s.candidate_id=a.id
        JOIN observations o ON o.event_id=a.id WHERE s.at BETWEEN ? AND ?
        AND json_extract(a.ai,'$.status')='ok'
    """, (since, now)):
        snap, native = json.loads(row["snapshot"]), json.loads(row["hyperdata"])
        ai = json.loads(row["ai"]) if row["ai"] else {}
        verdict = (json.loads(row["consensus"]) or {}) if row["consensus"] else {}
        category = verdict.get("category")
        consensus = {"CONSENSUS_LONG": "BULLISH", "CONSENSUS_SHORT": "BEARISH"}.get(category)
        for arm, direction in (("ai", ai.get("direction") if ai.get("status") == "ok" else None),
                               ("rule", native.get("direction")), ("consensus", consensus)):
            score = (outcome(direction, snap["metrics"].get("price", {}).get("value"),
                             row["end_price"], row["observation"])
                     if direction in {"BULLISH", "BEARISH"} else "no_direction")
            result["arms"][arm][str(row["minutes"])][score] += 1
    return result
