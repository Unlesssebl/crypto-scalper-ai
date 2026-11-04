"""Summarize AI direction against subsequent Hyperliquid mark prices."""
from contextlib import closing
import json
import sqlite3
from statistics import median

from .audit import audit_report

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
    if row["status"] in ("expired", "unconfigured", "insufficient_market_data"):
        return "skipped"
    if row["status"] is None and row["delivery"] == "pending":
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


def calculate_quality(path, now, windows):
    since = now - PERIOD_SECONDS
    results = {window: {**counters(), "triggers": {}} for window in windows}
    uri = path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("""
            SELECT a.id, a.window, a.delivery, a.event, a.analyzed_at,
                   json_extract(a.ai, '$.status') AS status,
                   json_extract(a.ai, '$.direction') AS direction,
                   CASE WHEN json_extract(a.snapshot, '$.metrics.price.quality') = 'ok'
                        THEN json_extract(a.snapshot, '$.metrics.price.value') END AS start_price,
                   o.minutes, o.price AS end_price, o.status AS observation
            FROM analyses a LEFT JOIN observations o ON o.event_id = a.id
            WHERE a.captured_at BETWEEN ? AND ?
        """, (since, now))
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
        experiments = context_quality(db, since, now, windows)
        audit = audit_report(db, since, now, windows)
    for window in results.values():
        for target in (window, *window["triggers"].values()):
            for counts in target["horizons"].values():
                counts["missing"] += target["attempts"]["directional"] - sum(counts.values())
        for group in window["triggers"].values():
            delays = group.pop("delays")
            group["median_delay_seconds"] = round(median(delays), 1) if delays else None
            group["delay_samples"] = len(delays)
    return {"calculated_at": now, "since": since, "windows": results, "trigger_labels": TRIGGERS,
            "context_experiment": experiments, "finding_audit": audit}
