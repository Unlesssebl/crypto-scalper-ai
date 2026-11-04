"""Summarize AI direction against subsequent Hyperliquid mark prices."""
from contextlib import closing
import sqlite3

HORIZONS = (1, 5, 15)
PERIOD_SECONDS = 24 * 60 * 60
OUTCOMES = ("correct", "incorrect", "flat", "pending", "missing")
ATTEMPTS = ("directional", "neutral", "insufficient", "error", "skipped", "pending")


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


def calculate_quality(path, now, windows):
    since = now - PERIOD_SECONDS
    results = {
        window: {
            "attempts": dict.fromkeys(ATTEMPTS, 0),
            "horizons": {minutes: dict.fromkeys(OUTCOMES, 0) for minutes in HORIZONS},
        }
        for window in windows
    }
    uri = path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("""
            SELECT a.id, a.window, a.delivery,
                   json_extract(a.ai, '$.status') AS status,
                   json_extract(a.ai, '$.direction') AS direction,
                   CASE WHEN json_extract(a.snapshot, '$.metrics.price.quality') = 'ok'
                        THEN json_extract(a.snapshot, '$.metrics.price.value') END AS start_price,
                   o.minutes, o.price AS end_price, o.status AS observation
            FROM analyses a LEFT JOIN observations o ON o.event_id = a.id
            WHERE a.captured_at BETWEEN ? AND ?
        """, (since, now))
        seen = set()
        for row in rows:
            window = results.get(row["window"])
            if window is None:
                continue
            category = attempt(row)
            if row["id"] not in seen:
                seen.add(row["id"])
                window["attempts"][category] += 1
            if category == "directional" and row["minutes"] in HORIZONS:
                result = outcome(row["direction"], row["start_price"], row["end_price"], row["observation"])
                window["horizons"][row["minutes"]][result] += 1
    for window in results.values():
        for counts in window["horizons"].values():
            counts["missing"] += window["attempts"]["directional"] - sum(counts.values())
    return {"calculated_at": now, "since": since, "windows": results}
