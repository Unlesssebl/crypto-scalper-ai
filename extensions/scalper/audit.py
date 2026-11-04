"""Objective post-delivery movement and decision reporting."""
from collections import Counter, defaultdict
from statistics import median

RULESET = "finding-v4"
STRONG_RANK = 3
RANK_LABELS = {
    "finding-v1": ("Нет данных", "Фоновая", "Заметная", "Сильная", "Критичная"),
    "finding-v2": ("Нет данных", "Фоновая", "Заметная", "Выраженное движение", "Сильное движение"),
    "finding-v3": ("Нет данных", "Фоновая", "Заметная", "Выраженное движение", "Сильное движение"),
    RULESET: ("Нет данных", "Фоновая", "Заметная", "Выраженное движение", "Сильное движение"),
}
MOVE_SECONDS = 300
COVERAGE_GAP_SECONDS = 30
CONTROL_INTERVAL = 900
CONTROL_DAYS = 7
MATCH_HOUR_LIMIT = 2 * 3600
DELAY_BUCKETS = ("0–15 с", ">15–45 с", ">45 с")


def update_move(row, now, observation):
    delivered = row["delivered_at"]
    deadline = delivered + MOVE_SECONDS
    anchor, last, peak, gap = (row[key] for key in ("anchor_price", "last_at", "peak_pct", "gap"))
    if observation:
        price, stamp = observation
        if delivered <= stamp <= min(now, deadline) and (last is None or stamp > last):
            if anchor is None:
                if stamp - delivered <= COVERAGE_GAP_SECONDS:
                    anchor, last, peak = price, stamp, 0.0
            else:
                gap = gap or stamp - last > COVERAGE_GAP_SECONDS
                last = stamp
                peak = max(peak, abs(price / anchor - 1) * 100)
    status = "pending"
    if now >= deadline + COVERAGE_GAP_SECONDS:
        if anchor is None or gap or deadline - last > COVERAGE_GAP_SECONDS:
            status = "missing"
        else:
            status = "hit" if peak >= row["target_pct"] else "quiet"
    return anchor, last, peak, int(gap), status


def delay_bucket(seconds):
    return DELAY_BUCKETS[0 if seconds <= 15 else 1 if seconds <= 45 else 2]


def match_controls(deliveries, controls):
    available = list(controls)
    pairs = []
    for delivery in deliveries:
        timestamp, target = delivery["delivered_at"], delivery["target_pct"]
        options = []
        for index, control in enumerate(available):
            distance = abs(control["delivered_at"] - timestamp) % 86400
            distance = min(distance, 86400 - distance)
            if (abs(control["delivered_at"] - timestamp) >= MOVE_SECONDS
                    and distance <= MATCH_HOUR_LIMIT and 0.5 <= control["target_pct"] / target <= 2):
                options.append((distance, abs(control["delivered_at"] - timestamp), index))
        if options:
            pairs.append((delivery, available.pop(min(options)[2])))
    return pairs


def audit_report(db, since, now, windows):
    results = {window: {"candidates": 0, "decisions": {}, "versions": {},
                        "delay_buckets": {}, "rank_labels": RANK_LABELS, "control": {"pairs": 0, "signal_hits": 0, "control_hits": 0}}
               for window in windows}
    for row in db.execute("""
        SELECT c.window,c.version,c.decision,COUNT(*) AS count
        FROM finding_candidates c WHERE c.detected_at BETWEEN ? AND ?
        GROUP BY c.window,c.version,c.decision
    """, (since, now)):
        result = results.get(row["window"])
        if result is None:
            continue
        count = row["count"]
        result["candidates"] += count
        decision = row["decision"]
        result["decisions"][decision] = result["decisions"].get(decision, 0) + count
        result["versions"].setdefault(row["version"], {})
    delays = defaultdict(list)
    deliveries = defaultdict(list)
    for row in db.execute("""
        SELECT a.symbol,a.window,m.rank,m.status,m.delivered_at,m.target_pct,c.detected_at,c.version
        FROM delivered_moves m JOIN analyses a ON a.id=m.analysis_id
        JOIN finding_candidates c ON c.analysis_id=a.id
        WHERE m.delivered_at BETWEEN ? AND ? ORDER BY m.delivered_at, a.id
    """, (since, now)):
        result = results.get(row["window"])
        if result is None:
            continue
        rank = row["rank"]
        groups = result["versions"].setdefault(row["version"], {})
        counts = groups.setdefault(rank, Counter())
        counts[row["status"]] += 1
        delay = row["delivered_at"] - row["detected_at"]
        delays[row["window"], row["version"], rank].append(delay)
        result["delay_buckets"].setdefault(delay_bucket(delay), Counter())[row["status"]] += 1
        if row["status"] in ("hit", "quiet"):
            deliveries[row["symbol"], row["window"]].append(row)
    for (window, version, rank), samples in delays.items():
        results[window]["versions"][version][rank]["median_delay_seconds"] = round(median(samples), 1)
    for (symbol, window), delivered in deliveries.items():
        controls = db.execute("""
            SELECT delivered_at,target_pct,status FROM quiet_moves
            WHERE symbol=? AND window=? AND delivered_at BETWEEN ? AND ?
              AND status IN ('hit','quiet')
            ORDER BY delivered_at
        """, (symbol, window, now - CONTROL_DAYS * 86400, now))
        result = results[window]["control"]
        for signal, control in match_controls(delivered, controls):
            result["pairs"] += 1
            result["signal_hits"] += signal["status"] == "hit"
            result["control_hits"] += control["status"] == "hit"
    return results
