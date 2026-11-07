"""Descriptive post-selection observations, never PnL or a backfilled forecast."""

import json
from statistics import median

from .fact_packet import checksum, packet_ready

HORIZONS = (60, 300, 900)


class NotificationQuality:
    def __init__(self, db):
        self.db = db
        db.executescript("""
            CREATE TABLE IF NOT EXISTS notification_candidates(
                id TEXT PRIMARY KEY,episode TEXT,symbol TEXT,at REAL,last_at REAL,checks INTEGER,
                decision TEXT,reason TEXT,phase TEXT,price REAL,last_quote REAL,low REAL,high REAL,
                continuous INTEGER,packet_id TEXT,evidence TEXT);
            CREATE TABLE IF NOT EXISTS notification_observations(
                candidate TEXT,horizon INTEGER,at REAL,price REAL,return_pct REAL,
                low_pct REAL,high_pct REAL,continuous INTEGER,status TEXT,
                PRIMARY KEY(candidate,horizon));
        """)

    def record(self, packet, state, decision, reason, *, card=None):
        zone = state.get("zone") or {}
        key = checksum(
            dict(
                episode=packet["episode_id"],
                decision=decision,
                reason=reason,
                phase=state.get("phase"),
                zone=zone.get("id"),
                card=card,
            )
        )
        at = packet["captured_at"]
        price = state.get("midpoint")
        if price is None or price <= 0:
            return
        quote = next((f.get("observed_at") for f in packet["facts"] if f["id"] == "book.midpoint"), None)
        if quote is None:
            return
        selected = [
            f
            for f in packet["facts"]
            if f["id"].startswith(
                (
                    "book.",
                    "context.phase.1m.",
                    "context.1m.natr",
                    "context.1m.wide_spread",
                    "frame.1m.ofi",
                    "frame.1m.first_trade_price",
                    "frame.1m.last_trade_price",
                )
            )
        ]
        evidence = dict(
            context_version=packet.get("context_version"),
            packet_id=packet["packet_id"],
            facts=selected,
            scenario=packet.get("scenarios"),
        )
        with self.db:
            self.db.execute(
                """INSERT INTO notification_candidates VALUES(?,?,?,?,?,1,?,?,?,?,?,?,?,1,?,?)
                ON CONFLICT(id) DO UPDATE SET last_at=excluded.last_at,checks=checks+1""",
                (
                    key,
                    packet["episode_id"],
                    packet["symbol"],
                    at,
                    at,
                    decision,
                    reason,
                    state.get("phase"),
                    price,
                    quote,
                    price,
                    price,
                    packet["packet_id"],
                    json.dumps(evidence, ensure_ascii=False),
                ),
            )

    def sample(self, packets, now):
        by_episode = {p["episode_id"]: p for p in packets.values()}
        rows = list(
            self.db.execute(
                "SELECT * FROM notification_candidates WHERE at>=? AND at<? "
                "AND id IN (SELECT id FROM notification_candidates WHERE "
                "(SELECT COUNT(*) FROM notification_observations WHERE candidate=id)<3)",
                (now - 7 * 86400, now),
            )
        )
        with self.db:
            for row in rows:
                p = by_episode.get(row["episode"])
                quote, price = None, None
                if p and packet_ready(p, now):
                    f = next((f for f in p["facts"] if f["id"] == "book.midpoint" and f["quality"] == "ok"), {})
                    quote, price = f.get("observed_at"), f.get("value")
                fresh = quote is not None and price is not None and quote > row["last_quote"]
                continuous = bool(row["continuous"]) and quote is not None and quote - row["last_quote"] <= 20
                low, high = row["low"], row["high"]
                if fresh:
                    low, high = min(low, price), max(high, price)
                    self.db.execute(
                        "UPDATE notification_candidates SET low=?,high=?,last_quote=?,continuous=? WHERE id=?",
                        (low, high, quote, int(continuous), row["id"]),
                    )
                elif now - row["last_quote"] > 20:
                    continuous = False
                    self.db.execute("UPDATE notification_candidates SET continuous=0 WHERE id=?", (row["id"],))
                for horizon in HORIZONS:
                    target = row["at"] + horizon
                    observed = fresh and target <= quote <= target + 15
                    if not observed and now <= target + 15:
                        continue
                    status = "observed" if observed else "outside_attention" if not p else "missing_or_stale"
                    self.db.execute(
                        """INSERT OR IGNORE INTO notification_observations VALUES(?,?,?,?,?,?,?,?,?)""",
                        (
                            row["id"],
                            horizon,
                            quote if observed else now,
                            price if observed else None,
                            (price / row["price"] - 1) * 100 if observed else None,
                            (low / row["price"] - 1) * 100 if observed and continuous else None,
                            (high / row["price"] - 1) * 100 if observed and continuous else None,
                            int(continuous),
                            status,
                        ),
                    )
            self.db.execute(
                "DELETE FROM notification_observations WHERE candidate IN "
                "(SELECT id FROM notification_candidates WHERE at<?)",
                (now - 7 * 86400,),
            )
            self.db.execute("DELETE FROM notification_candidates WHERE at<?", (now - 7 * 86400,))

    def report(self, now):
        candidates = [
            dict(r)
            for r in self.db.execute(
                "SELECT decision,reason,COUNT(*) AS count,SUM(checks) AS checks FROM notification_candidates "
                "WHERE at BETWEEN ? AND ? GROUP BY decision,reason",
                (now - 86400, now),
            )
        ]
        rows = list(
            self.db.execute(
                "SELECT o.*,c.decision FROM notification_observations o "
                "JOIN notification_candidates c ON c.id=o.candidate WHERE c.at BETWEEN ? AND ?",
                (now - 86400, now),
            )
        )
        outcomes = []
        for decision in ("queued", "blocked"):
            for horizon in HORIZONS:
                subset = [r for r in rows if r["decision"] == decision and r["horizon"] == horizon]
                known = [r for r in subset if r["status"] == "observed"]
                outcomes.append(
                    dict(
                        decision=decision,
                        horizon=horizon,
                        observed=len(known),
                        missing=len(subset) - len(known),
                        pending=sum(c["count"] for c in candidates if c["decision"] == decision) - len(subset),
                        continuous=sum(r["continuous"] for r in known),
                        median_absolute_change_pct=median(abs(r["return_pct"]) for r in known) if known else None,
                    )
                )
        return dict(
            candidates=candidates,
            outcomes=outcomes,
            meaning="Observed midpoint changes after selection; no fills, fees, PnL or causal efficacy claim.",
        )
