"""Versioned fact packets, model attempts and assertion checks in the existing journal."""

import json
import uuid
from collections import Counter, defaultdict

from .fact_packet import canonical, verify_packet

SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_episodes (
    id TEXT PRIMARY KEY, symbol TEXT NOT NULL, first_at REAL NOT NULL, last_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_packets (
    id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, symbol TEXT NOT NULL,
    at REAL NOT NULL, payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS fact_packets_episode ON fact_packets(episode_id,at);
CREATE TABLE IF NOT EXISTS fact_reviews (
    id TEXT PRIMARY KEY, packet_id TEXT NOT NULL REFERENCES fact_packets(id),
    at REAL NOT NULL, completed_at REAL, status TEXT NOT NULL, provider TEXT,
    model TEXT, raw TEXT, result TEXT, reason TEXT
);
CREATE INDEX IF NOT EXISTS fact_reviews_at ON fact_reviews(at);
CREATE TABLE IF NOT EXISTS fact_claims (
    review_id TEXT NOT NULL REFERENCES fact_reviews(id), claim_id TEXT NOT NULL,
    section TEXT NOT NULL, status TEXT NOT NULL, signature TEXT NOT NULL,
    novelty TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(review_id,claim_id)
);
CREATE INDEX IF NOT EXISTS fact_claims_signature ON fact_claims(signature);
"""


class ConsistencyStore:
    def __init__(self, db):
        self.db = db
        with db:
            db.executescript(SCHEMA)
            columns = {r[1] for r in db.execute("PRAGMA table_info(fact_reviews)")}
            for name in ("origin", "trigger_reason", "usage", "route"):
                if name not in columns:
                    db.execute(f"ALTER TABLE fact_reviews ADD COLUMN {name} TEXT")

    def observe_episode(self, packet):
        with self.db:
            self.db.execute(
                "INSERT INTO fact_episodes VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET last_at=excluded.last_at",
                (packet["episode_id"], packet["symbol"], packet["captured_at"], packet["captured_at"]),
            )

    def save_packet(self, packet):
        verify_packet(packet)
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO fact_packets VALUES(?,?,?,?,?)",
                (packet["packet_id"], packet["episode_id"], packet["symbol"], packet["captured_at"], canonical(packet)),
            )

    def packet(self, key):
        row = self.db.execute("SELECT payload FROM fact_packets WHERE id=?", (key,)).fetchone()
        return verify_packet(json.loads(row[0])) if row else None

    def begin(self, packet, now, provider, model, *, origin="manual", trigger_reason=None, route=None):
        self.save_packet(packet)
        self.observe_episode(packet)
        key = uuid.uuid4().hex
        with self.db:
            self.db.execute(
                "INSERT INTO fact_reviews(id,packet_id,at,status,provider,model,origin,trigger_reason,route) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (key, packet["packet_id"], now, "pending", provider, model, origin, trigger_reason, route),
            )
        return key

    def finish(self, key, status, now, *, result=None, raw=None, reason=None, model=None, usage=None):
        row = self.db.execute(
            "SELECT p.episode_id FROM fact_reviews r JOIN fact_packets p ON p.id=r.packet_id WHERE r.id=?", (key,)
        ).fetchone()
        if row is None:
            raise ValueError("Unknown review")
        if result is not None:
            for claim in result["claims"]:
                seen = self.db.execute(
                    """SELECT 1 FROM fact_claims c JOIN fact_reviews r ON r.id=c.review_id
                    JOIN fact_packets p ON p.id=r.packet_id WHERE p.episode_id=? AND c.signature=? LIMIT 1""",
                    (row[0], claim["signature"]),
                ).fetchone()
                claim["novelty"] = "repeated" if seen else "first_seen"
        with self.db:
            self.db.execute(
                "UPDATE fact_reviews SET completed_at=?,status=?,raw=?,result=?,reason=?,"
                "model=COALESCE(?,model),usage=? WHERE id=?",
                (
                    now,
                    status,
                    raw,
                    canonical(result) if result is not None else None,
                    reason,
                    model,
                    canonical(usage) if usage is not None else None,
                    key,
                ),
            )
            if result is not None:
                self.db.executemany(
                    "INSERT INTO fact_claims VALUES(?,?,?,?,?,?,?)",
                    (
                        (key, c["id"], c["section"], c["status"], c["signature"], c["novelty"], canonical(c))
                        for c in result["claims"]
                    ),
                )
        return self.review(key)

    def review(self, key):
        row = self.db.execute(
            "SELECT r.*,p.at AS packet_at FROM fact_reviews r JOIN fact_packets p ON p.id=r.packet_id WHERE r.id=?",
            (key,),
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["result"] = json.loads(result["result"]) if result["result"] else None
        result["usage"] = json.loads(result["usage"]) if result["usage"] else None
        # The raw response remains in SQLite for audit, never as trusted product text.
        result.pop("raw")
        return result

    def latest(self, packet_id):
        row = self.db.execute(
            "SELECT id FROM fact_reviews WHERE packet_id=? ORDER BY at DESC,rowid DESC LIMIT 1", (packet_id,)
        ).fetchone()
        return self.review(row[0]) if row else None

    def latest_episode(self, episode_id):
        row = self.db.execute(
            "SELECT r.id FROM fact_reviews r JOIN fact_packets p ON p.id=r.packet_id "
            "WHERE p.episode_id=? ORDER BY r.at DESC,r.rowid DESC LIMIT 1",
            (episode_id,),
        ).fetchone()
        return self.review(row[0]) if row else None

    def report(self, now):
        since = now - 86400
        statuses = Counter()
        reasons = Counter()
        abstentions = 0
        sections = defaultdict(Counter)
        novelty = Counter()
        evidence_used = evidence_available = 0
        seen_packets = set()
        episodes = set()
        answered_episodes = set()
        pending = 0
        origins = Counter()
        elapsed = []
        tokens = Counter()
        usage_reported = 0
        cost_total, cost_reported = 0, 0
        for row in self.db.execute(
            """SELECT r.*,p.episode_id FROM fact_reviews r
                JOIN fact_packets p ON p.id=r.packet_id WHERE r.at BETWEEN ? AND ?""",
            (since, now),
        ):
            statuses[row["status"]] += 1
            origins[row["origin"] or "legacy"] += 1
            if row["completed_at"] is not None:
                elapsed.append(max(0, row["completed_at"] - row["at"]))
            if row["usage"]:
                usage = json.loads(row["usage"])
                usage_reported += bool(usage)
                tokens.update({k: v for k, v in usage.items() if k.endswith("_tokens")})
                if type(usage.get("cost")) in (int, float):
                    cost_total += usage["cost"]
                    cost_reported += 1
            if row["reason"]:
                reasons[row["reason"]] += 1
            episodes.add(row["episode_id"])
            if row["status"] == "ok":
                answered_episodes.add(row["episode_id"])
            seen_packets.add(row["packet_id"])
            pending += row["status"] == "pending"
            if row["result"]:
                result = json.loads(row["result"])
                abstentions += result["abstention"]
                evidence_used += result["referenced_facts"]
                evidence_available += result["total_facts"]
                for claim in result["claims"]:
                    sections[claim["section"]][claim["status"]] += 1
                    novelty[claim["novelty"]] += 1
        observed_episodes = self.db.execute(
            "SELECT COUNT(*) FROM fact_episodes WHERE last_at BETWEEN ? AND ?", (since, now)
        ).fetchone()[0]
        api_attempts = (
            self.db.execute(
                "SELECT COUNT(*) FROM ai_requests WHERE (purpose='context' OR purpose LIKE 'context_%') "
                "AND at BETWEEN ? AND ?",
                (since, now),
            ).fetchone()[0]
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE name='ai_requests'").fetchone()
            else 0
        )
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE name='ai_route_requests'").fetchone():
            api_attempts += self.db.execute(
                "SELECT COUNT(*) FROM ai_route_requests WHERE route='free' AND at BETWEEN ? AND ?", (since, now)
            ).fetchone()[0]
        return dict(
            reported_cost=cost_total if cost_reported else None,
            cost_reported_attempts=cost_reported,
            unknown_cost_attempts=max(0, api_attempts - cost_reported),
            version=1,
            since=since,
            calculated_at=now,
            attempts=sum(statuses.values()),
            api_attempts=api_attempts,
            statuses=dict(statuses),
            pending=pending,
            abstentions=abstentions,
            origins=dict(origins),
            mean_latency_seconds=sum(elapsed) / len(elapsed) if elapsed else None,
            tokens=dict(tokens),
            usage_reported_attempts=usage_reported,
            reasons=dict(reasons),
            requested_episodes=len(episodes),
            reviewed_episodes=len(answered_episodes),
            observed_episodes=observed_episodes,
            reviewed_packets=len(seen_packets),
            claims={k: dict(v) for k, v in sections.items()},
            novelty=dict(novelty),
            referenced_facts_per_review_sum=evidence_used,
            available_facts_per_review_sum=evidence_available,
            limitation="Shared facts; interpretation truth and trader usefulness are not automatically scored.",
        )
