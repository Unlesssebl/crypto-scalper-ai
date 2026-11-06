"""Durable admission, separate free quota, shared paid budget, no fallback."""

import hashlib
import json
import math
import os
import time

import aiohttp

from .analysis import OpenRouter
from .fact_packet import canonical


class AIRoutes:
    def __init__(self, journal, paid):
        self.journal, self.db, self.paid = journal, journal.db, paid
        self.free = OpenRouter(
            paid.session,
            os.getenv("SCALPER_FREE_MODEL", "nvidia/nemotron-3.5-super-vl:free"),
            os.getenv("OPENROUTER_API_KEY", ""),
            proxy=getattr(paid, "proxy", None),
        )
        self.free.timeout_seconds = 20
        self.free.disable_reasoning = True
        self.catalog_until, self.free_available, self.catalog_error = 0, False, None
        self.db.executescript("""CREATE TABLE IF NOT EXISTS ai_route_requests(
            job_id TEXT PRIMARY KEY,episode_id TEXT NOT NULL,route TEXT NOT NULL,at REAL NOT NULL,
            state TEXT NOT NULL,status TEXT NOT NULL,review_id TEXT);
            CREATE INDEX IF NOT EXISTS ai_route_time ON ai_route_requests(route,at);
            CREATE TABLE IF NOT EXISTS ai_route_blocks(route TEXT PRIMARY KEY,until_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS ai_meta_seen(episode_id TEXT PRIMARY KEY,signature TEXT NOT NULL);
        """)
        with self.db:
            self.db.execute("UPDATE ai_route_requests SET status='interrupted' WHERE status='reserved'")

    async def check_free(self, now=None):
        now = time.time() if now is None else now
        if self.catalog_until > now:
            return self.free_available
        self.free_available = False
        self.catalog_until = now + 60
        if (
            os.getenv("SCALPER_FREE_ENABLED", "true").lower() != "true"
            or not self.free.key
            or not self.free.model.endswith(":free")
        ):
            self.catalog_error = "free_route_not_configured"
            return False
        try:
            async with self.free.session.get(
                "https://openrouter.ai/api/v1/models", timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                response.raise_for_status()
                body = await response.json()
            model = next((m for m in body["data"] if m["id"] == self.free.model), None)
            self.free_available = bool(model and all(float(model["pricing"][k]) == 0 for k in ("prompt", "completion")))
            self.catalog_until = now + 3600 if self.free_available else now + 60
            self.catalog_error = None if self.free_available else "zero_price_not_verified"
        except (aiohttp.ClientError, TimeoutError, ValueError, KeyError, TypeError):
            self.catalog_error = "catalog_unavailable"
        return self.free_available

    def last_sent(self, episode, route):
        row = self.db.execute(
            "SELECT state FROM ai_route_requests WHERE episode_id=? AND route=? ORDER BY at DESC LIMIT 1",
            (episode, route),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def reserve(self, packet, route, state, now, limit=40):
        from .context_auto import fingerprint

        signature = hashlib.sha256(
            canonical(
                dict(
                    base=fingerprint(state),
                    zone_size=(state.get("zone") or {}).get("size")
                    if state.get("_reason") == "density_changed"
                    else None,
                    meta=state.get("_meta"),
                )
            ).encode()
        ).hexdigest()
        job = hashlib.sha256(canonical([packet["episode_id"], route, signature]).encode()).hexdigest()
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            if self.db.execute("SELECT 1 FROM ai_route_requests WHERE job_id=?", (job,)).fetchone():
                return None, "unchanged_context"
            if self.db.execute("SELECT 1 FROM ai_route_blocks WHERE route=? AND until_at>?", (route, now)).fetchone():
                return None, "provider_cooldown"
            if self.db.execute(
                "SELECT 1 FROM ai_route_requests WHERE episode_id=? AND route=? AND at>?",
                (packet["episode_id"], route, now - 300),
            ).fetchone():
                return None, "cooldown"
            if route == "free":
                for interval, maximum in ((3600, 12), (86400, 40)):
                    used = self.db.execute(
                        "SELECT COUNT(*) FROM ai_route_requests WHERE route='free' AND at>?", (now - interval,)
                    ).fetchone()[0]
                    if used >= maximum:
                        return None, "free_quota"
            else:
                used = self.db.execute("SELECT COUNT(*) FROM ai_requests WHERE at>?", (now - 3600,)).fetchone()[0]
                if used >= limit:
                    return None, "hourly_limit"
                self.db.execute(
                    "INSERT INTO ai_requests(candidate_id,symbol,at,purpose) VALUES(?,?,?,?)",
                    (job, packet["symbol"], now, "context_" + route),
                )
            self.db.execute(
                "INSERT INTO ai_route_requests VALUES(?,?,?,?,?,'reserved',NULL)",
                (job, packet["episode_id"], route, now, canonical(state)),
            )
        return job, None

    def finish(self, job, status, review_id, retry_after=None):
        with self.db:
            row = self.db.execute("SELECT route FROM ai_route_requests WHERE job_id=?", (job,)).fetchone()
            self.db.execute(
                "UPDATE ai_route_requests SET status=?,review_id=? WHERE job_id=?", (status, review_id, job)
            )
            if status in {"http_429", "billing_mismatch"} and row:
                delay = retry_after if type(retry_after) in (int, float) and math.isfinite(retry_after) else 3600
                self.db.execute(
                    "INSERT OR REPLACE INTO ai_route_blocks VALUES(?,?)", (row[0], time.time() + max(1, delay))
                )

    def summary(self, now=None):
        now = time.time() if now is None else now
        return dict(
            free_enabled=os.getenv("SCALPER_FREE_ENABLED", "true").lower() == "true",
            free_model=self.free.model,
            free_verified=self.free_available,
            free_error=self.catalog_error,
            paid_model=getattr(self.paid, "model", None),
            free_hour_limit=12,
            free_day_limit=40,
            paid_hour_limit=40,
            requests=[
                dict(r)
                for r in self.db.execute(
                    "SELECT route,status,COUNT(*) AS count FROM ai_route_requests WHERE at>? GROUP BY route,status",
                    (now - 3600,),
                )
            ],
        )
