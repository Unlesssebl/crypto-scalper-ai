"""Persistent shadow observations; no notification or AI decision side effects."""
from __future__ import annotations

import json

from hyperdata_terminal.market_pulse.models import VERSION, digest, fit, score, specification, stream

SCHEMA = """
CREATE TABLE IF NOT EXISTS pulse_profiles (
    id TEXT PRIMARY KEY, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pulse_evaluations (
    id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, observed_at REAL NOT NULL,
    baseline_id TEXT NOT NULL REFERENCES pulse_profiles(id), payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS pulse_evaluations_episode ON pulse_evaluations(episode_id, observed_at);
CREATE TABLE IF NOT EXISTS pulse_targets (
    id TEXT PRIMARY KEY, target_hash TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pulse_input_versions (
    id TEXT PRIMARY KEY, content_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pulse_recalc_targets (id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pulse_frozen_profiles (id TEXT PRIMARY KEY, baseline_id TEXT NOT NULL);
"""


class ShadowJournal:
    def __init__(self, db):
        self.db = db
        self.db.executescript(SCHEMA)
        self.profile_cache = {}

    def evaluate(self, rows, target, now):
        output = {"version": VERSION, "mode": "shadow", "live_approved": False, "metrics": {}}
        for metric in ("execution_count", "quote_volume"):
            reference = target["window_start_ms"] // 3_600_000 * 3_600_000
            cache_key = digest([list(stream(target)), specification(target), metric, reference, "hourly_frozen.v1"])
            profile = self.profile_cache.get(cache_key)
            if profile is None:
                saved = self.db.execute("SELECT p.payload FROM pulse_profiles p JOIN pulse_frozen_profiles f "
                                        "ON f.baseline_id=p.id WHERE f.id=?", (cache_key,)).fetchone()
                if saved:
                    profile = json.loads(saved[0])
                else:
                    profile = fit(rows, {**target, "window_start_ms": reference}, metric)
                    profile["reference_policy"] = "hourly_frozen.v1"
                    profile["baseline_version"] = digest(profile)
                self.profile_cache[cache_key] = profile
            past = [r for r in rows if stream(r) == stream(target)
                    and target["window_start_ms"] - 14 * 60_000 <= r["window_start_ms"]
                    < target["window_start_ms"]]
            # Online CUSUM requires a frozen reference and replay of its sequence.
            # Report it in offline holdout only; do not reset/re-fit it every minute.
            assessment = score(profile, target, past)
            assessment["candidates"].pop("cusum")
            assessment.pop("cusum")
            episode = digest([VERSION, list(stream(target)), target["window_start_ms"], metric])
            payload = {"episode_id": episode, "baseline_version": profile["baseline_version"],
                       "target": target, "local_inputs": past, "assessment": assessment,
                       "mode": "shadow", "live_approved": False}
            identity = digest(payload)
            previous = self.db.execute(
                "SELECT id FROM pulse_evaluations WHERE episode_id=? ORDER BY observed_at DESC,rowid DESC LIMIT 1",
                (episode,),
            ).fetchone()
            if previous and previous[0] != identity:
                payload["correction_of"] = previous[0]
                payload["correction_only"] = True
            with self.db:
                self.db.execute("INSERT OR IGNORE INTO pulse_profiles VALUES(?,?)",
                                (profile["baseline_version"], json.dumps(profile, allow_nan=False)))
                self.db.execute("INSERT OR REPLACE INTO pulse_frozen_profiles VALUES(?,?)",
                                (cache_key, profile["baseline_version"]))
                self.db.execute("INSERT OR IGNORE INTO pulse_evaluations VALUES(?,?,?,?,?)",
                                (identity, episode, now, profile["baseline_version"],
                                 json.dumps(payload, allow_nan=False)))
                if previous and previous[0] != identity:
                    self.correct_cards(episode, identity, target, assessment, now)
            output["metrics"][metric] = {
                **assessment, "evaluation_id": identity, "episode_id": episode,
                "baseline_version": profile["baseline_version"], "samples": profile["samples"],
                "coverage": profile["coverage"], "tail_samples": profile["tail_samples"],
            }
        key = digest([list(stream(target)), target["window_start_ms"]])
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO pulse_targets VALUES(?,?,?)",
                            (key, digest(target), json.dumps(target, allow_nan=False)))
        return output

    def pending_revisions(self):
        return self.db.execute("SELECT COUNT(*) FROM pulse_recalc_targets").fetchone()[0]

    def correct_cards(self, episode, identity, target, assessment, now):
        # Tests/research may use a standalone ledger without Telegram tables.
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE name='pulse_card_links'").fetchone():
            return
        cards = self.db.execute(
            "SELECT a.id,a.snapshot FROM pulse_card_links l JOIN analyses a ON a.id=l.event_id "
            "WHERE l.episode_id=? AND l.evaluation_id<>?", (episode, identity),
        ).fetchall()
        for key, raw in cards:
            snapshot = json.loads(raw)
            corrections = snapshot.setdefault("market_pulse_corrections", {})
            corrections[episode] = {"venue": target["venue"], "window_start_ms": target["window_start_ms"],
                                    "version": target["version"], "quality": target["quality"],
                                    "execution_count": target["execution_count"],
                                    "buy_quote": target.get("buy_quote"), "sell_quote": target.get("sell_quote"),
                                    "normalization_blocked": target.get("coverage", {}).get(
                                        "notional_blocked_events", 0),
                                    "evaluation_id": identity, "status": assessment["status"]}
            self.db.execute("UPDATE analyses SET snapshot=? WHERE id=?", (json.dumps(snapshot), key))
            self.db.execute("UPDATE pulse_card_links SET evaluation_id=? WHERE event_id=? AND episode_id=?",
                            (identity, key, episode))
            self.db.execute("INSERT OR REPLACE INTO pulse_card_edits VALUES(?,'pending',?)", (key, now))

    def reconcile(self, rows, now):
        """Append corrections to previously observed minutes; never emit a live event."""
        index = {digest([list(stream(r)), r["window_start_ms"]]): r for r in rows}
        known = dict(self.db.execute("SELECT id,content_hash FROM pulse_input_versions"))
        changed = [r for key, r in index.items() if known.get(key) != digest(r)]
        current_reference = int(now * 1000) // 3_600_000 * 3_600_000
        if any(r["window_start_ms"] < current_reference for r in changed):
            # Historical corrections invalidate references; new minutes do not
            # create another full seven-day profile on every scan.
            self.profile_cache.clear()
            with self.db:
                self.db.execute("DELETE FROM pulse_frozen_profiles")
        for key, old_hash in self.db.execute("SELECT id,target_hash FROM pulse_targets").fetchall():
            target = index.get(key)
            if target is None or target["window_start_ms"] < (int(now * 1000) - 86_400_000):
                continue
            depends = any(stream(r) == stream(target)
                          and target["window_start_ms"] - 10_080 * 60_000 <= r["window_start_ms"]
                          < target["window_start_ms"] for r in changed)
            if digest(target) != old_hash or depends:
                with self.db:
                    self.db.execute("INSERT OR IGNORE INTO pulse_recalc_targets VALUES(?)", (key,))
        with self.db:
            self.db.executemany("INSERT OR REPLACE INTO pulse_input_versions VALUES(?,?)",
                                [(digest([list(stream(r)), r["window_start_ms"]]), digest(r)) for r in changed])
        # A large historical correction must not monopolize the screener loop.
        for (key,) in self.db.execute("SELECT id FROM pulse_recalc_targets LIMIT 20").fetchall():
            target = index.get(key)
            if target is not None:
                self.evaluate(rows, target, now)
            with self.db:
                self.db.execute("DELETE FROM pulse_recalc_targets WHERE id=?", (key,))
