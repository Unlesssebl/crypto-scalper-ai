"""Durable event, assessment, delivery and forward-price journal."""
import json
import sqlite3
import time

from .audit import RULESET, update_move

SCHEMA = """
CREATE TABLE IF NOT EXISTS analyses (
    id TEXT PRIMARY KEY, symbol TEXT NOT NULL, window TEXT NOT NULL,
    captured_at REAL NOT NULL, event TEXT NOT NULL, snapshot TEXT NOT NULL,
    hyperdata TEXT NOT NULL, ai TEXT, consensus TEXT, analyzed_at REAL,
    delivery TEXT NOT NULL DEFAULT 'pending', delivered_at REAL
);
CREATE INDEX IF NOT EXISTS analyses_latest ON analyses(symbol, window, captured_at DESC);
CREATE INDEX IF NOT EXISTS analyses_captured_at ON analyses(captured_at);
CREATE TABLE IF NOT EXISTS observations (
    event_id TEXT REFERENCES analyses(id), minutes INTEGER, due_at REAL NOT NULL,
    observed_at REAL, price REAL, status TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY(event_id, minutes)
);
CREATE TABLE IF NOT EXISTS context_trials (
    event_id TEXT PRIMARY KEY REFERENCES analyses(id), baseline_ai TEXT NOT NULL, ready_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS context_trials_ready ON context_trials(ready_at);
CREATE TABLE IF NOT EXISTS context_observations (
    event_id TEXT REFERENCES context_trials(event_id), minutes INTEGER, due_at REAL NOT NULL,
    observed_at REAL, price REAL, status TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY(event_id, minutes)
);
CREATE INDEX IF NOT EXISTS context_observations_due ON context_observations(due_at) WHERE status = 'pending';
CREATE TABLE IF NOT EXISTS instrument_samples (
    symbol TEXT, window TEXT, slot INTEGER,
    trades REAL, natr REAL, spread_bps REAL, return_pct REAL, imbalance REAL,
    PRIMARY KEY(symbol, window, slot)
);
CREATE INDEX IF NOT EXISTS instrument_samples_slot ON instrument_samples(slot);
CREATE TABLE IF NOT EXISTS finding_candidates (
    id TEXT PRIMARY KEY, symbol TEXT NOT NULL, window TEXT NOT NULL, detected_at REAL NOT NULL,
    version TEXT NOT NULL, snapshot TEXT NOT NULL, event TEXT NOT NULL,
    decision TEXT NOT NULL, analysis_id TEXT UNIQUE REFERENCES analyses(id)
);
CREATE INDEX IF NOT EXISTS finding_candidates_detected ON finding_candidates(detected_at);
CREATE TABLE IF NOT EXISTS finding_decisions (
    candidate_id TEXT REFERENCES finding_candidates(id), at REAL NOT NULL, decision TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS finding_decisions_candidate ON finding_decisions(candidate_id);
CREATE TABLE IF NOT EXISTS delivered_moves (
    analysis_id TEXT PRIMARY KEY REFERENCES analyses(id), delivered_at REAL NOT NULL,
    rank INTEGER NOT NULL, target_pct REAL, anchor_price REAL, last_at REAL,
    peak_pct REAL, gap INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS delivered_moves_delivered ON delivered_moves(delivered_at);
CREATE INDEX IF NOT EXISTS delivered_moves_pending ON delivered_moves(analysis_id) WHERE status='pending';
CREATE TABLE IF NOT EXISTS quiet_moves (
    symbol TEXT NOT NULL, window TEXT NOT NULL, slot INTEGER NOT NULL,
    delivered_at REAL NOT NULL, target_pct REAL NOT NULL, anchor_price REAL, last_at REAL,
    peak_pct REAL, gap INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY(symbol,window,slot)
);
CREATE INDEX IF NOT EXISTS quiet_moves_pending ON quiet_moves(slot) WHERE status='pending';
CREATE INDEX IF NOT EXISTS quiet_moves_time ON quiet_moves(delivered_at);
CREATE TABLE IF NOT EXISTS telegram_offsets (
    bot_id INTEGER PRIMARY KEY, next_update INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS observations_due ON observations(due_at) WHERE status = 'pending';
"""


def dump(data):
    return json.dumps(data, ensure_ascii=False, allow_nan=False)


class Journal:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        # A separate SQLite lock allows readers of the WAL journal while
        # preventing a second service from recovering or delivering its jobs.
        self.lock = sqlite3.connect(path.with_suffix(path.suffix + ".owner.sqlite"), timeout=0)
        try:
            self.lock.execute("BEGIN EXCLUSIVE")
        except sqlite3.OperationalError as exc:
            self.lock.close()
            raise RuntimeError(f"Cannot acquire the journal lock for {path}") from exc
        try:
            self.db = sqlite3.connect(path, timeout=1)
            try:
                self.db.row_factory = sqlite3.Row
                self.db.execute("PRAGMA journal_mode=WAL")
                self.db.executescript(SCHEMA)
            except BaseException:
                self.db.close()
                raise
        except BaseException:
            self.lock.close()
            raise

    def recover(self):
        with self.db:
            self.db.execute("UPDATE analyses SET delivery='interrupted' WHERE delivery='pending' AND ai IS NULL")
            self.db.execute("UPDATE analyses SET delivery='unknown' WHERE delivery IN ('sending', 'pending')")
            candidates = self.db.execute(
                "SELECT id FROM finding_candidates WHERE decision IN ('queued','analyzing','delivery_pending')"
            ).fetchall()
        if candidates:
            now = time.time()
            for row in candidates:
                self.candidate_decision(row["id"], "interrupted", now)

    def record_candidate(self, key, snapshot, event, decision, now):
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO finding_candidates VALUES(?,?,?,?,?,?,?,?,NULL)",
                (key, snapshot["symbol"], snapshot["window"], now, RULESET,
                 dump(snapshot), dump(event), decision),
            ).rowcount
            if inserted:
                self.db.execute("INSERT INTO finding_decisions VALUES(?,?,?)", (key, now, decision))

    def candidate_decision(self, key, decision, now, analysis_id=None):
        with self.db:
            changed = self.db.execute(
                "UPDATE finding_candidates SET decision=?,analysis_id=COALESCE(?,analysis_id) "
                "WHERE id=? AND decision<>?", (decision, analysis_id, key, decision),
            ).rowcount
            if changed:
                self.db.execute("INSERT INTO finding_decisions VALUES(?,?,?)", (key, now, decision))

    def record(self, key, event, snapshot, hyperdata):
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO analyses(id,symbol,window,captured_at,event,snapshot,hyperdata)"
                " VALUES(?,?,?,?,?,?,?)",
                (key, snapshot["symbol"], snapshot["window"], snapshot["captured_at"],
                 dump(event), dump(snapshot), dump(hyperdata)),
            ).rowcount
            if inserted:
                self.db.executemany("INSERT INTO observations(event_id,minutes,due_at) VALUES(?,?,?)", (
                    (key, minutes, snapshot["captured_at"] + 60 * minutes) for minutes in (1, 5, 15)
                ))
        return bool(inserted)

    def finish(self, key, ai, consensus, now):
        with self.db:
            self.db.execute("UPDATE analyses SET ai=?,consensus=?,analyzed_at=? WHERE id=?",
                            (dump(ai), dump(consensus), now, key))

    def delivery(self, key, state, at=None):
        with self.db:
            self.db.execute("UPDATE analyses SET delivery=?,delivered_at=? WHERE id=?", (state, at, key))
            if state != "sending":
                decision = "delivered" if state == "sent" else "delivery_" + state
                candidates = self.db.execute(
                    "SELECT id FROM finding_candidates WHERE analysis_id=? AND decision<>?", (key, decision),
                ).fetchall()
                for candidate in candidates:
                    self.db.execute("UPDATE finding_candidates SET decision=? WHERE id=?", (decision, candidate["id"]))
                    self.db.execute("INSERT INTO finding_decisions VALUES(?,?,?)",
                                    (candidate["id"], at if at is not None else time.time(), decision))
            if state == "sent" and at is not None:
                self.db.execute("""
                    INSERT OR IGNORE INTO delivered_moves (analysis_id, delivered_at, rank, target_pct, status)
                    SELECT a.id, ?, COALESCE(json_extract(a.event,'$.strength.rank'),0),
                           json_extract(a.snapshot,'$.metrics.natr.value'),
                           CASE WHEN json_extract(a.snapshot,'$.metrics.natr.quality')='ok'
                                AND json_extract(a.snapshot,'$.metrics.natr.value')>0
                                THEN 'pending' ELSE 'no_natr' END
                    FROM analyses a JOIN finding_candidates c ON c.analysis_id=a.id WHERE a.id=?
                """, (at, key))

    def latest(self, symbol, window, limit=1, *, before=None, completed=False, accepted=False):
        sql = "SELECT * FROM analyses WHERE symbol=? AND window=?"
        params = [symbol, window]
        if before is not None:
            sql += " AND captured_at<?"
            params.append(before)
        if accepted:
            sql += " AND json_extract(ai, '$.status') = 'ok'"
        elif completed:
            sql += " AND ai IS NOT NULL"
        return [self.decode(row) for row in self.db.execute(
            sql + " ORDER BY captured_at DESC LIMIT ?", (*params, limit)
        )]

    def last_delivery(self, symbol, window):
        return self.db.execute(
            "SELECT MAX(delivered_at) FROM analyses WHERE symbol=? AND window=?",
            (symbol, window),
        ).fetchone()[0]

    def decode(self, row):
        data = dict(row)
        for key in ("event", "snapshot", "hyperdata", "ai", "consensus"):
            data[key] = json.loads(data[key]) if data[key] else None
        data["observations"] = [dict(item) for item in self.db.execute(
            "SELECT minutes,due_at,observed_at,price,status FROM observations WHERE event_id=? ORDER BY minutes",
            (data["id"],),
        )]
        return data

    def record_context_trial(self, key, baseline_ai, ready_at):
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO context_trials VALUES(?,?,?)", (key, dump(baseline_ai), ready_at),
            ).rowcount
            if inserted:
                self.db.executemany(
                    "INSERT INTO context_observations(event_id,minutes,due_at) VALUES(?,?,?)",
                    ((key, minutes, ready_at + 60 * minutes) for minutes in (0, 1, 5, 15)),
                )

    def observe(self, now, prices):
        with self.db:
            for table in ("observations", "context_observations"):
                self.db.execute(f"UPDATE {table} SET status='missed' WHERE status='pending' AND due_at<?",
                                (now - 30,))
                for row in self.db.execute(
                    f"SELECT o.*,a.symbol FROM {table} o JOIN analyses a ON o.event_id=a.id "
                    "WHERE o.status='pending' AND o.due_at<=?", (now,)
                ).fetchall():
                    value = prices.get(row["symbol"])
                    if value is None:
                        continue
                    price, observed_at = value
                    if row["due_at"] <= observed_at <= now:
                        self.db.execute(
                            f"UPDATE {table} SET status='observed',price=?,observed_at=? WHERE event_id=? AND minutes=?",
                            (price, observed_at, row["event_id"], row["minutes"]),
                        )

            for row in self.db.execute(
                "SELECT m.*,a.symbol FROM delivered_moves m JOIN analyses a ON a.id=m.analysis_id "
                "WHERE m.status='pending'"
            ).fetchall():
                state = update_move(row, now, prices.get(row["symbol"]))
                previous = tuple(row[key] for key in ("anchor_price", "last_at", "peak_pct", "gap", "status"))
                if state != previous:
                    self.db.execute(
                        "UPDATE delivered_moves SET anchor_price=?,last_at=?,peak_pct=?,gap=?,status=? WHERE analysis_id=?",
                        (*state, row["analysis_id"]),
                    )

            for row in self.db.execute("SELECT * FROM quiet_moves WHERE status='pending'").fetchall():
                state = update_move(row, now, prices.get(row["symbol"]))
                previous = tuple(row[key] for key in ("anchor_price", "last_at", "peak_pct", "gap", "status"))
                if state != previous:
                    self.db.execute(
                        "UPDATE quiet_moves SET anchor_price=?,last_at=?,peak_pct=?,gap=?,status=? "
                        "WHERE symbol=? AND window=? AND slot=?",
                        (*state, row["symbol"], row["window"], row["slot"]),
                    )

    def record_quiet_moves(self, samples):
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO quiet_moves(symbol,window,slot,delivered_at,target_pct) VALUES(?,?,?,?,?)",
                samples,
            )

    def record_instrument_samples(self, samples):
        with self.db:
            self.db.executemany("INSERT OR IGNORE INTO instrument_samples VALUES(?,?,?,?,?,?,?,?)", samples)

    def prune_instrument_samples(self, before):
        with self.db:
            self.db.execute("DELETE FROM instrument_samples WHERE slot < ?", (before,))
            self.db.execute("DELETE FROM quiet_moves WHERE delivered_at < ?", (before,))

    def telegram_offset(self, bot_id):
        row = self.db.execute("SELECT next_update FROM telegram_offsets WHERE bot_id=?", (bot_id,)).fetchone()
        return row[0] if row else 0

    def save_telegram_offset(self, bot_id, offset):
        with self.db:
            self.db.execute(
                "INSERT INTO telegram_offsets VALUES(?,?) ON CONFLICT(bot_id) DO UPDATE SET next_update=excluded.next_update",
                (bot_id, offset),
            )

    def close(self):
        try:
            self.db.close()
        finally:
            self.lock.close()
