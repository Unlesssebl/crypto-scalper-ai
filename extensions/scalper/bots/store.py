"""SQLite storage for bot contracts, observations and forward outcomes.

All tables live in the existing screener journal database.  Every id is derived
from immutable inputs, so a service restart cannot create a second observation
for the same episode.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_versions (
    bot_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    contract_hash TEXT NOT NULL,
    contract TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (bot_id, version)
);

CREATE TABLE IF NOT EXISTS bot_observations (
    id TEXT PRIMARY KEY,
    bot_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    trigger_event_id TEXT NOT NULL,
    control_set_id TEXT,
    symbol TEXT NOT NULL,
    window TEXT NOT NULL,
    mode TEXT NOT NULL,
    hypothesis TEXT NOT NULL,
    strength INTEGER NOT NULL,
    direction INTEGER NOT NULL,
    decided_at REAL NOT NULL,
    entry_due_at REAL NOT NULL,
    entry_price REAL,
    entry_at REAL,
    entry_source TEXT,
    trigger_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'observed',
    unscored_reason TEXT,
    repeat_count INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'live',
    UNIQUE (bot_id, version, trigger_event_id)
);
CREATE INDEX IF NOT EXISTS bot_observations_bot ON bot_observations(bot_id, version, decided_at);
CREATE INDEX IF NOT EXISTS bot_observations_entry ON bot_observations(entry_due_at)
    WHERE entry_price IS NULL;

CREATE TABLE IF NOT EXISTS bot_state (
    bot_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    window TEXT NOT NULL,
    direction INTEGER NOT NULL,
    episode_id TEXT,
    started_at REAL,
    last_seen REAL NOT NULL,
    PRIMARY KEY (bot_id, version, symbol, window)
);

CREATE TABLE IF NOT EXISTS bot_outcomes (
    observation_id TEXT NOT NULL,
    horizon_s INTEGER NOT NULL,
    exit_due_at REAL NOT NULL,
    exit_price REAL,
    exit_at REAL,
    exit_source TEXT,
    move_bps REAL,
    mae_bps REAL,
    mfe_bps REAL,
    path_points INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    unscored_reason TEXT,
    PRIMARY KEY (observation_id, horizon_s)
);
CREATE INDEX IF NOT EXISTS bot_outcomes_due ON bot_outcomes(exit_due_at)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS bot_outcomes_priced ON bot_outcomes(observation_id)
    WHERE status = 'priced';

CREATE TABLE IF NOT EXISTS bot_path_points (
    observation_id TEXT NOT NULL,
    at REAL NOT NULL,
    price REAL NOT NULL,
    PRIMARY KEY (observation_id, at)
);

CREATE TABLE IF NOT EXISTS bot_control_sets (
    id TEXT PRIMARY KEY,
    trigger_event_id TEXT NOT NULL,
    direction INTEGER NOT NULL,
    mode TEXT NOT NULL,
    symbols TEXT NOT NULL,
    bucket TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS bot_control_points (
    control_set_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    horizon_s INTEGER NOT NULL,
    entry_due_at REAL NOT NULL,
    exit_due_at REAL NOT NULL,
    entry_price REAL,
    entry_at REAL,
    exit_price REAL,
    exit_at REAL,
    move_bps REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    unscored_reason TEXT,
    PRIMARY KEY (control_set_id, symbol, horizon_s)
);
CREATE INDEX IF NOT EXISTS bot_control_points_due ON bot_control_points(entry_due_at, exit_due_at)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS bot_control_points_priced ON bot_control_points(control_set_id)
    WHERE status = 'priced';
"""


def dumps(data) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def derive_id(*parts) -> str:
    raw = "\x1f".join(str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    # A prototype journal may predate the live/replay split.  Keep the
    # migration additive: every existing row is live by construction.
    columns = {row[1] for row in db.execute("PRAGMA table_info(bot_observations)").fetchall()}
    if "source" not in columns:
        with db:
            db.execute("ALTER TABLE bot_observations ADD COLUMN source TEXT NOT NULL DEFAULT 'live'")


def register_spec(db: sqlite3.Connection, spec, now: float) -> None:
    """Register a contract version; refuse to reinterpret an existing one."""
    row = db.execute(
        "SELECT contract_hash FROM bot_versions WHERE bot_id=? AND version=?",
        (spec.id, spec.version),
    ).fetchone()
    if row is not None:
        if row[0] != spec.contract_hash:
            raise ValueError(
                f"Bot contract {spec.id} v{spec.version} changed without a version bump "
                f"({row[0]} -> {spec.contract_hash})"
            )
        return
    with db:
        db.execute(
            "INSERT INTO bot_versions(bot_id, version, contract_hash, contract, created_at) "
            "VALUES(?,?,?,?,?)",
            (spec.id, spec.version, spec.contract_hash, dumps(spec.payload()), now),
        )


def state_row(db: sqlite3.Connection, spec, symbol: str, window: str):
    return db.execute(
        "SELECT * FROM bot_state WHERE bot_id=? AND version=? AND symbol=? AND window=?",
        (spec.id, spec.version, symbol, window),
    ).fetchone()


def save_state(
    db: sqlite3.Connection,
    spec,
    symbol: str,
    window: str,
    *,
    direction: int,
    episode_id: str | None,
    started_at: float | None,
    last_seen: float,
) -> None:
    with db:
        db.execute(
            "INSERT INTO bot_state(bot_id, version, symbol, window, direction, episode_id, started_at, last_seen) "
            "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(bot_id, version, symbol, window) DO UPDATE SET "
            "direction=excluded.direction, episode_id=excluded.episode_id, "
            "started_at=excluded.started_at, last_seen=excluded.last_seen",
            (spec.id, spec.version, symbol, window, direction, episode_id, started_at, last_seen),
        )
