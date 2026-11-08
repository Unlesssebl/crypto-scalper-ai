"""PostgreSQL store for Market Pulse.

Requires the optional ``asyncpg`` extra: ``pip install 'hyperdata-terminal[pulse]'``.
The append transaction is the durability boundary: event rows, conflict rows,
and recalculation markers commit together.  A PostgreSQL advisory lock keeps the
single-writer rule even if an operator accidentally starts two ingests.
"""
from __future__ import annotations

import json
import time
from dataclasses import replace
from typing import Any, Iterable, Sequence

from .contracts import (
    WINDOW_MS,
    ExecutionEvent,
    Gap,
    InstrumentSpec,
    QualityState,
    WindowAggregate,
    affected_window,
)
from .storage import (
    RECENT_GAP_MS,
    AppendResult,
    ChannelCoverage,
    ConflictRecord,
    RawSegmentRecord,
    StoredEvent,
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS overview_candles (
    symbol TEXT NOT NULL, start_ms BIGINT NOT NULL, received_at_ms BIGINT NOT NULL,
    payload JSONB NOT NULL, PRIMARY KEY(symbol,start_ms)
);
CREATE INDEX IF NOT EXISTS overview_candles_time ON overview_candles(start_ms);
CREATE TABLE IF NOT EXISTS pulse_control (
    name TEXT PRIMARY KEY, updated_at_ms BIGINT NOT NULL, payload JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS event_quarantine (
    event_seq BIGINT PRIMARY KEY, reason TEXT NOT NULL, at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS attention_history (
    at_ms BIGINT PRIMARY KEY, payload JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS instrument_specs (
    venue text NOT NULL,
    native_market_id text NOT NULL,
    spec_version text NOT NULL,
    base_asset text NOT NULL,
    valid_from_ms bigint NOT NULL,
    valid_to_ms bigint,
    payload jsonb NOT NULL,
    PRIMARY KEY (venue, native_market_id, spec_version)
);

CREATE TABLE IF NOT EXISTS execution_events (
    event_seq bigserial PRIMARY KEY,
    venue text NOT NULL,
    native_market_id text NOT NULL,
    channel text NOT NULL,
    aggregation_kind text NOT NULL,
    trade_id text NOT NULL,
    event_time_ms bigint NOT NULL,
    receive_time_ms bigint NOT NULL,
    source_epoch text NOT NULL,
    side text NOT NULL,
    price numeric NOT NULL,
    native_amount numeric NOT NULL,
    base_amount numeric,
    quote_amount numeric,
    instrument_spec_version text NOT NULL,
    raw_ref text,
    fingerprint text NOT NULL,
    delivery_kind text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (venue, native_market_id, channel, aggregation_kind, trade_id)
);
CREATE INDEX IF NOT EXISTS execution_events_window_idx
    ON execution_events (venue, native_market_id, channel, aggregation_kind, event_time_ms);

CREATE TABLE IF NOT EXISTS event_conflicts (
    conflict_id bigserial PRIMARY KEY,
    venue text NOT NULL,
    native_market_id text NOT NULL,
    channel text NOT NULL,
    aggregation_kind text NOT NULL,
    trade_id text NOT NULL,
    existing_fingerprint text NOT NULL,
    incoming_fingerprint text NOT NULL,
    incoming_payload jsonb NOT NULL,
    detected_at_ms bigint NOT NULL,
    reason text NOT NULL,
    resolved boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS gaps (
    gap_id bigserial PRIMARY KEY,
    venue text NOT NULL,
    native_market_id text NOT NULL,
    channel text NOT NULL,
    source_epoch text NOT NULL,
    start_ms bigint NOT NULL,
    end_ms bigint,
    detected_at_ms bigint NOT NULL,
    state text NOT NULL,
    method text,
    reason text,
    CHECK (end_ms IS NULL OR end_ms >= start_ms)
);
CREATE INDEX IF NOT EXISTS gaps_window_idx ON gaps (venue, native_market_id, channel, start_ms, end_ms);

CREATE TABLE IF NOT EXISTS coverage (
    venue text NOT NULL,
    native_market_id text NOT NULL,
    channel text NOT NULL,
    source_epoch text NOT NULL,
    state text NOT NULL,
    updated_at_ms bigint NOT NULL,
    last_event_ms bigint,
    last_receive_ms bigint,
    heartbeat_ms bigint,
    connected_since_ms bigint,
    reason text,
    PRIMARY KEY (venue, native_market_id, channel)
);

CREATE TABLE IF NOT EXISTS windows (
    venue text NOT NULL,
    native_market_id text NOT NULL,
    channel text NOT NULL,
    aggregation_kind text NOT NULL,
    window_start_ms bigint NOT NULL,
    window_end_ms bigint NOT NULL,
    version integer NOT NULL,
    quality text NOT NULL,
    event_count integer NOT NULL,
    execution_count integer NOT NULL,
    provisional boolean NOT NULL,
    revised_at_ms bigint,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (venue, native_market_id, channel, aggregation_kind, window_start_ms)
);

CREATE TABLE IF NOT EXISTS recalc_queue (
    venue text NOT NULL,
    native_market_id text NOT NULL,
    channel text NOT NULL,
    aggregation_kind text NOT NULL,
    window_start_ms bigint NOT NULL,
    reason text NOT NULL,
    created_at_ms bigint NOT NULL,
    PRIMARY KEY (venue, native_market_id, channel, aggregation_kind, window_start_ms)
);

CREATE TABLE IF NOT EXISTS raw_segments (
    path text PRIMARY KEY,
    venue text NOT NULL,
    first_receive_ms bigint NOT NULL,
    last_receive_ms bigint NOT NULL,
    bytes bigint NOT NULL,
    sha256 text,
    sealed boolean NOT NULL,
    created_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_cursor (
    id integer PRIMARY KEY CHECK (id = 1),
    last_event_seq bigint NOT NULL
);
INSERT INTO ingest_cursor (id, last_event_seq) VALUES (1, 0) ON CONFLICT DO NOTHING;
"""

_APPEND_LOCK = 0x4D50554C5345


class PostgresStore:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str, *, min_size: int = 1, max_size: int = 4) -> "PostgresStore":
        try:
            import asyncpg
        except ImportError as exc:  # pragma: no cover - exercised only without the extra
            raise RuntimeError(
                "PostgreSQL support requires asyncpg; install 'hyperdata-terminal[pulse]'"
            ) from exc
        pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size, command_timeout=30)
        store = cls(pool)
        await store.initialize()
        return store

    async def initialize(self) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(SCHEMA_SQL)

    async def put_overview(self, rows, now_ms):
        async with self.pool.acquire() as connection, connection.transaction():
            await connection.executemany("""INSERT INTO overview_candles VALUES($1,$2,$3,$4::jsonb)
                ON CONFLICT(symbol,start_ms) DO UPDATE SET received_at_ms=EXCLUDED.received_at_ms,
                payload=EXCLUDED.payload WHERE overview_candles.received_at_ms<=EXCLUDED.received_at_ms""",
                [(r["symbol"], r["start_ms"], r["received_at_ms"], json.dumps(r)) for r in rows])
            if now_ms - getattr(self, "_overview_pruned", 0) >= 3_600_000:
                await connection.execute("DELETE FROM overview_candles WHERE start_ms<$1", now_ms - 30*86400_000)
                self._overview_pruned = now_ms

    async def get_overview(self, from_ms):
        async with self.pool.acquire() as connection:
            rows = await connection.fetch("SELECT payload FROM overview_candles WHERE start_ms>=$1 "
                                          "ORDER BY start_ms,symbol LIMIT 16000", from_ms)
        return [json.loads(r["payload"]) if isinstance(r["payload"], str) else dict(r["payload"]) for r in rows]

    async def event_head(self):
        async with self.pool.acquire() as connection:
            return await connection.fetchval("SELECT COALESCE(MAX(event_seq),0) FROM execution_events")

    async def get_control(self, name):
        async with self.pool.acquire() as connection:
            raw = await connection.fetchval("SELECT payload FROM pulse_control WHERE name=$1", name)
        return None if raw is None else _json(raw)

    async def resources(self):
        async with self.pool.acquire() as connection:
            size = await connection.fetchval("SELECT pg_database_size(current_database())")
            oldest = await connection.fetchval("SELECT MIN(window_start_ms) FROM recalc_queue")
        return {"database_bytes": size, "oldest_recalc_window_ms": oldest}

    async def quarantine_events(self, sequences, reason, now_ms):
        if not sequences:
            return 0
        async with self.pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT pg_advisory_xact_lock($1)", _APPEND_LOCK)
            inserted = await connection.fetch("""INSERT INTO event_quarantine
                SELECT event_seq,$2,$3 FROM execution_events WHERE event_seq=ANY($1::bigint[])
                ON CONFLICT DO NOTHING RETURNING event_seq""", sequences, reason, now_ms)
            keys = await connection.fetch("""SELECT DISTINCT venue,native_market_id,channel,aggregation_kind,
                event_time_ms/60000*60000 AS minute FROM execution_events WHERE event_seq=ANY($1::bigint[])""",
                                          [r["event_seq"] for r in inserted])
            for row in keys:
                key = (row["venue"], row["native_market_id"], row["channel"], row["aggregation_kind"], row["minute"])
                await connection.execute("""UPDATE windows SET quality='gap_detected',version=version+1,
                    payload=jsonb_set(jsonb_set(payload,'{quality}',to_jsonb('gap_detected'::text)),
                                      '{version}',to_jsonb(version+1))
                    WHERE venue=$1 AND native_market_id=$2 AND channel=$3 AND aggregation_kind=$4
                    AND window_start_ms=$5""", *key)
                await self._queue_recalc(connection, key, "raw_identity_quarantine", now_ms)
                await connection.execute("""INSERT INTO gaps
                    (venue,native_market_id,channel,source_epoch,start_ms,end_ms,detected_at_ms,state,reason)
                    VALUES($1,$2,$3,'raw-identity-audit-v1',$4,$5,$6,'gap_detected',$7)""",
                                         key[0], key[1], key[2], key[4], key[4]+60000, now_ms, reason)
        return len(inserted)

    async def set_control(self, name, payload, now_ms):
        async with self.pool.acquire() as connection, connection.transaction():
            row = await connection.fetchval(
                "INSERT INTO pulse_control VALUES($1,$2,$3::jsonb) "
                "ON CONFLICT(name) DO UPDATE SET updated_at_ms=EXCLUDED.updated_at_ms,payload=EXCLUDED.payload "
                "WHERE pulse_control.updated_at_ms<=EXCLUDED.updated_at_ms RETURNING payload",
                name, now_ms, json.dumps(payload))
            if row is not None and name == "attention":
                await connection.execute("INSERT INTO attention_history VALUES($1,$2::jsonb) ON CONFLICT DO NOTHING",
                                         now_ms, json.dumps(payload))
        return row is not None

    async def register_instruments(self, specs: Iterable[InstrumentSpec]) -> None:
        rows = [
            (
                spec.venue,
                spec.native_market_id,
                spec.spec_version,
                spec.base_asset,
                spec.valid_from_ms,
                spec.valid_to_ms,
                json.dumps(spec.to_payload()),
            )
            for spec in specs
        ]
        if not rows:
            return
        async with self.pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO instrument_specs
                    (venue, native_market_id, spec_version, base_asset,
                     valid_from_ms, valid_to_ms, payload)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                ON CONFLICT (venue, native_market_id, spec_version) DO NOTHING
                """,
                rows,
            )

    async def get_instrument_spec(
        self, venue: str, native_market_id: str, at_ms: int
    ) -> InstrumentSpec | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT payload FROM instrument_specs
                WHERE venue=$1 AND native_market_id=$2
                  AND valid_from_ms <= $3
                  AND (valid_to_ms IS NULL OR valid_to_ms > $3)
                ORDER BY valid_from_ms DESC, spec_version DESC
                LIMIT 1
                """,
                venue,
                native_market_id,
                at_ms,
            )
        return None if row is None else InstrumentSpec.from_payload(_json(row["payload"]))

    async def append_events(self, events: Sequence[ExecutionEvent], *, now_ms: int) -> AppendResult:
        accepted: list[ExecutionEvent] = []
        sequences: list[int] = []
        duplicates = 0
        conflicts = 0
        affected: set[tuple[str, str, str, str, int]] = set()
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute("SELECT pg_advisory_xact_lock($1)", _APPEND_LOCK)
                for event in events:
                    fingerprint = event.content_fingerprint()
                    row = await connection.fetchrow(
                        """
                        INSERT INTO execution_events
                            (venue, native_market_id, channel, aggregation_kind, trade_id,
                             event_time_ms, receive_time_ms, source_epoch, side, price,
                             native_amount, base_amount, quote_amount,
                             instrument_spec_version, raw_ref, fingerprint, delivery_kind, payload)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                                $11, $12, $13, $14, $15, $16, $17, $18::jsonb)
                        ON CONFLICT (venue, native_market_id, channel, aggregation_kind, trade_id)
                        DO NOTHING
                        RETURNING event_seq
                        """,
                        event.venue,
                        event.native_market_id,
                        event.channel,
                        event.aggregation_kind,
                        event.trade_id,
                        event.event_time_ms,
                        event.receive_time_ms,
                        event.source_epoch,
                        event.side,
                        event.price,
                        event.native_amount,
                        event.base_amount,
                        event.quote_amount,
                        event.instrument_spec_version,
                        event.raw_ref,
                        fingerprint,
                        event.delivery_kind,
                        json.dumps(event.to_payload()),
                    )
                    if row is not None:
                        sequence = int(row["event_seq"])
                        accepted.append(event)
                        sequences.append(sequence)
                        affected.add(affected_window(event))
                        await self._queue_recalc(connection, affected_window(event), "accepted", now_ms)
                        continue
                    existing = await connection.fetchrow(
                        """
                        SELECT event_seq, fingerprint, event_time_ms FROM execution_events
                        WHERE venue=$1 AND native_market_id=$2 AND channel=$3
                          AND aggregation_kind=$4 AND trade_id=$5
                        """,
                        event.venue,
                        event.native_market_id,
                        event.channel,
                        event.aggregation_kind,
                        event.trade_id,
                    )
                    if existing is not None and existing["fingerprint"] == fingerprint:
                        duplicates += 1
                        continue
                    conflicts += 1
                    # Both the stored minute and the incoming minute are
                    # affected: the true execution time is now ambiguous.
                    keys = {affected_window(event)}
                    if existing is not None:
                        old_time = int(existing["event_time_ms"])
                        keys.add(
                            (
                                event.venue,
                                event.native_market_id,
                                event.channel,
                                event.aggregation_kind,
                                old_time // WINDOW_MS * WINDOW_MS,
                            )
                        )
                    for key in keys:
                        affected.add(key)
                        await connection.execute(
                            """
                            INSERT INTO gaps
                                (venue, native_market_id, channel, source_epoch, start_ms, end_ms,
                                 detected_at_ms, state, reason)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            """,
                            event.venue,
                            event.native_market_id,
                            event.channel,
                            event.source_epoch,
                            key[4],
                            key[4] + WINDOW_MS,
                            now_ms,
                            QualityState.GAP_DETECTED.value,
                            f"trade id conflict: {event.trade_id}",
                        )
                        await self._queue_recalc(connection, key, "conflict", now_ms)
                    errors = await connection.execute(
                        """
                        INSERT INTO event_conflicts
                            (venue, native_market_id, channel, aggregation_kind, trade_id,
                             existing_fingerprint, incoming_fingerprint, incoming_payload,
                             detected_at_ms, reason)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)
                        """,
                        event.venue,
                        event.native_market_id,
                        event.channel,
                        event.aggregation_kind,
                        event.trade_id,
                        "" if existing is None else existing["fingerprint"],
                        fingerprint,
                        json.dumps(event.to_payload()),
                        now_ms,
                        "same dedup key with different content",
                    )
                    _ = errors
                cursor = await connection.fetchval("SELECT COALESCE(MAX(event_seq), 0) FROM execution_events")
                await connection.execute(
                    "UPDATE ingest_cursor SET last_event_seq=$1 WHERE id=1",
                    int(cursor or 0),
                )
        return AppendResult(
            accepted=tuple(accepted),
            accepted_event_sequences=tuple(sequences),
            duplicates=duplicates,
            conflicts=conflicts,
            affected=tuple(sorted(affected)),
            cursor=int(cursor or 0),
        )

    async def _queue_recalc(
        self,
        connection: Any,
        key: tuple[str, str, str, str, int],
        reason: str,
        now_ms: int,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO recalc_queue
                (venue, native_market_id, channel, aggregation_kind, window_start_ms, reason, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
            """,
            key[0],
            key[1],
            key[2],
            key[3],
            key[4],
            reason,
            now_ms,
        )

    async def list_events_for_window(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
    ) -> list[ExecutionEvent]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT payload FROM execution_events
                WHERE venue=$1 AND native_market_id=$2 AND channel=$3
                  AND aggregation_kind=$4 AND event_time_ms >= $5 AND event_time_ms < $6
                  AND NOT EXISTS(SELECT 1 FROM event_quarantine q WHERE q.event_seq=execution_events.event_seq)
                ORDER BY event_time_ms, event_seq
                """,
                venue,
                native_market_id,
                channel,
                aggregation_kind,
                window_start_ms,
                window_start_ms + WINDOW_MS,
            )
        return [ExecutionEvent.from_payload(_json(row["payload"])) for row in rows]

    async def list_events_after(
        self,
        cursor: int,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 500,
    ) -> list[StoredEvent]:
        clauses = ["event_seq > $1", "NOT EXISTS(SELECT 1 FROM event_quarantine q "
                   "WHERE q.event_seq=execution_events.event_seq)"]
        args: list[Any] = [cursor]
        if venue is not None:
            args.append(venue)
            clauses.append(f"venue = ${len(args)}")
        if native_market_id is not None:
            args.append(native_market_id)
            clauses.append(f"native_market_id = ${len(args)}")
        if from_ms is not None:
            args.append(from_ms)
            clauses.append(f"event_time_ms >= ${len(args)}")
        if to_ms is not None:
            args.append(to_ms)
            clauses.append(f"event_time_ms < ${len(args)}")
        args.append(limit)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT event_seq, payload FROM execution_events
                WHERE {' AND '.join(clauses)}
                ORDER BY event_seq
                LIMIT ${len(args)}
                """,
                *args,
            )
        return [
            StoredEvent(int(row["event_seq"]), ExecutionEvent.from_payload(_json(row["payload"])))
            for row in rows
        ]

    async def upsert_window(self, window: WindowAggregate) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO windows
                    (venue, native_market_id, channel, aggregation_kind, window_start_ms,
                     window_end_ms, version, quality, event_count, execution_count,
                     provisional, revised_at_ms, payload, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, now())
                ON CONFLICT (venue, native_market_id, channel, aggregation_kind, window_start_ms)
                DO UPDATE SET
                    window_end_ms = EXCLUDED.window_end_ms,
                    version = EXCLUDED.version,
                    quality = EXCLUDED.quality,
                    event_count = EXCLUDED.event_count,
                    execution_count = EXCLUDED.execution_count,
                    provisional = EXCLUDED.provisional,
                    revised_at_ms = EXCLUDED.revised_at_ms,
                    payload = EXCLUDED.payload,
                    updated_at = now()
                """,
                window.venue,
                window.native_market_id,
                window.channel,
                window.aggregation_kind,
                window.window_start_ms,
                window.window_end_ms,
                window.version,
                window.quality,
                window.event_count,
                window.execution_count,
                window.provisional,
                window.revised_at_ms,
                json.dumps(window.to_payload()),
            )

    async def get_window(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
    ) -> WindowAggregate | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT payload FROM windows
                WHERE venue=$1 AND native_market_id=$2 AND channel=$3
                  AND aggregation_kind=$4 AND window_start_ms=$5
                """,
                venue,
                native_market_id,
                channel,
                aggregation_kind,
                window_start_ms,
            )
        return None if row is None else WindowAggregate.from_payload(_json(row["payload"]))

    async def list_windows(
        self,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 500,
    ) -> list[WindowAggregate]:
        clauses = ["1=1"]
        args: list[Any] = []
        if venue is not None:
            args.append(venue)
            clauses.append(f"venue = ${len(args)}")
        if native_market_id is not None:
            args.append(native_market_id)
            clauses.append(f"native_market_id = ${len(args)}")
        if from_ms is not None:
            args.append(from_ms)
            clauses.append(f"window_start_ms >= ${len(args)}")
        if to_ms is not None:
            args.append(to_ms)
            clauses.append(f"window_start_ms < ${len(args)}")
        args.append(limit)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT payload FROM windows
                WHERE {' AND '.join(clauses)}
                ORDER BY window_start_ms, venue, native_market_id
                LIMIT ${len(args)}
                """,
                *args,
            )
        return [WindowAggregate.from_payload(_json(row["payload"])) for row in rows]

    async def due_windows(self, now_ms, grace_ms):
        async with self.pool.acquire() as connection:
            rows = await connection.fetch("""SELECT payload FROM windows
                WHERE window_start_ms+60000+$2 <= $1 AND
                ((payload->>'provisional')::boolean OR
                 (payload->>'quality'='partial' AND
                  payload->'coverage'->>'partial_reason'='no liveness at window end'))
                ORDER BY (payload->>'provisional')::boolean DESC,window_start_ms DESC LIMIT 1000""",
                                          now_ms, grace_ms)
        return [WindowAggregate.from_payload(_json(r["payload"])) for r in rows]

    async def mark_window_quality(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
        quality: str,
        reason: str,
        *,
        now_ms: int,
    ) -> None:
        current = await self.get_window(
            venue, native_market_id, channel, aggregation_kind, window_start_ms
        )
        if current is None or current.quality == quality:
            return
        await self.upsert_window(
            replace(
                current,
                quality=quality,
                version=current.version + 1,
                revised_at_ms=now_ms,
                coverage={**(current.coverage or {}), "quality_reason": reason},
            )
        )

    async def record_conflict(self, conflict: ConflictRecord) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO event_conflicts
                    (venue, native_market_id, channel, aggregation_kind, trade_id,
                     existing_fingerprint, incoming_fingerprint, incoming_payload,
                     detected_at_ms, reason, resolved)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11)
                """,
                conflict.venue,
                conflict.native_market_id,
                conflict.channel,
                conflict.aggregation_kind,
                conflict.trade_id,
                conflict.existing_fingerprint,
                conflict.incoming_fingerprint,
                json.dumps(conflict.incoming_payload),
                conflict.detected_at_ms,
                conflict.reason,
                conflict.resolved,
            )

    async def list_conflicts(
        self, *, venue: str | None = None, native_market_id: str | None = None, limit: int = 200
    ) -> list[ConflictRecord]:
        clauses = ["1=1"]
        args: list[Any] = []
        if venue is not None:
            args.append(venue)
            clauses.append(f"venue = ${len(args)}")
        if native_market_id is not None:
            args.append(native_market_id)
            clauses.append(f"native_market_id = ${len(args)}")
        args.append(limit)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT * FROM event_conflicts WHERE {' AND '.join(clauses)}
                ORDER BY conflict_id DESC LIMIT ${len(args)}
                """,
                *args,
            )
        return [
            ConflictRecord(
                venue=row["venue"],
                native_market_id=row["native_market_id"],
                channel=row["channel"],
                aggregation_kind=row["aggregation_kind"],
                trade_id=row["trade_id"],
                existing_fingerprint=row["existing_fingerprint"],
                incoming_fingerprint=row["incoming_fingerprint"],
                incoming_payload=_json(row["incoming_payload"]),
                detected_at_ms=int(row["detected_at_ms"]),
                reason=row["reason"],
                resolved=bool(row["resolved"]),
            )
            for row in rows
        ]

    async def add_gap(self, gap: Gap) -> Gap:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO gaps
                    (venue, native_market_id, channel, source_epoch, start_ms, end_ms,
                     detected_at_ms, state, method, reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING *
                """,
                gap.venue,
                gap.native_market_id,
                gap.channel,
                gap.source_epoch,
                gap.start_ms,
                gap.end_ms,
                gap.detected_at_ms,
                gap.state,
                gap.method,
                gap.reason,
            )
        return _gap_from_row(row)

    async def update_gap(self, gap_id: int, **changes: Any) -> Gap | None:
        allowed = {"end_ms", "state", "method", "reason"}
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError(f"unsupported gap fields: {sorted(unknown)}")
        if not changes:
            return None
        sets = []
        args: list[Any] = []
        for name, value in changes.items():
            args.append(value)
            sets.append(f"{name} = ${len(args)}")
        args.append(gap_id)
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"UPDATE gaps SET {', '.join(sets)} WHERE gap_id = ${len(args)} RETURNING *",
                *args,
            )
        return None if row is None else _gap_from_row(row)

    async def list_gaps(
        self,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        channel: str | None = None,
        state: str | None = None,
        limit: int = 500,
    ) -> list[Gap]:
        clauses = ["1=1"]
        args: list[Any] = []
        for name, value in (
            ("venue", venue),
            ("native_market_id", native_market_id),
            ("channel", channel),
            ("state", state),
        ):
            if value is not None:
                args.append(value)
                clauses.append(f"{name} = ${len(args)}")
        args.append(limit)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT * FROM gaps WHERE {' AND '.join(clauses)}
                ORDER BY start_ms, gap_id LIMIT ${len(args)}
                """,
                *args,
            )
        return [_gap_from_row(row) for row in rows]

    async def gaps_overlapping(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        start_ms: int,
        end_ms: int,
    ) -> list[Gap]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT * FROM gaps
                WHERE venue=$1 AND native_market_id=$2 AND channel=$3
                  AND start_ms < $5 AND (end_ms IS NULL OR GREATEST(end_ms, start_ms + 1) > $4)
                ORDER BY start_ms, gap_id
                """,
                venue,
                native_market_id,
                channel,
                start_ms,
                end_ms,
            )
        return [_gap_from_row(row) for row in rows]

    async def set_coverage(self, coverage: ChannelCoverage) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO coverage
                    (venue, native_market_id, channel, source_epoch, state, updated_at_ms,
                     last_event_ms, last_receive_ms, heartbeat_ms, connected_since_ms, reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (venue, native_market_id, channel) DO UPDATE SET
                    source_epoch = EXCLUDED.source_epoch,
                    state = EXCLUDED.state,
                    updated_at_ms = EXCLUDED.updated_at_ms,
                    last_event_ms = EXCLUDED.last_event_ms,
                    last_receive_ms = EXCLUDED.last_receive_ms,
                    heartbeat_ms = EXCLUDED.heartbeat_ms,
                    connected_since_ms = EXCLUDED.connected_since_ms,
                    reason = EXCLUDED.reason
                """,
                coverage.venue,
                coverage.native_market_id,
                coverage.channel,
                coverage.source_epoch,
                coverage.state,
                coverage.updated_at_ms,
                coverage.last_event_ms,
                coverage.last_receive_ms,
                coverage.heartbeat_ms,
                coverage.connected_since_ms,
                coverage.reason,
            )

    async def get_coverage(self, venue: str, native_market_id: str, channel: str) -> ChannelCoverage | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM coverage WHERE venue=$1 AND native_market_id=$2 AND channel=$3",
                venue,
                native_market_id,
                channel,
            )
        return None if row is None else _coverage_from_row(row)

    async def list_coverage(self) -> list[ChannelCoverage]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT * FROM coverage ORDER BY venue, native_market_id, channel"
            )
        return [_coverage_from_row(row) for row in rows]

    async def record_raw_segment(self, segment: RawSegmentRecord) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO raw_segments
                    (path, venue, first_receive_ms, last_receive_ms, bytes, sha256, sealed, created_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (path) DO UPDATE SET
                    last_receive_ms = EXCLUDED.last_receive_ms,
                    bytes = EXCLUDED.bytes,
                    sha256 = EXCLUDED.sha256,
                    sealed = EXCLUDED.sealed
                """,
                segment.path,
                segment.venue,
                segment.first_receive_ms,
                segment.last_receive_ms,
                segment.bytes,
                segment.sha256,
                segment.sealed,
                segment.created_at_ms,
            )

    async def list_raw_segments(self, *, venue: str | None = None, limit: int = 200) -> list[RawSegmentRecord]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT * FROM raw_segments
                WHERE ($1::text IS NULL OR venue = $1)
                ORDER BY created_at_ms DESC LIMIT $2
                """,
                venue,
                limit,
            )
        return [
            RawSegmentRecord(
                venue=row["venue"],
                path=row["path"],
                first_receive_ms=int(row["first_receive_ms"]),
                last_receive_ms=int(row["last_receive_ms"]),
                bytes=int(row["bytes"]),
                sha256=row["sha256"],
                sealed=bool(row["sealed"]),
                created_at_ms=int(row["created_at_ms"]),
            )
            for row in rows
        ]

    async def pending_recalc(self, limit: int = 1_000) -> list[tuple[str, str, str, str, int]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT venue, native_market_id, channel, aggregation_kind, window_start_ms
                FROM recalc_queue ORDER BY created_at_ms, window_start_ms LIMIT $1
                """,
                limit,
            )
        return [
            (
                row["venue"],
                row["native_market_id"],
                row["channel"],
                row["aggregation_kind"],
                int(row["window_start_ms"]),
            )
            for row in rows
        ]

    async def complete_recalc(self, affected: Iterable[tuple[str, str, str, str, int]]) -> None:
        rows = list(affected)
        if not rows:
            return
        async with self.pool.acquire() as connection:
            await connection.executemany(
                """
                DELETE FROM recalc_queue
                WHERE venue=$1 AND native_market_id=$2 AND channel=$3
                  AND aggregation_kind=$4 AND window_start_ms=$5
                """,
                rows,
            )

    async def health(self, now_ms: int | None = None) -> dict[str, Any]:
        now_ms = int(time.time() * 1_000) if now_ms is None else now_ms
        async with self.pool.acquire() as connection:
            events = await connection.fetchval("SELECT COUNT(*) FROM execution_events")
            windows = await connection.fetchval("SELECT COUNT(*) FROM windows")
            cursor = await connection.fetchval("SELECT COALESCE(MAX(event_seq), 0) FROM execution_events")
            conflicts = await connection.fetchval("SELECT COUNT(*) FROM event_conflicts")
            open_gaps = await connection.fetchval(
                "SELECT COUNT(*) FROM gaps WHERE state = $1",
                QualityState.GAP_DETECTED.value,
            )
            recent_gaps = await connection.fetchval(
                "SELECT COUNT(*) FROM gaps WHERE state = $1 AND detected_at_ms >= $2",
                QualityState.GAP_DETECTED.value,
                now_ms - RECENT_GAP_MS,
            )
            declared_outages = await connection.fetchval(
                "SELECT COUNT(*) FROM gaps WHERE state = $1",
                QualityState.DECLARED_OUTAGE.value,
            )
            unavailable = await connection.fetchval(
                "SELECT COUNT(*) FROM coverage WHERE state = $1",
                QualityState.UNAVAILABLE.value,
            )
            recalc = await connection.fetchval("SELECT COUNT(*) FROM recalc_queue")
            quarantined = await connection.fetchval("SELECT COUNT(*) FROM event_quarantine")
        return {
            "status": "degraded" if recent_gaps or unavailable or conflicts or quarantined else "ok",
            "quarantined_events": int(quarantined or 0),
            "events": int(events or 0),
            "windows": int(windows or 0),
            "cursor": int(cursor or 0),
            "conflicts": int(conflicts or 0),
            "open_gaps": int(open_gaps or 0),
            "recent_open_gaps": int(recent_gaps or 0),
            "historical_open_gaps": int(open_gaps or 0) - int(recent_gaps or 0),
            "declared_outages": int(declared_outages or 0),
            "unavailable_channels": int(unavailable or 0),
            "pending_recalc": int(recalc or 0),
        }

    async def close(self) -> None:
        await self.pool.close()


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return json.loads(value)


def _gap_from_row(row: Any) -> Gap:
    return Gap(
        gap_id=int(row["gap_id"]),
        venue=row["venue"],
        native_market_id=row["native_market_id"],
        channel=row["channel"],
        source_epoch=row["source_epoch"],
        start_ms=int(row["start_ms"]),
        end_ms=None if row["end_ms"] is None else int(row["end_ms"]),
        detected_at_ms=int(row["detected_at_ms"]),
        state=row["state"],
        method=row["method"],
        reason=row["reason"],
    )


def _coverage_from_row(row: Any) -> ChannelCoverage:
    return ChannelCoverage(
        venue=row["venue"],
        native_market_id=row["native_market_id"],
        channel=row["channel"],
        source_epoch=row["source_epoch"],
        state=row["state"],
        updated_at_ms=int(row["updated_at_ms"]),
        last_event_ms=None if row["last_event_ms"] is None else int(row["last_event_ms"]),
        last_receive_ms=None if row["last_receive_ms"] is None else int(row["last_receive_ms"]),
        heartbeat_ms=None if row["heartbeat_ms"] is None else int(row["heartbeat_ms"]),
        connected_since_ms=None if row["connected_since_ms"] is None else int(row["connected_since_ms"]),
        reason=row["reason"],
    )
