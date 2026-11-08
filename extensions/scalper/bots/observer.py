"""Observation engine: turn screener rows into episodes and forward outcomes.

The engine is intentionally boring.  It records what was observable at the
decision moment, schedules fixed horizons, and never rewrites history when a
threshold changes (a contract change requires a version bump).
"""

from __future__ import annotations

import logging
import math
import random
import sqlite3
import time

from .contracts import active_specs, spec_by_id
from .store import derive_id, dumps, ensure_schema, register_spec, save_state, state_row
from .triggers import EVENT_TRIGGERS, TRIGGERS, attention_entry

LOG = logging.getLogger(__name__)

HORIZONS = (60, 300, 900)
WINDOW_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}
CANDLE_WINDOW = {60: "1m", 300: "5m", 900: "15m", 3600: "1h"}
# The native market snapshot (the mark timestamp) refreshes every ~5.5-6s, so
# a 5s entry window missed roughly one decision in five. 10s always spans at
# least one refresh; the actual lag stays recorded in entry_at.
ENTRY_TOLERANCE_S = 10.0
# A scan sees a mark up to one scan interval after the mark's own timestamp.
# Capture and expiry share this bound so an in-window mark is never expired
# before the scan that carries it.
ENTRY_CAPTURE_LAG_S = 10.0
EXIT_TOLERANCE_S = 30.0
END_GRACE_S = 15.0
MIN_EPISODE_S = 60.0
CONTROL_TARGET = 5
ATTENTION_QUIET_S = 600  # a detail slot counts as a new entry after 10 minutes without one


def _quote(prices: dict, symbol: str):
    value = prices.get(symbol)
    if not value:
        return None
    price, at = value
    if (
        not isinstance(price, (int, float))
        or not math.isfinite(price)
        or price <= 0
        or not isinstance(at, (int, float))
        or not math.isfinite(at)
    ):
        return None
    return float(price), float(at)


def _metric(row: dict, name: str):
    item = (row.get("snapshot") or {}).get("metrics", {}).get(name) or {}
    value = item.get("value")
    if item.get("quality") != "ok" or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def natr_bucket(natr):
    if natr is None or not math.isfinite(natr) or natr <= 0:
        return None
    return round(math.log2(natr) * 2) / 2


def _pick(metric_by_symbol: dict, signal_metric, seed: str, distinct=None):
    """Deterministic same-bucket draw; widen to the nearest buckets if thin.

    The bucket is a coarse log2 band so a quiet instrument is not compared
    with a high-volatility one.  If the bucket is too thin the search widens
    rather than silently substituting a mismatched control; the stored bucket
    shows what was targeted.  ``distinct`` maps a symbol to a key allowed
    once among the chosen controls (the underlying asset).
    """
    target_bucket = natr_bucket(signal_metric)
    if not metric_by_symbol:
        return [], target_bucket
    rng = random.Random(seed)
    symbols = sorted(metric_by_symbol)
    rng.shuffle(symbols)
    if target_bucket is not None:
        # Stable sort: same bucket first, then the nearest ones, each in seeded order.
        symbols.sort(key=lambda symbol: abs((natr_bucket(metric_by_symbol[symbol]) or 0) - target_bucket))
    chosen, seen = [], set()
    for symbol in symbols:
        key = distinct(symbol) if distinct else symbol
        if key in seen:
            continue
        seen.add(key)
        chosen.append(symbol)
        if len(chosen) == CONTROL_TARGET:
            break
    return chosen, target_bucket


def choose_controls(rows: dict, window: str, signal_symbol: str, excluded: set[str], signal_natr, seed: str):
    """``random_natr_5``: same-time scan rows matched on the NATR bucket.

    ``excluded`` is the detail set (``plan.detail``).  In the unified runtime
    the scan rows are exactly ``plan.selected``, which rotates every scan, so
    overview rows rarely carry NATR; see ``choose_scout_controls``.
    """
    natr_by_symbol = {}
    for (symbol, frame), row in rows.items():
        if frame != window or symbol == signal_symbol or symbol in excluded:
            continue
        natr = _metric(row, "natr")
        if natr is None or natr <= 0:
            continue
        natr_by_symbol[symbol] = natr
    return _pick(natr_by_symbol, signal_natr, seed)


def underlying(symbol: str) -> str:
    """Venue-free asset name: BINANCE:BNB and BNB are the same underlying."""
    return symbol.removeprefix("BINANCE:").upper()


def choose_scout_controls(volatility: dict, prices: dict, signal_symbol: str, excluded: set[str], seed: str):
    """``random_scout_vol_5``: inactive instruments from the whole scouted universe.

    ``volatility`` is the allocator's RMS mark-move rate (bps/s, ~10 min) for
    every scouted instrument, signal included, so both sides of the match use
    one measure.  Candidates are outside the attention plan (``excluded`` is
    ``plan.selected | plan.detail``), not the signal's underlying on another
    venue, and must have a mark now so their entry can be priced.  At most one
    control per underlying, so BINANCE:ETH and ETH never both count.
    """
    base = underlying(signal_symbol)
    pool = {
        symbol: value
        for symbol, value in volatility.items()
        if symbol != signal_symbol
        and symbol not in excluded
        and underlying(symbol) != base
        and isinstance(value, (int, float))
        and math.isfinite(value)
        and value > 0
        and _quote(prices, symbol) is not None
    }
    return _pick(pool, volatility.get(signal_symbol), seed, distinct=underlying)


class BotObservatory:
    """Live observation engine backed by the screener journal."""

    def __init__(
        self,
        db: sqlite3.Connection,
        specs=None,
        *,
        now: float | None = None,
        source: str = "live",
    ):
        if source not in {"live", "replay"}:
            raise ValueError("source must be live or replay")
        self.db = db
        self.source = source
        self.specs = tuple(specs) if specs is not None else active_specs()
        ensure_schema(db)
        stamp = time.time() if now is None else now
        for spec in self.specs:
            register_spec(db, spec, stamp)
        self._implemented = {spec.id: TRIGGERS.get(spec.trigger) for spec in self.specs}
        self._fallback_attempts: dict[tuple[str, int], float] = {}
        self._last_true: dict[tuple[str, str, str], float] = {}
        # The latest pass's plan and price/volatility context, for events recorded between passes.
        self._last_selected: set[str] = set()
        self._last_detail: set[str] = set()
        self._last_context: dict = {}
        self._prev_plan: tuple[set[str], set[str]] | None = None  # first pass is a baseline
        self._detail_seen: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def tick(
        self,
        now: float,
        rows: dict,
        prices: dict,
        attention: dict | None = None,
        volatility: dict | None = None,
    ) -> None:
        """Observe one screener pass.  Safe to call from the synchronous scan.

        ``volatility`` is the scout volatility of the whole universe; contracts
        with ``control="random_scout_vol_5"`` need it.  Without it (replay
        recordings do not carry it) their control sets stay empty.
        """
        plan = attention or {}
        detail = set(plan.get("detail") or [])
        selected = set(plan.get("selected") or []) | detail
        context = dict(prices=prices, volatility=volatility)
        self._last_selected, self._last_detail, self._last_context = selected, detail, context
        self._observe_attention(now, plan, selected, detail, rows, prices)
        for spec in self.specs:
            trigger = self._implemented.get(spec.id)
            if not spec.enabled or trigger is None:
                continue
            for (symbol, window), row in list(rows.items()):
                if window != spec.window:
                    continue
                try:
                    event = trigger(row)
                    if event is None:
                        self._end_state(spec, symbol, window, now)
                    else:
                        self._observe_event(spec, symbol, window, row, event, now, rows, selected, detail, context)
                except Exception:
                    LOG.exception("Bot %s trigger failed for %s/%s", spec.id, symbol, window)
        self._capture_marks(now, prices)

    def implemented(self, spec) -> bool:
        return self._implemented.get(spec.id) is not None or spec.trigger in EVENT_TRIGGERS

    def _observe_attention(self, now, plan, selected, detail, rows, prices) -> None:
        """attention.relative_move: one event per symbol getting a detail slot (anomaly or mover).

        plan.selected is not used: its tail is a scout ranking that rotates through hundreds of symbols
        a minute. An entry counts only after ATTENTION_QUIET_S out of detail, and never on the first pass.
        """
        first = self._prev_plan is None
        self._prev_plan = (set(selected), set(detail))
        entries = [s for s in sorted(detail)
                   if not first and now - self._detail_seen.get(s, float("-inf")) > ATTENTION_QUIET_S]
        for symbol in detail:
            self._detail_seen[symbol] = now
        for symbol in [s for s, at in self._detail_seen.items() if now - at > 2 * ATTENTION_QUIET_S]:
            del self._detail_seen[symbol]
        movers = {r.get("symbol") for r in plan.get("movers") or []}
        for symbol in entries:
            quote = _quote(prices, symbol)
            if quote is None:
                continue
            slot = "mover" if symbol in movers else "detail" if symbol in detail else "overview"
            rank = ((rows.get((symbol, "5m")) or {}).get("strength") or {}).get("rank")
            self.record("attention.relative_move", symbol,
                        attention_entry(slot, quote[0], rank if type(rank) is int else None), now)

    def record(self, bot_id: str, symbol: str, event: dict | None, now: float) -> bool:
        """Observe an event that does not come from a screener row (tape reading, level, cascade).

        Episodes, cooldowns, controls and outcomes follow the same rules as row triggers; controls use
        the last pass's scout volatility. Returns True when the bot is active and took the event.
        """
        spec = next((s for s in self.specs if s.id == bot_id), None)
        if spec is None or not spec.enabled or event is None:
            return False
        try:
            self._observe_event(spec, symbol, spec.window, {}, event, now, {}, self._last_selected,
                                self._last_detail, self._last_context)
        except Exception:
            LOG.exception("Bot %s event failed for %s", bot_id, symbol)
            return False
        return True

    def _end_state(self, spec, symbol: str, window: str, now: float) -> None:
        state = state_row(self.db, spec, symbol, window)
        if state is None or state["direction"] == 0:
            return
        # One stale scan is not the end of a flow episode.  Require the
        # condition to stay false for a short grace period.  `_last_true` is
        # updated on every successful scan, so a lagging DB write does not
        # shorten the grace window.
        key = (spec.id, symbol, window)
        last_true = self._last_true.get(key, float(state["last_seen"]))
        if now - last_true < END_GRACE_S:
            return
        self._last_true.pop(key, None)
        save_state(
            self.db,
            spec,
            symbol,
            window,
            direction=0,
            episode_id=None,
            started_at=None,
            last_seen=now,
        )

    def _observe_event(
        self, spec, symbol, window, row, event, now, rows, selected, detail=frozenset(), context=None
    ) -> None:
        direction = int(event["direction"])
        self._last_true[(spec.id, symbol, window)] = now
        state = state_row(self.db, spec, symbol, window)
        stale_after = max(2 * WINDOW_SECONDS.get(window, 300), 120)
        if state is not None:
            last_seen = float(state["last_seen"])
            same = int(state["direction"]) == direction
            if not same and int(state["direction"]) != 0:
                started = float(state["started_at"] or state["last_seen"])
                if now - started < MIN_EPISODE_S:
                    return
            if same and now - last_seen <= stale_after:
                self._touch_state(spec, symbol, window, state, now)
                return
            if same and now - last_seen > stale_after and now - last_seen < spec.cooldown_s:
                return
            if int(state["direction"]) == 0 and now - last_seen < spec.cooldown_s:
                return

        episode_id = derive_id(
            "bot-episode",
            spec.id,
            spec.version,
            symbol,
            window,
            direction,
            int(now // WINDOW_SECONDS.get(window, 300)),
        )
        control_set_id = derive_id("bot-controls", episode_id)
        trigger_json = dumps({**event, "symbol": symbol, "window": window, "selected": symbol in selected})
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO bot_observations("
                "id,bot_id,version,trigger_event_id,control_set_id,symbol,window,mode,hypothesis,"
                "strength,direction,decided_at,entry_due_at,trigger_json,status,source"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'observed',?)",
                (
                    episode_id,
                    spec.id,
                    spec.version,
                    episode_id,
                    control_set_id,
                    symbol,
                    window,
                    spec.mode,
                    spec.hypothesis,
                    int(event["strength"]),
                    direction,
                    now,
                    now + 1.0,
                    trigger_json,
                    self.source,
                ),
            ).rowcount
            if inserted:
                for horizon in HORIZONS:
                    self.db.execute(
                        "INSERT INTO bot_outcomes(observation_id,horizon_s,exit_due_at,status) "
                        "VALUES(?,?,?,'pending')",
                        (episode_id, horizon, now + 1.0 + horizon),
                    )
                controls, bucket = self._controls(spec, symbol, window, rows, event, selected, detail,
                                                  context or {}, episode_id)
                self.db.execute(
                    "INSERT OR IGNORE INTO bot_control_sets("
                    "id,trigger_event_id,direction,mode,symbols,bucket,created_at"
                    ") VALUES(?,?,?,?,?,?,?)",
                    (
                        control_set_id,
                        episode_id,
                        direction,
                        spec.mode,
                        dumps(controls),
                        "" if bucket is None else f"{bucket:+.1f}",
                        now,
                    ),
                )
                for control in controls:
                    for horizon in HORIZONS:
                        self.db.execute(
                            "INSERT OR IGNORE INTO bot_control_points("
                            "control_set_id,symbol,horizon_s,entry_due_at,exit_due_at,status"
                            ") VALUES(?,?,?,?,?,'pending')",
                            (control_set_id, control, horizon, now + 1.0, now + 1.0 + horizon),
                        )
        save_state(
            self.db,
            spec,
            symbol,
            window,
            direction=direction,
            episode_id=episode_id,
            started_at=now,
            last_seen=now,
        )

    def _controls(self, spec, symbol, window, rows, event, selected, detail, context, episode_id):
        if spec.control == "random_scout_vol_5":
            volatility = context.get("volatility")
            if not volatility:
                return [], None
            return choose_scout_controls(volatility, context.get("prices") or {}, symbol, selected, episode_id)
        return choose_controls(rows, window, symbol, detail, event.get("natr"), seed=episode_id)

    def _touch_state(self, spec, symbol: str, window: str, state, now: float) -> None:
        last_seen = float(state["last_seen"])
        # State is a liveness marker, not an audit trail: refreshing it every
        # 30 seconds is enough and avoids a write on every 5-second scan.
        if now - last_seen < 30:
            return
        save_state(
            self.db,
            spec,
            symbol,
            window,
            direction=int(state["direction"]),
            episode_id=state["episode_id"],
            started_at=state["started_at"],
            last_seen=now,
        )
        if state["episode_id"]:
            with self.db:
                self.db.execute(
                    "UPDATE bot_observations SET repeat_count=repeat_count+1 WHERE id=?",
                    (state["episode_id"],),
                )

    # ------------------------------------------------------------------
    # Mark capture and settlement
    # ------------------------------------------------------------------

    def _capture_marks(self, now: float, prices: dict) -> None:
        for row in self.db.execute(
            "SELECT id,symbol,entry_due_at FROM bot_observations "
            "WHERE entry_price IS NULL AND status='observed' AND entry_due_at<=? AND entry_due_at>=?",
            (now, now - ENTRY_TOLERANCE_S - ENTRY_CAPTURE_LAG_S),
        ).fetchall():
            quote = _quote(prices, row["symbol"])
            if quote is None or not 0 <= quote[1] - row["entry_due_at"] <= ENTRY_TOLERANCE_S:
                continue
            with self.db:
                self.db.execute(
                    "UPDATE bot_observations SET entry_price=?,entry_at=?,entry_source='mark' WHERE id=?",
                    (quote[0], quote[1], row["id"]),
                )

        for row in self.db.execute(
            "SELECT o.id,o.symbol,o.entry_at FROM bot_observations o "
            "WHERE o.entry_price IS NOT NULL AND o.status='observed' "
            "AND EXISTS (SELECT 1 FROM bot_outcomes x WHERE x.observation_id=o.id "
            "AND x.status IN ('pending','priced'))"
        ).fetchall():
            quote = _quote(prices, row["symbol"])
            if quote is None or quote[1] < float(row["entry_at"]) - 1:
                continue
            last = self.db.execute(
                "SELECT MAX(at) FROM bot_path_points WHERE observation_id=?", (row["id"],)
            ).fetchone()[0]
            if last is not None and quote[1] - float(last) < 15:
                continue
            with self.db:
                self.db.execute(
                    "INSERT OR IGNORE INTO bot_path_points(observation_id,at,price) VALUES(?,?,?)",
                    (row["id"], quote[1], quote[0]),
                )

        for row in self.db.execute(
            "SELECT x.observation_id,x.horizon_s,x.exit_due_at,o.symbol "
            "FROM bot_outcomes x JOIN bot_observations o ON o.id=x.observation_id "
            "WHERE x.status='pending' AND x.exit_due_at<=? AND x.exit_due_at>=?",
            (now, now - 60),
        ).fetchall():
            quote = _quote(prices, row["symbol"])
            if quote is None or not 0 <= quote[1] - row["exit_due_at"] <= EXIT_TOLERANCE_S:
                continue
            with self.db:
                self.db.execute(
                    "UPDATE bot_outcomes SET exit_price=?,exit_at=?,exit_source='mark',status='priced' "
                    "WHERE observation_id=? AND horizon_s=?",
                    (quote[0], quote[1], row["observation_id"], row["horizon_s"]),
                )

        for row in self.db.execute(
            "SELECT control_set_id,symbol,horizon_s,entry_due_at FROM bot_control_points "
            "WHERE status='pending' AND entry_due_at<=? AND entry_due_at>=?",
            (now, now - ENTRY_TOLERANCE_S - ENTRY_CAPTURE_LAG_S),
        ).fetchall():
            quote = _quote(prices, row["symbol"])
            if quote is None or not 0 <= quote[1] - row["entry_due_at"] <= ENTRY_TOLERANCE_S:
                continue
            with self.db:
                self.db.execute(
                    "UPDATE bot_control_points SET entry_price=?,entry_at=?,status='active' "
                    "WHERE control_set_id=? AND symbol=? AND horizon_s=?",
                    (quote[0], quote[1], row["control_set_id"], row["symbol"], row["horizon_s"]),
                )

        for row in self.db.execute(
            "SELECT control_set_id,symbol,horizon_s,exit_due_at FROM bot_control_points "
            "WHERE status='active' AND exit_due_at<=? AND exit_due_at>=?",
            (now, now - 60),
        ).fetchall():
            quote = _quote(prices, row["symbol"])
            if quote is None or not 0 <= quote[1] - row["exit_due_at"] <= EXIT_TOLERANCE_S:
                continue
            with self.db:
                self.db.execute(
                    "UPDATE bot_control_points SET exit_price=?,exit_at=?,status='priced' "
                    "WHERE control_set_id=? AND symbol=? AND horizon_s=?",
                    (quote[0], quote[1], row["control_set_id"], row["symbol"], row["horizon_s"]),
                )

    async def settle(self, now: float, hub=None, *, max_fallbacks: int = 8) -> None:
        """Finalize priced outcomes and fall back to candle closes when needed."""
        if len(self._fallback_attempts) > 5000:
            cutoff = now - 3600
            self._fallback_attempts = {
                key: stamp for key, stamp in self._fallback_attempts.items() if stamp >= cutoff
            }
        self._expire_entries(now)
        await self._fallback_exits(now, hub, max_fallbacks=max_fallbacks)
        self._expire_exits(now)
        self._finalize_outcomes(now)
        self._finalize_controls(now)
        self._expire_controls(now)

    def settle_mark_only(self, now: float) -> None:
        """Replay settlement: no network, no candle fallback, marks only."""
        self._expire_entries(now)
        self._expire_exits(now)
        self._finalize_outcomes(now)
        self._finalize_controls(now)
        self._expire_controls(now)

    def _expire_entries(self, now: float) -> None:
        rows = self.db.execute(
            "SELECT id FROM bot_observations WHERE entry_price IS NULL AND status='observed' "
            "AND entry_due_at<?",
            (now - ENTRY_TOLERANCE_S - ENTRY_CAPTURE_LAG_S,),
        ).fetchall()
        for row in rows:
            with self.db:
                self.db.execute(
                    "UPDATE bot_observations SET status='unscored',unscored_reason='no_entry_mark' "
                    "WHERE id=?",
                    (row["id"],),
                )
                self.db.execute(
                    "UPDATE bot_outcomes SET status='unscored',unscored_reason='no_entry_mark' "
                    "WHERE observation_id=? AND status='pending'",
                    (row["id"],),
                )

    async def _fallback_exits(self, now: float, hub, *, max_fallbacks: int) -> None:
        fetch = getattr(getattr(hub, "market", None), "get_candles", None) if hub is not None else None
        if fetch is None:
            return
        rows = self.db.execute(
            "SELECT x.observation_id,x.horizon_s,x.exit_due_at,o.symbol "
            "FROM bot_outcomes x JOIN bot_observations o ON o.id=x.observation_id "
            "WHERE x.status='pending' AND x.exit_price IS NULL AND x.exit_due_at<=? "
            "AND x.exit_due_at>=? AND o.entry_price IS NOT NULL ORDER BY x.exit_due_at LIMIT ?",
            (now, now - 3600, max_fallbacks),
        ).fetchall()
        for row in rows:
            key = (row["observation_id"], row["horizon_s"])
            if now - self._fallback_attempts.get(key, 0) < 30:
                continue
            self._fallback_attempts[key] = now
            window = CANDLE_WINDOW.get(row["horizon_s"], "5m")
            seconds = WINDOW_SECONDS[window]
            try:
                candles = await fetch(row["symbol"], window, bars=50)
            except Exception:
                LOG.debug("Bot candle fallback failed for %s/%s", row["symbol"], window, exc_info=True)
                continue
            candidates = []
            for bar in candles or []:
                stamp, close = bar.get("timestamp"), bar.get("close")
                if not isinstance(stamp, int) or not isinstance(close, (int, float)) or close <= 0:
                    continue
                close_at = (stamp + seconds * 1000) / 1000
                if close_at + 1e-6 >= row["exit_due_at"] and close_at <= row["exit_due_at"] + 2 * seconds:
                    candidates.append((close_at, float(close)))
            if not candidates:
                continue
            close_at, close = min(candidates, key=lambda item: item[0])
            if close_at > now + 1:
                continue
            with self.db:
                self.db.execute(
                    "UPDATE bot_outcomes SET exit_price=?,exit_at=?,exit_source='candle_close',status='priced' "
                    "WHERE observation_id=? AND horizon_s=?",
                    (close, close_at, row["observation_id"], row["horizon_s"]),
                )

    def _expire_exits(self, now: float) -> None:
        rows = self.db.execute(
            "SELECT x.observation_id,x.horizon_s,x.exit_due_at FROM bot_outcomes x "
            "JOIN bot_observations o ON o.id=x.observation_id "
            "WHERE x.status='pending' AND x.exit_price IS NULL AND o.entry_price IS NOT NULL"
        ).fetchall()
        for row in rows:
            window = CANDLE_WINDOW.get(row["horizon_s"], "5m")
            tolerance = 2 * WINDOW_SECONDS[window] + 60
            if now <= row["exit_due_at"] + tolerance:
                continue
            with self.db:
                self.db.execute(
                    "UPDATE bot_outcomes SET status='unscored',unscored_reason='no_exit_price' "
                    "WHERE observation_id=? AND horizon_s=? AND status='pending'",
                    (row["observation_id"], row["horizon_s"]),
                )

    def _finalize_outcomes(self, now: float) -> None:
        rows = self.db.execute(
            "SELECT x.observation_id,x.horizon_s,x.exit_price,x.exit_at,"
            "o.entry_price,o.entry_at,o.direction,o.mode "
            "FROM bot_outcomes x JOIN bot_observations o ON o.id=x.observation_id "
            "WHERE x.status='priced'"
        ).fetchall()
        for row in rows:
            points = self.db.execute(
                "SELECT at,price FROM bot_path_points WHERE observation_id=? "
                "AND at>=? AND at<=? ORDER BY at",
                (row["observation_id"], row["entry_at"], row["exit_at"]),
            ).fetchall()
            values = [(float(row["entry_at"]), float(row["entry_price"]))]
            values.extend((float(p["at"]), float(p["price"])) for p in points)
            values.append((float(row["exit_at"]), float(row["exit_price"])))
            changes = []
            for _, price in values:
                change = (price / float(row["entry_price"]) - 1) * 10_000
                if row["mode"] == "directional":
                    change *= int(row["direction"])
                else:
                    change = abs(change)
                changes.append(change)
            final = changes[-1]
            with self.db:
                self.db.execute(
                    "UPDATE bot_outcomes SET move_bps=?,mae_bps=?,mfe_bps=?,path_points=?,status='scored' "
                    "WHERE observation_id=? AND horizon_s=?",
                    (
                        final,
                        min(changes) if changes else None,
                        max(changes) if changes else None,
                        len(points),
                        row["observation_id"],
                        row["horizon_s"],
                    ),
                )
            if row["horizon_s"] == 300:
                with self.db:
                    self.db.execute(
                        "UPDATE bot_observations SET status='scored' WHERE id=? AND status='observed'",
                        (row["observation_id"],),
                    )
            remaining = self.db.execute(
                "SELECT COUNT(*) FROM bot_outcomes WHERE observation_id=? AND status IN ('pending','priced')",
                (row["observation_id"],),
            ).fetchone()[0]
            if remaining == 0:
                with self.db:
                    self.db.execute(
                        "DELETE FROM bot_path_points WHERE observation_id=?", (row["observation_id"],)
                    )

    def _finalize_controls(self, now: float) -> None:
        rows = self.db.execute(
            "SELECT p.control_set_id,p.symbol,p.horizon_s,p.entry_price,p.exit_price,"
            "s.direction,s.mode FROM bot_control_points p "
            "JOIN bot_control_sets s ON s.id=p.control_set_id WHERE p.status='priced'"
        ).fetchall()
        for row in rows:
            change = (float(row["exit_price"]) / float(row["entry_price"]) - 1) * 10_000
            if row["mode"] == "directional":
                change *= int(row["direction"])
            else:
                change = abs(change)
            with self.db:
                self.db.execute(
                    "UPDATE bot_control_points SET move_bps=?,status='scored' "
                    "WHERE control_set_id=? AND symbol=? AND horizon_s=?",
                    (change, row["control_set_id"], row["symbol"], row["horizon_s"]),
                )

    def _expire_controls(self, now: float) -> None:
        with self.db:
            self.db.execute(
                "UPDATE bot_control_points SET status='unscored',unscored_reason='no_control_price' "
                "WHERE status IN ('pending','active') AND exit_due_at<?",
                (now - 60,),
            )

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def report(self, bot_id: str | None = None) -> dict:
        from .report import bot_card, bots_summary
        from .stats import benjamini_hochberg, summarize

        if bot_id is not None:
            spec = spec_by_id(bot_id)
            if spec is None:
                return {"status": "unknown", "bot_id": bot_id, "text": "Неизвестный бот.", "html": "Неизвестный бот."}
            data = summarize(
                self.db, spec, implemented=self.implemented(spec), source=self.source
            )
            return {
                "status": "ok",
                "bot": data,
                "text": bot_card(data, rich=False),
                "html": bot_card(data, rich=True),
            }
        bots = [
            summarize(
                self.db, spec, implemented=self.implemented(spec), source=self.source
            )
            for spec in active_specs()
        ]
        tested = [bot for bot in bots if bot.get("implemented") and (bot.get("primary") or {}).get("p") is not None]
        if tested:
            rejected, adjusted = benjamini_hochberg(
                [(bot["primary"] or {})["p"] for bot in tested], q=0.1
            )
            for bot, is_rejected, q_value in zip(tested, rejected, adjusted):
                bot["primary"]["q"] = q_value
                bot["primary"]["fdr_rejected"] = is_rejected
        return {
            "status": "ok",
            "bots": bots,
            "text": bots_summary(bots, rich=False),
            "html": bots_summary(bots, rich=True),
        }
