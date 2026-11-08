"""What happened after each sent card, and a per-asset summary of it.

Every card the operational chat received is scored once, five minutes after it was sent, from 1-minute
candles of the same venue (native `/v1/candles`): the price at +5 minutes, and the best/worst move over
that time. The signal bar itself is skipped — most of its range happened before the card. A card with a
side (▲/▼) is "right" when the +5m price moved its way; a card without a side only has a range.

This is observation after selection, not a trading result: no fills, fees or position sizing.
"""

import asyncio
import json
import logging
import math
import time

LOG = logging.getLogger(__name__)
HORIZON = 300  # seconds after the card
SETTLE = 60  # wait for the +5m candle to close before scoring
MAX_BARS = 500  # /v1/candles accepts 2..500 bars
LOOKBACK = 8 * 3600  # 500 one-minute bars ≈ 8.3h: older cards cannot be covered
BATCH = 20


def candle_start(candle):
    stamp = candle.get("timestamp")
    if not isinstance(stamp, (int, float)):
        return None
    return stamp / 1000 if stamp > 1e12 else stamp


def score(at, price, side, candles):
    """dict(close, high, low, move_pct, favour_pct, against_pct) or None when the candles do not cover it."""
    if not price or price <= 0:
        return None
    bars = sorted(((candle_start(c), c) for c in candles if candle_start(c) is not None), key=lambda b: b[0])
    end = at + HORIZON
    # Bars that opened after the card, up to and including the one holding +5 minutes.
    following = [c for start, c in bars if at < start <= end]
    closing = next((c for start, c in bars if start <= end < start + 60), None)
    if closing is None or not following:
        return None
    try:
        high = max(float(c["high"]) for c in following)
        low = min(float(c["low"]) for c in following)
        close = float(closing["close"])
    except (KeyError, TypeError, ValueError):
        return None
    if not all(math.isfinite(v) and v > 0 for v in (high, low, close)):
        return None
    move = close / price - 1
    up, down = max(0.0, high / price - 1), max(0.0, 1 - low / price)
    favour, against = (up, down) if side > 0 else (down, up) if side < 0 else (None, None)
    return dict(
        close=close,
        high=high,
        low=low,
        move_pct=move * 100,
        favour_pct=None if favour is None else favour * 100,
        against_pct=None if against is None else against * 100,
    )


def mean(values):
    return sum(values) / len(values) if values else None


class SignalOutcomes:
    def __init__(self, db):
        self.db = db
        self.error = None
        self.db.execute("""CREATE TABLE IF NOT EXISTS signal_outcomes(
            event_id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, at REAL NOT NULL, side INTEGER NOT NULL,
            price REAL, horizon INTEGER NOT NULL, status TEXT NOT NULL, close REAL, high REAL, low REAL,
            move_pct REAL, favour_pct REAL, against_pct REAL, measured_at REAL NOT NULL)""")
        self.db.execute("CREATE INDEX IF NOT EXISTS signal_outcomes_symbol ON signal_outcomes(symbol, at)")

    def pending(self, now):
        return self.db.execute(
            "SELECT e.id, e.at, e.state, p.symbol FROM telegram_ux_events e JOIN fact_packets p ON p.id=e.packet "
            "WHERE e.status='queued' AND e.at<=? AND e.at>=? "
            "AND NOT EXISTS(SELECT 1 FROM signal_outcomes o WHERE o.event_id=e.id) ORDER BY e.at LIMIT ?",
            (now - HORIZON - SETTLE, now - LOOKBACK, BATCH),
        ).fetchall()

    def record(self, event_id, symbol, at, state, candles, now):
        from .telegram_ux import card_arrow

        arrow = card_arrow(state)
        side = 1 if arrow == "▲" else -1 if arrow == "▼" else 0
        level = state.get("level")
        if level:
            # A level card is scored as "the level held": up from a bid, down from an ask.
            side = 1 if level["zone"]["side"] == "bid" else -1
        price = state.get("midpoint")
        result = score(at, price, side, candles) if candles else None
        status = "measured" if result else "missing"
        result = result or {}
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO signal_outcomes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (event_id, symbol, at, side, price, HORIZON, status, result.get("close"), result.get("high"),
                 result.get("low"), result.get("move_pct"), result.get("favour_pct"), result.get("against_pct"), now),
            )
        return status

    async def once(self, hub, now=None):
        now = time.time() if now is None else now
        rows = self.pending(now)
        cache = {}
        for event_id, at, state_json, symbol in rows:
            bars = max(2, min(MAX_BARS, int((now - at) / 60) + 4))
            key = (symbol, bars)
            if key not in cache:
                try:
                    cache[key] = await hub.get_candles(symbol, "1m", bars=bars)
                except Exception as exc:
                    status = getattr(exc, "status", None)
                    if isinstance(status, int) and 400 <= status < 500:
                        cache[key] = []  # the API refuses this instrument: record it as missing, move on
                    else:
                        self.error = type(exc).__name__  # unavailable: retry the whole batch next pass
                        return 0
            self.record(event_id, symbol, at, json.loads(state_json), cache[key], now)
        self.error = None
        return len(rows)

    async def run(self, hub):
        while True:
            try:
                await self.once(hub)
            except Exception:
                LOG.exception("Signal outcome scoring failed")
            await asyncio.sleep(60)

    def channel_summary(self, channel, now, hours=24):
        """One bot's cards in the last N hours, scored or not, with a strong/notable split."""
        rows = self.db.execute(
            "SELECT e.at, json_extract(e.state,'$.strength'), o.status, o.side, o.move_pct, o.favour_pct, "
            "o.against_pct FROM telegram_ux_events e LEFT JOIN signal_outcomes o ON o.event_id=e.id "
            "WHERE e.status='queued' AND COALESCE(e.channel,'anomalies')=? AND e.at>=?",
            (channel, now - hours * 3600),
        ).fetchall()
        measured = [r for r in rows if r[2] == "measured"]
        sided = [r for r in measured if r[3]]
        by_strength = []
        for strength in ("strong", "notable"):
            group = [r for r in sided if r[1] == strength]
            by_strength.append(dict(strength=strength, sided=len(group),
                                    right=sum(1 for r in group if r[4] * r[3] > 0)))
        return dict(
            sent=len(rows),
            measured=len(measured),
            sided=len(sided),
            right=sum(1 for r in sided if r[4] * r[3] > 0),
            neutral=len(measured) - len(sided),
            favour_pct=mean([r[5] for r in sided]),
            against_pct=mean([r[6] for r in sided]),
            pending=sum(1 for r in rows if r[2] is None and r[0] >= now - LOOKBACK),
            by_strength=by_strength,
        )

    def summary(self, symbols, now, hours=24):
        """Cards sent for these symbols in the last N hours and how the scored ones turned out."""
        if not symbols:
            return None
        marks = ",".join("?" * len(symbols))
        since = now - hours * 3600
        sent = self.db.execute(
            f"SELECT COUNT(*) FROM telegram_ux_events e JOIN fact_packets p ON p.id=e.packet "
            f"WHERE e.status='queued' AND e.at>=? AND p.symbol IN ({marks})",
            (since, *symbols),
        ).fetchone()[0]
        rows = self.db.execute(
            f"SELECT side, move_pct, favour_pct, against_pct FROM signal_outcomes "
            f"WHERE status='measured' AND at>=? AND symbol IN ({marks})",
            (since, *symbols),
        ).fetchall()
        recent_unscored = self.db.execute(
            f"SELECT COUNT(*) FROM telegram_ux_events e JOIN fact_packets p ON p.id=e.packet "
            f"WHERE e.status='queued' AND e.at>=? AND p.symbol IN ({marks}) "
            f"AND NOT EXISTS(SELECT 1 FROM signal_outcomes o WHERE o.event_id=e.id)",
            (max(since, now - LOOKBACK), *symbols),
        ).fetchone()[0]
        sided = [r for r in rows if r[0] != 0]
        right = sum(1 for side, move, _, _ in sided if move * side > 0)
        return dict(
            hours=hours,
            sent=sent,
            measured=len(rows),
            sided=len(sided),
            right=right,
            favour_pct=mean([r[2] for r in sided]),
            against_pct=mean([r[3] for r in sided]),
            horizon_minutes=HORIZON // 60,
            pending=recent_unscored,  # will be scored (or are younger than 6 minutes)
            unscored=max(0, sent - len(rows) - recent_unscored),  # older than the candle history reaches
        )
