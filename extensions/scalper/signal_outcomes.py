"""What happened after each sent card, and a per-asset summary of it.

Every card the operational chat received is scored once, sixteen minutes after it was sent, from 1-minute
candles of the same venue (native `/v1/candles`): the price at +5 minutes, and the best/worst move over
that time. The signal bar itself is skipped — most of its range happened before the card. A card with a
side (▲/▼) is "right" when the +5m price moved its way; a card without a side only has a range.

"Right" counts any tick in its favour, so it sits near 50% on noise and follows the day's drift. The
verdict asks for a real move instead. The barrier is the instrument's typical 5-minute range over the hour
before the card. A sided card is "confirmed" when the price travels one barrier its way within 15 minutes
before it travels one barrier against it ("failed"), and "flat" when it does neither. A card without a side
is "moved" when either barrier is reached. One bar touching both barriers counts against the card. The
background applies the same rule, same side and same barrier, from every minute of the hour before the
card: how often this instrument would have "confirmed" anyway.

This is observation after selection, not a trading result: no fills, fees or position sizing.
"""

import asyncio
import json
import logging
import math
import time

LOG = logging.getLogger(__name__)
HORIZON = 300  # seconds after the card
WINDOW = 900  # the verdict's window: a barrier must be reached within 15 minutes
SETTLE = 60  # wait for the last candle of the window to close before scoring
SCORE_AFTER = max(HORIZON, WINDOW) + SETTLE
RANGE_BARS = 60  # the typical 5-minute range is read from the hour before the card
BASE_STARTS = 60  # background: one start per minute of the hour before the card
PRE_BARS = BASE_STARTS + WINDOW // 60  # history needed before the card
MAX_BARS = 500  # /v1/candles accepts 2..500 bars
LOOKBACK = (MAX_BARS - PRE_BARS - 6) * 60  # ≈ 7h: older cards cannot be covered with their history
BATCH = 20
SCHEME = 2  # 1: +5m sign only; 2: adds the barrier verdict and its background


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


def typical_range(before):
    """Median 5-minute range, as a fraction of price, over the bars before the card (oldest first)."""
    tail = before[-RANGE_BARS:]
    ranges = []
    for i in range(0, len(tail) - 4, 5):
        chunk = tail[i:i + 5]
        try:
            high = max(float(c["high"]) for c in chunk)
            low = min(float(c["low"]) for c in chunk)
            ref = float(chunk[0]["open"])
        except (KeyError, TypeError, ValueError):
            continue
        if math.isfinite(high) and math.isfinite(low) and ref > 0 and high >= low:
            ranges.append((high - low) / ref)
    if len(ranges) < RANGE_BARS // 10:  # at least half the hour
        return None
    ranges.sort()
    middle = len(ranges) // 2
    value = ranges[middle] if len(ranges) % 2 else (ranges[middle - 1] + ranges[middle]) / 2
    return value if value > 0 else None


def barrier_verdict(price, side, barrier, following, covered):
    """confirmed/failed/flat with a side, moved/flat without one, None when undecided.

    following: the bars after the entry, oldest first, inside the window; covered: they reach its end.
    """
    up, down = price * (1 + barrier), price * (1 - barrier)
    for c in following:
        try:
            high, low = float(c["high"]), float(c["low"])
        except (KeyError, TypeError, ValueError):
            return None
        hit_up, hit_down = high >= up, low <= down
        if side == 0:
            if hit_up or hit_down:
                return "moved"
            continue
        favour, against = (hit_up, hit_down) if side > 0 else (hit_down, hit_up)
        if against:
            return "failed"  # including a bar that touched both: the order inside it is unknown
        if favour:
            return "confirmed"
    return "flat" if covered else None


def verdict(at, price, side, candles):
    """dict(barrier_pct, verdict, base_rate, base_n); None when the history or the window is not covered."""
    if not price or price <= 0:
        return None
    bars = sorted(((candle_start(c), c) for c in candles if candle_start(c) is not None), key=lambda b: b[0])
    before = [c for start, c in bars if start + 60 <= at]
    barrier = typical_range(before)
    if barrier is None:
        return None
    end = at + WINDOW
    following = [c for start, c in bars if at < start <= end]
    covered = any(start <= end < start + 60 for start, _ in bars)
    result = barrier_verdict(price, side, barrier, following, covered)
    if result is None:
        return None
    hit = "moved" if side == 0 else "confirmed"
    steps = WINDOW // 60
    history = before[-PRE_BARS:]
    outcomes = []
    for i in range(max(0, len(history) - steps - BASE_STARTS), len(history) - steps):
        try:
            entry = float(history[i]["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if entry > 0 and math.isfinite(entry):
            outcomes.append(barrier_verdict(entry, side, barrier, history[i + 1:i + 1 + steps], True))
    outcomes = [o for o in outcomes if o is not None]
    return dict(
        barrier_pct=barrier * 100,
        verdict=result,
        base_rate=sum(1 for o in outcomes if o == hit) / len(outcomes) if outcomes else None,
        base_n=len(outcomes),
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
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(signal_outcomes)")}
        for name, kind in (("barrier_pct", "REAL"), ("verdict", "TEXT"), ("base_rate", "REAL"),
                           ("base_n", "INTEGER"), ("scheme", "INTEGER")):
            if name not in columns:
                self.db.execute(f"ALTER TABLE signal_outcomes ADD COLUMN {name} {kind}")
        self.db.execute("CREATE INDEX IF NOT EXISTS signal_outcomes_symbol ON signal_outcomes(symbol, at)")

    def pending(self, now):
        return self.db.execute(
            "SELECT e.id, e.at, e.state, p.symbol FROM telegram_ux_events e JOIN fact_packets p ON p.id=e.packet "
            "WHERE e.status='queued' AND e.at<=? AND e.at>=? "
            # Cards scored before the verdict existed are scored again while their candles are reachable.
            "AND NOT EXISTS(SELECT 1 FROM signal_outcomes o WHERE o.event_id=e.id AND o.scheme>=?) "
            "ORDER BY e.at LIMIT ?",
            (now - SCORE_AFTER, now - LOOKBACK, SCHEME, BATCH),
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
        judged = (verdict(at, price, side, candles) if status == "measured" else None) or {}
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO signal_outcomes(event_id,symbol,at,side,price,horizon,status,close,high,"
                "low,move_pct,favour_pct,against_pct,measured_at,barrier_pct,verdict,base_rate,base_n,scheme) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (event_id, symbol, at, side, price, HORIZON, status, result.get("close"), result.get("high"),
                 result.get("low"), result.get("move_pct"), result.get("favour_pct"), result.get("against_pct"), now,
                 judged.get("barrier_pct"), judged.get("verdict"), judged.get("base_rate"), judged.get("base_n"),
                 SCHEME),
            )
        return status

    async def once(self, hub, now=None):
        now = time.time() if now is None else now
        rows = self.pending(now)
        cache = {}
        for event_id, at, state_json, symbol in rows:
            bars = max(2, min(MAX_BARS, int((now - at) / 60) + 5 + PRE_BARS))
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
            "o.against_pct, o.verdict, o.base_rate FROM telegram_ux_events e "
            "LEFT JOIN signal_outcomes o ON o.event_id=e.id "
            "WHERE e.status='queued' AND COALESCE(e.channel,'anomalies')=? AND e.at>=?",
            (channel, now - hours * 3600),
        ).fetchall()
        measured = [r for r in rows if r[2] == "measured"]
        sided = [r for r in measured if r[3]]
        unsided = [r for r in measured if not r[3]]

        def judged(group, hit):
            done = [r for r in group if r[7]]
            return dict(
                judged=len(done),
                hit=sum(1 for r in done if r[7] == hit),
                failed=sum(1 for r in done if r[7] == "failed"),
                flat=sum(1 for r in done if r[7] == "flat"),
                base_rate=mean([r[8] for r in done if r[8] is not None]),
                unjudged=len(group) - len(done),
            )

        by_strength = []
        for strength in ("strong", "notable"):
            group = [r for r in sided if r[1] == strength]
            by_strength.append(dict(strength=strength, sided=len(group),
                                    right=sum(1 for r in group if r[4] * r[3] > 0),
                                    **judged(group, "confirmed")))
        return dict(
            sent=len(rows),
            measured=len(measured),
            sided=len(sided),
            right=sum(1 for r in sided if r[4] * r[3] > 0),
            neutral=len(unsided),
            favour_pct=mean([r[5] for r in sided]),
            against_pct=mean([r[6] for r in sided]),
            pending=sum(1 for r in rows if r[2] is None and r[0] >= now - LOOKBACK),
            sided_verdict=judged(sided, "confirmed"),
            neutral_verdict=judged(unsided, "moved"),
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
            pending=recent_unscored,  # will be scored (or are younger than 16 minutes)
            unscored=max(0, sent - len(rows) - recent_unscored),  # older than the candle history reaches
        )
