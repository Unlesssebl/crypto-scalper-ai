"""Liquidations as context for cards: is the move riding forced closures?

Not a signal of its own (docs/multi-bot-plan.md): single liquidations are noisy, and Hyperliquid large
prints are only estimates. Only exchange-confirmed events count. A card gets a line with the last 60s
of liquidated longs/shorts for its asset across venues, and "на каскаде" when the forced closures are
large against the asset's own 60s turnover and on the side the price is moving (longs liquidated while
it falls, shorts while it rises). Thresholds are initial engineering values.
"""

import asyncio
import logging
import time

LOG = logging.getLogger(__name__)
WINDOW = 60
POLL = 10
SHOW_MIN_USD = 10_000  # below this the line is noise
CASCADE_MIN_USD = 50_000
CASCADE_TURNOVER_SHARE = 0.2  # forced closures ≥ 20% of the asset's own 60s turnover


def base_symbol(symbol):
    return symbol.removeprefix("BINANCE:").upper()


class LiquidationContext:
    def __init__(self):
        self.events = []
        self.updated_at = None
        self.error = None

    def ingest(self, events, now):
        keep = []
        for e in events:
            if not isinstance(e, dict) or not e.get("confirmed"):
                continue
            stamp, size = e.get("timestamp"), e.get("size_usd")
            if not isinstance(stamp, (int, float)) or not isinstance(size, (int, float)) or size <= 0:
                continue
            if e.get("side") not in ("long", "short") or now - stamp > WINDOW * 3:
                continue
            keep.append(dict(t=float(stamp), symbol=str(e.get("symbol", "")).upper(), side=e["side"],
                             usd=float(size), exchange=e.get("exchange")))
        self.events = keep
        self.updated_at = now

    def recent(self, symbol, now, turnover_60s=None, move_bps=None):
        """None when the feed is stale; otherwise totals and whether the move rides a cascade."""
        if self.updated_at is None or now - self.updated_at > POLL * 4:
            return None
        base = base_symbol(symbol)
        rows = [e for e in self.events if e["symbol"] == base and 0 <= now - e["t"] <= WINDOW]
        longs = sum(e["usd"] for e in rows if e["side"] == "long")
        shorts = sum(e["usd"] for e in rows if e["side"] == "short")
        total = longs + shorts
        dominant = "long" if longs >= shorts else "short"
        big = total >= CASCADE_MIN_USD and (
            not turnover_60s or total >= CASCADE_TURNOVER_SHARE * turnover_60s
        )
        # Longs are force-sold into a falling price, shorts force-bought into a rising one.
        aligned = move_bps is not None and (move_bps < 0 if dominant == "long" else move_bps > 0)
        return dict(
            longs_usd=longs,
            shorts_usd=shorts,
            count=len(rows),
            venues=sorted({e["exchange"] for e in rows if e["exchange"]}),
            cascade=bool(big and aligned),
            cascade_side=dominant if big and aligned else None,
        )

    async def run(self, hub):
        while True:
            try:
                payload = await hub.fetch_liquidations(minutes=3, limit=1000)
                self.ingest(payload.get("events") or [], time.time())
                self.error = None
            except Exception as exc:  # the native API may be briefly unavailable; the line then disappears
                self.error = type(exc).__name__
            await asyncio.sleep(POLL)


def liquidation_line(info, usd):
    """'💥 Ликвидации за 60с: лонги $1.2M · шорты $40k' or None when there is nothing worth a line."""
    if not info or info["longs_usd"] + info["shorts_usd"] < SHOW_MIN_USD:
        return None
    parts = []
    if info["longs_usd"]:
        parts.append(f"лонги {usd(info['longs_usd'])}")
    if info["shorts_usd"]:
        parts.append(f"шорты {usd(info['shorts_usd'])}")
    line = "💥 Ликвидации за 60с: " + " · ".join(parts)
    if info["cascade"]:
        line += " · движение на каскаде"
    return line
