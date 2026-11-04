"""Closed-candle volatility and relative tape activity."""
import asyncio
import logging
import math
import time
from statistics import median

import numpy as np
import talib

LOG = logging.getLogger(__name__)
CANDLE_SOURCE = "Hyperliquid candleSnapshot"
BASELINE_BARS = 14
BURST_RATIO = 2.0
MIN_BURST_TRADES = 10


def candle_indicators(candles, seconds, now):
    interval = seconds * 1000
    closed = {bar["timestamp"]: bar for bar in candles if isinstance(bar.get("timestamp"), int)
              and bar["timestamp"] + interval <= now * 1000}
    times = sorted(closed)
    if len(times) < 16:
        return None
    # Work from the newest contiguous segment. Missing candles are never
    # bridged as if their prices were observed.
    start = len(times) - 1
    while start > 0 and times[start] - times[start - 1] == interval:
        start -= 1
    bars = [closed[stamp] for stamp in times[start:]]
    if len(bars) < 16:
        return None
    high, low, close = (np.array([bar[name] for bar in bars], dtype=float)
                        for name in ("high", "low", "close"))
    if not all(np.isfinite(array).all() and (array > 0).all() for array in (high, low, close)):
        return None
    if not ((high >= close) & (close >= low)).all():
        return None
    natr = talib.NATR(high, low, close, timeperiod=14)
    previous_atr = natr[-2] * close[-2] / 100
    true_range = talib.TRANGE(high, low, close)[-1]
    return {
        "closed_at": (times[-1] + interval) / 1000,
        "natr": float(natr[-1]),
        "range_ratio": float(true_range / previous_atr) if previous_atr > 0 else None,
        "counts": [((bar["timestamp"] + interval) / 1000, bar.get("trade_count"))
                   for bar in bars[-(BASELINE_BARS + 2):]],
    }


def tape_baseline(indicators, now, seconds):
    # Exclude any candle overlapping the rolling live window.
    samples = [count for end, count in indicators["counts"] if end <= now - seconds][-BASELINE_BARS:]
    if len(samples) != BASELINE_BARS or any(type(count) is not int or count < 0 for count in samples):
        return None
    return median(samples)


def active_anomalies(snapshot):
    metrics = snapshot["metrics"]
    ratio = metrics.get("activity_ratio", {})
    trades = metrics.get("trade_count", {})
    volatility = metrics.get("range_ratio", {})
    active = set()
    if ratio.get("quality") == "ok" and ratio["value"] >= BURST_RATIO and trades["value"] >= MIN_BURST_TRADES:
        active.add("tape_burst")
    if volatility.get("quality") == "ok" and volatility["value"] >= BURST_RATIO:
        active.add("range_burst")
    return frozenset(active)


async def refresh_candles(hub, windows, cache):
    due = {}
    while True:
        keys = [(asset.symbol, window) for asset in hub.get_all_assets()[:20] for window in windows]
        active = set(keys)
        for key in due.keys() - active:
            del due[key]
            cache.pop(key, None)
        for key in sorted(keys, key=lambda key: due.get(key, 0)):
            now = time.time()
            if due.get(key, 0) > now:
                continue
            symbol, window = key
            seconds = windows[window]
            try:
                candles = await hub.market.get_candles(symbol, window, bars=50)
                indicators = candle_indicators(candles, seconds, time.time())
                if indicators:
                    cache[key] = indicators
            except Exception as exc:
                LOG.debug("Candle refresh failed for %s/%s: %s", symbol, window, type(exc).__name__)
            due[key] = max(now + 30, (math.floor(now / seconds) + 1) * seconds + 2)
            # Keep candle requests below the venue's shared REST weight limit.
            await asyncio.sleep(2)
        await asyncio.sleep(2)
