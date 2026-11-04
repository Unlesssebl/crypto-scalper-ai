"""Closed-candle volatility and relative tape activity."""
import asyncio
import logging
import math
import time
from statistics import median

import numpy as np
import talib

from .profile import fresh

LOG = logging.getLogger(__name__)
CANDLE_SOURCE = "Hyperliquid candleSnapshot"
BASELINE_BARS = 14
BURST_RATIO = 2.0
MIN_BURST_TRADES = 10
WIDE_SPREAD_ATR_FRACTION = 0.25
PRICE_CONFIRMATION_ATR_FRACTION = 0.25
FINDING_LEVELS = (
    ("⚪", "Нет свежих данных"),
    ("🔵", "Фоновая"),
    ("🟡", "Заметная"),
    ("🟠", "Сильная"),
    ("🔴", "Критичная"),
)


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
    recent_high, recent_low = max(high[-4:]), min(low[-4:])
    return {
        "return_4": float((close[-1] / close[-5] - 1) * 100),
        "range_4": float((recent_high - recent_low) / close[-1] * 100),
        "range_position_4": float((close[-1] - recent_low) / (recent_high - recent_low))
                            if recent_high > recent_low else None,
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


def wide_spread(snapshot):
    metrics = snapshot["metrics"]
    spread, mark, natr = (fresh(metrics, key) for key in ("spread", "price", "natr"))
    return (spread is not None and mark is not None and mark > 0 and natr is not None and natr > 0
            and 100 * spread / mark >= WIDE_SPREAD_ATR_FRACTION * natr)


def finding_strength(snapshot, native):
    metrics = snapshot["metrics"]
    anomalies = active_anomalies(snapshot)
    reasons = []
    if "tape_burst" in anomalies:
        reasons.append("Ускорение ленты")
    if "range_burst" in anomalies:
        reasons.append("Всплеск диапазона")

    if fresh(metrics, "price") is None or fresh(metrics, "ofi") is None:
        return {"rank": 0, "icon": FINDING_LEVELS[0][0], "label": FINDING_LEVELS[0][1],
                "reasons": ["Нужны свежая цена и лента", *reasons]}

    start, end, natr = (fresh(metrics, key) for key in ("first_trade_price", "last_trade_price", "natr"))
    direction = native["direction"]
    change = 100 * (end / start - 1) if start and end is not None else 0
    confirms = (direction in {"BULLISH", "BEARISH"} and natr is not None and natr > 0
                and abs(change) >= PRICE_CONFIRMATION_ATR_FRACTION * natr
                and (change > 0) == (direction == "BULLISH"))
    if confirms:
        reasons.append("Цена движется вместе с потоком")
    strong_flow = native["native_signal"] in {"STRONG_BULL", "STRONG_BEAR"}
    rank = 1
    if anomalies or (strong_flow and confirms):
        rank = 2
    if len(anomalies) == 2 or (anomalies and confirms):
        rank = 3
    if len(anomalies) == 2 and confirms:
        rank = 4

    if fresh(metrics, "spread") is None:
        rank = min(rank, 2)
        reasons.append("Нет свежего стакана")
    elif wide_spread(snapshot):
        rank = min(rank, 2)
        reasons.append("Широкий спред ограничивает приоритет")
    icon, label = FINDING_LEVELS[rank]
    return {"rank": rank, "icon": icon, "label": label, "reasons": reasons}


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
