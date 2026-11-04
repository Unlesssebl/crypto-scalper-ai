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
BTC_RETURN_SAMPLES = 36
BTC_BAR_SECONDS = 300
BURST_RATIO = 2.0
MIN_BURST_TRADES = 10
WIDE_SPREAD_ATR_FRACTION = 0.25
MIN_STRONG_MOVE_PCT = 0.10
MIN_LARGE_MOVE_PCT = 0.20
SPREAD_MULTIPLE = 4
FINDING_LEVELS = (
    ("⚪", "Нет свежих данных"),
    ("🔵", "Фоновая"),
    ("🟡", "Заметная"),
    ("🟠", "Выраженное движение"),
    ("🔴", "Сильное движение"),
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
    recent_returns = {}
    if seconds == BTC_BAR_SECONDS:
        recent_returns = {
            (stamp + interval) / 1000: float(change)
            for stamp, change in zip(times[start + 1:][-BTC_RETURN_SAMPLES:],
                                     (100 * np.diff(close) / close[:-1])[-BTC_RETURN_SAMPLES:])
        }
    return {
        "returns": recent_returns,
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


def btc_context(asset, btc, now):
    if not asset or not btc or asset is btc:
        return None
    asset_returns, btc_returns = asset["returns"], btc["returns"]
    shared = sorted(asset_returns.keys() & btc_returns.keys())[-BTC_RETURN_SAMPLES:]
    if (len(shared) != BTC_RETURN_SAMPLES or not 0 <= now - shared[-1] <= BTC_BAR_SECONDS + 90
            or any(end - start != BTC_BAR_SECONDS for start, end in zip(shared, shared[1:]))):
        return None
    x = np.array([asset_returns[stamp] for stamp in shared])
    y = np.array([btc_returns[stamp] for stamp in shared])
    if not np.isfinite(x).all() or not np.isfinite(y).all() or np.ptp(x) == 0 or np.ptp(y) == 0:
        return None
    return {
        "reference": "BTC-PERP", "window": "5m", "samples": len(shared), "updated_at": shared[-1],
        "correlation": float(np.corrcoef(x, y)[0, 1]),
        "btc_last_return_pct": float(y[-1]), "asset_last_return_pct": float(x[-1]),
        "relative_last_return_pp": float(x[-1] - y[-1]),
    }


def tape_baseline(indicators, now, seconds):
    # Exclude any candle overlapping the rolling live window.
    samples = [count for end, count in indicators["counts"] if end <= now - seconds][-BASELINE_BARS:]
    if len(samples) != BASELINE_BARS or any(type(count) is not int or count < 0 for count in samples):
        return None
    return median(samples)


def active_anomalies(snapshot, *, tape_ratio=BURST_RATIO):
    metrics = snapshot["metrics"]
    ratio = metrics.get("activity_ratio", {})
    trades = metrics.get("trade_count", {})
    volatility = metrics.get("range_ratio", {})
    active = set()
    if ratio.get("quality") == "ok" and ratio["value"] >= tape_ratio and trades["value"] >= MIN_BURST_TRADES:
        active.add("tape_burst")
    if volatility.get("quality") == "ok" and volatility["value"] >= BURST_RATIO:
        active.add("range_burst")
    return frozenset(active)


def wide_spread(snapshot):
    metrics = snapshot["metrics"]
    spread, mark, natr = (fresh(metrics, key) for key in ("spread", "price", "natr"))
    return (spread is not None and mark is not None and mark > 0 and natr is not None and natr > 0
            and 100 * spread / mark >= WIDE_SPREAD_ATR_FRACTION * natr)


def finding_strength(snapshot, native, *, tape_ratio=BURST_RATIO, price_floor=MIN_STRONG_MOVE_PCT,
                     price_natr_multiple=1.0):
    metrics = snapshot["metrics"]
    anomalies = active_anomalies(snapshot, tape_ratio=tape_ratio)
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
    spread, mark = (fresh(metrics, key) for key in ("spread", "price"))
    spread_pct = 100 * spread / mark if spread is not None and mark and mark > 0 else None
    aligned = direction in {"BULLISH", "BEARISH"} and change != 0 and (change > 0) == (direction == "BULLISH")
    confirms = (aligned and natr is not None and natr > 0 and spread_pct is not None
                and abs(change) >= max(price_floor, price_natr_multiple * natr, SPREAD_MULTIPLE * spread_pct))
    if confirms:
        reasons.append(f"Цена уже прошла {abs(change):.2f}% в сторону потока")
    strong_flow = native["native_signal"] in {"STRONG_BULL", "STRONG_BEAR"}
    rank = 2 if anomalies or (strong_flow and confirms) else 1
    if confirms and (anomalies or strong_flow):
        rank = 3
        if len(anomalies) == 2 and abs(change) >= max(MIN_LARGE_MOVE_PCT, 1.5 * natr):
            rank = 4
    elif anomalies:
        reasons.append("Всплеск без подтверждённого движения цены")

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
        if ("BTC", "5m") not in keys and "5m" in windows:
            keys.insert(0, ("BTC", "5m"))
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
