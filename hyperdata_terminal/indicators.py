"""Closed-candle indicators shared by the API and the screener.

The screener used to own these helpers. They live here so the native REST API
can return the same NATR / range / tape baseline values it computes for
`/v1/snapshot`, without importing a fork extension.
"""
from __future__ import annotations

from statistics import median

import numpy as np
import talib

CANDLE_SOURCE = "Hyperliquid candleSnapshot"
BASELINE_BARS = 14
BTC_RETURN_SAMPLES = 36
BTC_BAR_SECONDS = 300


def candle_indicators(candles, seconds, now):
    """NATR, range ratio and per-candle trade counts from closed candles.

    Returns None until there are at least 16 contiguous closed candles.
    """
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
    """Pearson correlation of the last 36 matching 5m returns vs BTC."""
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
    """Median trade count of the 14 closed candles before the live window."""
    # Exclude any candle overlapping the rolling live window.
    samples = [count for end, count in indicators["counts"] if end <= now - seconds][-BASELINE_BARS:]
    if len(samples) != BASELINE_BARS or any(type(count) is not int or count < 0 for count in samples):
        return None
    return median(samples)
