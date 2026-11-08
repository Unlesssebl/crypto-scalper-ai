"""Stable observation keys; an exchange contract never aliases another venue."""

BINANCE_PREFIX = "BINANCE:"


def is_binance(symbol):
    return symbol.startswith(BINANCE_PREFIX)


def binance_key(native):
    if not isinstance(native, str) or not native.endswith("USDT") or not native[:-4].isalnum():
        raise ValueError("expected an exact Binance USDT-M instrument")
    return BINANCE_PREFIX + native[:-4]


def identity(symbol):
    if is_binance(symbol):
        return "binance", symbol[len(BINANCE_PREFIX):] + "USDT"
    return "hyperliquid", symbol


def observation_key(venue, native):
    return binance_key(native) if venue == "binance" else native
