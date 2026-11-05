"""Payload builders shared by the REST API and the in-process TUI.

Both consumers must see exactly the same numbers: the API serializes these
dicts, and `build_extra` feeds the same dicts to the Rich panels.
"""
from __future__ import annotations


def _symbol_price(hub, symbol: str) -> float:
    price = hub.positions.market_prices.get(symbol) or 0.0
    if not price:
        asset = hub.market.assets.get(symbol)
        price = getattr(asset, "mark_price", 0.0) or getattr(asset, "price", 0.0)
    return price


def hlp_payload(hub) -> dict:
    hlp = hub.hlp
    stats = hlp.get_stats()
    return {
        **stats,
        "warmup": hlp.get_latest_snapshot() is None,
        "positions": list(hlp.get_top_positions(10)),
        "absorptions": list(hlp.get_liquidation_absorptions(60)[-10:]),
        "absorbed_24h": len(hlp.get_liquidation_absorptions(1440)),
    }


def heatmap_payload(hub, symbol: str = "BTC", buckets: int = 40,
                    range_pct: float = 15.0) -> dict:
    from hyperdata_terminal.dashboards.liquidation_heatmap import compute_heatmap_buckets

    price = _symbol_price(hub, symbol)
    rows = compute_heatmap_buckets(
        list(hub.positions.positions), price,
        symbol=symbol, n_buckets=buckets, range_pct=range_pct,
    )
    return {
        "symbol": symbol,
        "price": price,
        "range_pct": range_pct,
        "scan_age_seconds": hub.positions.scan_age_seconds(),
        "buckets": [{
            "price_low": b.price_low, "price_high": b.price_high, "mid": b.mid,
            "long_usd": b.long_usd, "short_usd": b.short_usd,
            "long_count": b.long_count, "short_count": b.short_count,
        } for b in rows],
    }


def smart_money_payload(hub) -> dict:
    def wallet(profile) -> dict:
        return {
            "rank": profile.rank, "tier": profile.tier,
            "win_rate": profile.win_rate, "confidence": profile.confidence,
            "realized_pnl": profile.total_realized_pnl,
            "account_value": profile.account_value,
            "active_symbols": list(profile.active_symbols)[:3],
        }

    def signal(item) -> dict:
        return {
            "timestamp": item.timestamp, "tier": item.tier, "action": item.action,
            "symbol": item.symbol, "size_usd": item.size_usd,
            "wallet_rank": item.wallet_rank, "wallet_win_rate": item.wallet_win_rate,
            "signal_type": item.signal_type,
            "wallet_confidence": getattr(item, "wallet_confidence", 0.0),
        }

    return {
        "stats": hub.smart_money.get_stats(),
        "smart": [wallet(p) for p in hub.get_smart_money(5)],
        "dumb": [wallet(p) for p in hub.get_dumb_money(3)],
        "signals": [signal(s) for s in hub.get_smart_money_signals(8)],
    }
