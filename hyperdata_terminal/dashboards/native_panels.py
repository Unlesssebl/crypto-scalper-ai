"""Rich panels for the native data the external screener can reach.

Pure presentation helpers: each takes a plain dict/list (API payload or a
normalized row) and returns a Rich renderable, or None when there is nothing
to show. Both the in-process hub and `RemoteHub` feed the same builders, so
the TUI and the API never drift into two different layouts.
"""
from __future__ import annotations

from datetime import datetime, timezone

from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from hyperdata_terminal.utils.helpers import format_usd as fmt_usd

BULL_STYLE = "bold bright_green"
BEAR_STYLE = "bold bright_red"


def _get(obj, name, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _signal_style(signal: str | None) -> str:
    signal = (signal or "").upper()
    if "BULL" in signal:
        return BULL_STYLE
    if "BEAR" in signal:
        return BEAR_STYLE
    return "dim bright_white"


def _side_style(side: str | None) -> str:
    return BEAR_STYLE if (side or "").lower() in ("short", "sell") else BULL_STYLE


def _base_table(title: str, border: str) -> Table:
    return Table(
        title=title,
        title_style=f"bold {border}",
        box=box.SIMPLE_HEAVY,
        border_style=border,
        header_style="bold bright_white",
        expand=True,
        padding=(0, 1),
    )


def orderflow_panel(rows, limit: int = 6) -> Table | None:
    """CVD/OFI by symbol with an explicit venue label."""
    if not rows:
        return None
    table = _base_table("\U0001f30a ORDER FLOW \u00b7 1m (native)", "bright_blue")
    table.add_column("SYM", style="bold bright_white", min_width=6)
    table.add_column("SIGNAL", min_width=10)
    table.add_column("VENUE", min_width=8)
    table.add_column("CVD", justify="right", min_width=10)
    table.add_column("OFI", justify="right", min_width=7)
    table.add_column("TRADES", justify="right", min_width=7)
    table.add_column("Q", min_width=9)
    for row in rows[:limit]:
        signal = row.get("signal") or "NEUTRAL"
        cvd = row.get("cvd")
        ofi = row.get("ofi")
        table.add_row(
            Text(str(row.get("symbol", "?"))),
            Text(str(signal), style=_signal_style(signal)),
            Text(str(row.get("venue") or "?"), style="dim bright_cyan"),
            Text(fmt_usd(cvd) if cvd is not None else "—"),
            Text(f"{ofi:+.0%}" if ofi is not None else "—"),
            Text(str(row.get("trades")) if row.get("trades") is not None else "—"),
            Text(str(row.get("quality") or "—"), style="dim"),
        )
    return table


def liquidations_panel(stats, limit: int = 4) -> Table | None:
    """Confirmed liquidations over the window, by exchange then symbol."""
    if not stats:
        return None
    table = _base_table("\U0001f4a5 LIQUIDATIONS \u00b7 60m (confirmed)", "bright_red")
    table.add_column("GROUP", style="bold bright_white", min_width=10)
    table.add_column("COUNT", justify="right", min_width=6)
    table.add_column("VOLUME", justify="right", min_width=12)

    confirmed = stats.get("confirmed_count") or 0
    volume = stats.get("confirmed_volume_usd") or 0.0
    table.add_row(
        Text("TOTAL", style="bold bright_white"),
        Text(str(confirmed)),
        Text(fmt_usd(volume)),
    )
    by_exchange = stats.get("by_exchange") or {}
    for exchange, data in list(by_exchange.items())[:limit]:
        table.add_row(
            Text(f"  {exchange}", style="dim bright_cyan"),
            Text(str((data or {}).get("count", 0))),
            Text(fmt_usd((data or {}).get("volume_usd") or 0.0)),
        )
    by_symbol = stats.get("by_symbol") or {}
    for symbol, data in list(by_symbol.items())[:limit]:
        table.add_row(
            Text(f"  {symbol}", style="dim"),
            Text(str((data or {}).get("count", 0))),
            Text(fmt_usd((data or {}).get("volume_usd") or 0.0)),
        )
    return table


def positions_panel(payload, *, title: str, limit: int = 5) -> Table | None:
    """A positions table shared by the danger-zone and whale views."""
    positions = (payload or {}).get("positions") or []
    if not positions:
        return None
    table = _base_table(title, "bright_yellow")
    table.add_column("SYM", style="bold bright_white", min_width=6)
    table.add_column("SIDE", min_width=6)
    table.add_column("SIZE", justify="right", min_width=10)
    table.add_column("LEV", justify="right", min_width=5)
    table.add_column("DIST", justify="right", min_width=7)
    for position in positions[:limit]:
        side = _get(position, "side", "?")
        distance = _get(position, "distance_pct")
        leverage = _get(position, "leverage")
        table.add_row(
            Text(str(_get(position, "symbol", "?"))),
            Text(str(side), style=_side_style(side)),
            Text(fmt_usd(_get(position, "size_usd") or 0.0)),
            Text(f"{leverage:.0f}x" if leverage is not None else "—"),
            Text(f"{distance:.1f}%" if distance is not None else "—"),
        )
    return table


def danger_zone_panel(payload, limit: int = 5) -> Table | None:
    return positions_panel(payload, title="\u26a0 DANGER ZONE \u00b7 closest to liquidation", limit=limit)


def whales_panel(payload, limit: int = 5) -> Table | None:
    return positions_panel(payload, title="\U0001f40b LARGEST POSITIONS", limit=limit)


def hlp_panel(payload, limit: int = 3) -> Panel | None:
    """HLP vault summary plus its largest positions."""
    if not payload or payload.get("warmup"):
        return None
    header = Text()
    header.append(" AUM ", style="dim")
    header.append(fmt_usd(payload.get("account_value") or 0.0), style="bold bright_white")
    header.append("  PnL ", style="dim")
    pnl = payload.get("session_pnl") or 0.0
    if payload.get("pnl_known"):
        header.append(f"{'+' if pnl >= 0 else ''}{fmt_usd(pnl)}",
                      style="bright_green" if pnl >= 0 else "bright_red")
    else:
        header.append("--", style="dim")
    header.append(f"  Pos {payload.get('num_positions') or 0}", style="bright_white")

    delta = payload.get("net_delta") or 0.0
    zscore = payload.get("delta_zscore") or 0.0
    delta_line = Text()
    delta_line.append(" Net \u0394 ", style="dim")
    delta_line.append(f"{'+' if delta >= 0 else ''}{fmt_usd(delta)}",
                      style="bright_green" if delta >= 0 else "bright_red")
    delta_line.append("  Z ", style="dim")
    delta_line.append(f"{zscore:+.2f}",
                      style="bold bright_red" if abs(zscore) > 2 else "bright_white")
    delta_line.append(f"  Gross {fmt_usd(payload.get('total_exposure') or 0.0)}", style="dim")

    table = Table(box=box.SIMPLE_HEAVY, border_style="bright_magenta",
                  header_style="bold bright_white", expand=True, padding=(0, 0))
    table.add_column("L/S", justify="center", width=3)
    table.add_column("SYM", justify="center", min_width=5)
    table.add_column("SIZE", justify="right", min_width=8)
    table.add_column("PnL", justify="right", min_width=7)
    table.add_column("LV", justify="right", width=3)
    for position in (payload.get("positions") or [])[:limit]:
        side = _get(position, "side", "?")
        pnl_value = _get(position, "unrealized_pnl") or 0.0
        leverage = _get(position, "leverage")
        table.add_row(
            Text(str(side)[:1].upper(), style=_side_style(side)),
            Text(str(_get(position, "symbol", "?"))),
            Text(fmt_usd(_get(position, "size_usd") or 0.0)),
            Text(f"{'+' if pnl_value >= 0 else ''}{fmt_usd(pnl_value)}",
                 style="bright_green" if pnl_value >= 0 else "bright_red"),
            Text(f"{leverage:.0f}x" if leverage is not None else "—"),
        )

    footer = Text()
    footer.append(f" Absorbed 24h: {payload.get('absorbed_24h') or 0}", style="bright_yellow")
    footer.append(f"  Fills: {payload.get('total_trades') or 0}", style="dim")

    return Panel(
        Group(header, delta_line, table, footer),
        title="\U0001f3e6 HLP VAULT", border_style="bright_magenta",
        box=box.ROUNDED, padding=(0, 1),
    )


def heatmap_panel(payload, limit: int = 8) -> Table | None:
    """Liquidation USD per price bucket around the current price."""
    buckets = [
        bucket for bucket in (payload or {}).get("buckets") or []
        if (bucket.get("long_usd") or 0) + (bucket.get("short_usd") or 0) > 0
    ]
    if not buckets:
        return None
    price = payload.get("price") or 0.0
    if price:
        buckets = sorted(buckets, key=lambda b: abs(b["mid"] - price))[:limit]
    else:
        buckets = buckets[:limit]
    buckets.sort(key=lambda b: b["mid"], reverse=True)
    peak = max((b.get("long_usd") or 0) + (b.get("short_usd") or 0) for b in buckets) or 1.0

    table = _base_table(
        f"\U0001f9ca LIQ HEATMAP \u00b7 {payload.get('symbol', '?')} "
        f"\u00b1{payload.get('range_pct', 15):.0f}%", "bright_cyan")
    table.add_column("PRICE", justify="right", min_width=9)
    table.add_column("LONG", justify="right", min_width=8)
    table.add_column("SHORT", justify="right", min_width=8)
    table.add_column("USD", min_width=10)
    for bucket in buckets:
        long_usd = bucket.get("long_usd") or 0.0
        short_usd = bucket.get("short_usd") or 0.0
        total = long_usd + short_usd
        bar = "\u2588" * max(1, int(total / peak * 14))
        table.add_row(
            Text(f"{bucket['mid']:.6g}"),
            Text(fmt_usd(long_usd) if long_usd else "—", style="bright_red"),
            Text(fmt_usd(short_usd) if short_usd else "—", style="bright_green"),
            Text(bar, style="bright_cyan"),
        )
    return table


def smart_money_panel(payload, limit: int = 3) -> Panel | None:
    """Top smart wallets plus the latest follow/fade signals."""
    if not payload:
        return None
    stats = payload.get("stats") or {}
    smart = payload.get("smart") or []
    signals = payload.get("signals") or []
    if not smart and not signals:
        return None

    header = Text()
    header.append(f" Tracked {stats.get('total_wallets', 0)}", style="bright_white")
    header.append(f"  Ranked {stats.get('ranked_wallets', 0)}", style="bright_yellow")
    header.append(f"  Smart {stats.get('smart_wallets', 0)}", style="bright_green")
    header.append(f"  Dumb {stats.get('dumb_wallets', 0)}", style="bright_red")

    table = Table(box=box.SIMPLE_HEAVY, border_style="bright_yellow",
                  header_style="bold bright_white", expand=True, padding=(0, 0))
    table.add_column("#", style="dim", width=3)
    table.add_column("WIN%", justify="right", min_width=5)
    table.add_column("CONF", justify="right", min_width=5)
    table.add_column("PnL", justify="right", min_width=8)
    for wallet in smart[:limit]:
        win = wallet.get("win_rate") or 0.0
        table.add_row(
            Text(f"#{wallet.get('rank', '?')}"),
            Text(f"{win * 100:.0f}%",
                 style="bright_green" if win >= 0.5 else "bright_red"),
            Text(f"{(wallet.get('confidence') or 0) * 100:.0f}%", style="dim"),
            Text(fmt_usd(wallet.get("realized_pnl") or 0.0),
                 style="bright_green" if (wallet.get("realized_pnl") or 0) >= 0 else "bright_red"),
        )

    lines = Text()
    for signal in signals[-limit:]:
        stamp = datetime.fromtimestamp(signal.get("timestamp", 0), tz=timezone.utc).strftime("%H:%M")
        follow = signal.get("signal_type") == "follow"
        lines.append(f" {stamp} ", style="dim")
        lines.append(f"{'FOLLOW' if follow else 'FADE'} #{signal.get('wallet_rank', '?')} ",
                     style="bold bright_green" if follow else "bold bright_red")
        lines.append(f"{signal.get('action', '')} {signal.get('symbol', '')} "
                     f"{fmt_usd(signal.get('size_usd') or 0.0)}\n", style="bright_white")
    if not lines.plain:
        lines.append(" Waiting for signals...", style="dim")

    return Panel(
        Group(header, table, lines),
        title="\U0001f9e0 SMART MONEY", border_style="bright_yellow",
        box=box.ROUNDED, padding=(0, 1),
    )
