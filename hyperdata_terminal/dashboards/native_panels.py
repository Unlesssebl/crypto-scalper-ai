"""Rich panels for the native data the external screener can reach.

Pure presentation helpers: each takes a plain dict/list (API payload or a
normalized row) and returns a Rich renderable, or None when there is nothing
to show. Both the in-process hub and `RemoteHub` feed the same builders, so
the TUI and the API never drift into two different layouts.
"""
from __future__ import annotations

from rich import box
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
