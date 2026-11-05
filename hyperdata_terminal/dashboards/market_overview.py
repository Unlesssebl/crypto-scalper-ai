"""
Market Overview Dashboard -- Terminal Dashboard
Retro hacker aesthetic using rich + pyfiglet.

Shows funding rates, open interest, prices, and extreme funding for all
Hyperliquid assets in real-time.
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import random
import time
from datetime import datetime, timezone

import pyfiglet
from rich import box
from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from hyperdata_terminal.dashboards.native_panels import (
    danger_zone_panel,
    heatmap_panel,
    hlp_panel,
    liquidations_panel,
    orderflow_panel,
    smart_money_panel,
    whales_panel,
)

# ---------------------------------------------------------------------------
# Resolve project root so imports work when run as a script
# ---------------------------------------------------------------------------
from hyperdata_terminal.data_layer.market_data import AssetInfo, MarketData
from hyperdata_terminal.utils.helpers import format_pct as fmt_pct
from hyperdata_terminal.utils.helpers import format_price as fmt_price

# -- Formatting helpers (imported from central helpers) ---------------------
from hyperdata_terminal.utils.helpers import format_usd as fmt_usd


def fmt_funding(value: float) -> str:
    """Format a funding rate (hourly) as a percentage with sign."""
    pct = value * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.4f}%"


def make_bar(magnitude: float, max_magnitude: float, width: int = 20) -> str:
    """Create a visual bar using block chars."""
    if max_magnitude <= 0:
        return ""
    fill = int(min(abs(magnitude) / max_magnitude, 1.0) * width)
    return "\u2588" * fill + "\u2591" * (width - fill)


# -- Mock data generator ----------------------------------------------------

def generate_mock_assets(n: int = 25) -> list[AssetInfo]:
    """Generate realistic-looking mock asset data for demo mode."""
    mock_data = [
        ("BTC", 83_500.0, 500.0),
        ("ETH", 3_450.0, 50.0),
        ("SOL", 178.0, 5.0),
        ("DOGE", 0.165, 0.005),
        ("XRP", 0.62, 0.02),
        ("AVAX", 38.0, 1.5),
        ("LINK", 18.50, 0.5),
        ("ARB", 1.35, 0.1),
        ("WIF", 2.80, 0.15),
        ("PEPE", 0.0000125, 0.000002),
        ("SUI", 3.45, 0.2),
        ("APT", 11.80, 0.5),
        ("OP", 3.20, 0.15),
        ("SEI", 0.85, 0.05),
        ("TIA", 12.50, 0.8),
        ("JUP", 1.15, 0.08),
        ("ONDO", 1.45, 0.1),
        ("RENDER", 8.90, 0.4),
        ("INJ", 28.50, 1.5),
        ("FET", 2.35, 0.15),
        ("NEAR", 7.20, 0.3),
        ("FIL", 6.80, 0.25),
        ("ATOM", 11.50, 0.5),
        ("BONK", 0.000028, 0.000003),
        ("FLOKI", 0.00022, 0.00002),
    ]

    assets: list[AssetInfo] = []
    for symbol, base_price, jitter in mock_data[:n]:
        price = base_price + random.uniform(-jitter, jitter)
        funding = random.uniform(-0.0005, 0.0005)
        # Occasionally spike the funding
        if random.random() < 0.15:
            funding = random.choice([-1, 1]) * random.uniform(0.001, 0.005)

        oi_usd = random.uniform(5_000_000, 2_000_000_000)
        volume = random.uniform(10_000_000, 5_000_000_000)
        change = random.uniform(-0.08, 0.08)

        assets.append(AssetInfo(
            symbol=symbol,
            price=price,
            funding_rate=funding,
            open_interest=oi_usd,
            volume_24h=volume,
            price_change_24h_pct=change,
            mark_price=price * (1 + random.uniform(-0.001, 0.001)),
            index_price=price * (1 + random.uniform(-0.002, 0.002)),
        ))

    return sorted(assets, key=lambda a: a.volume_24h, reverse=True)


# -- Dashboard class --------------------------------------------------------

def _compact_panels(panels: list) -> list:
    return [panel for panel in panels if panel is not None]


class MarketOverviewDashboard:
    """Rich-powered terminal dashboard for Hyperliquid market overview."""

    def __init__(
        self,
        market_data: MarketData | None = None,
        demo: bool = False,
        refresh_rate: int = 10,
        asset_reader=None,
        signal_reader=None,
        context_reader=None,
        health_reader=None,
        extra_reader=None,
        page: str = "auto",
        rotate_seconds: float = 10.0,
    ) -> None:
        self.console = Console()
        self.market_data = market_data
        self.demo = demo
        self.cycle = 0
        self.refresh_rate = refresh_rate
        self.asset_reader = asset_reader
        self.signal_reader = signal_reader
        # Optional callable returning the native market-context block (L/S,
        # basis, IV, liquidations, whales, extreme funding). Kept as a reader
        # so the same widget serves the in-process hub and the remote screener.
        self.context_reader = context_reader
        # Optional callable returning the /v1/health subset (status, feeds,
        # per-venue orderflow). Rendered as a badge so the TUI never claims
        # LIVE while a feed is partial/stale.
        self.health_reader = health_reader
        # Optional (a)sync callable returning the native panels payload:
        # {"orderflow": [...], "liquidations": {...}, "danger_zone": {...},
        #  "whales": {...}}.
        self.extra_reader = extra_reader
        # Screen mode cannot scroll, so on short terminals the native panels
        # are split into two fitted pages that rotate. "all" pins the tall
        # single-page layout; "market"/"risk" pin one page.
        self.page = page if page in ("auto", "all", "market", "risk", "vault") else "auto"
        self.rotate_seconds = max(3.0, rotate_seconds)
        self.assets: list[AssetInfo] = []
        self.context: dict = {}
        self.health: dict = {}
        self.extra: dict = {}
        self.last_update: float = 0.0
        self.update_error: str | None = None

    # -- Header ---------------------------------------------------------------

    def _health_badge(self) -> tuple[str, str] | None:
        overall = str((self.health or {}).get("overall")
                      or (self.health or {}).get("status") or "").lower()
        if not overall:
            return None
        return {
            "ok": ("✓ LIVE", "bold bright_green"),
            "warn": ("⚠ PARTIAL", "bold yellow"),
            "stale": ("⚠ STALE", "bold bright_red"),
            "drift": ("⚠ DRIFT", "bold yellow"),
            "fail": ("⚠ DEGRADED", "bold bright_red"),
        }.get(overall, (f"● {overall.upper()}", "bold bright_white"))

    def _flow_label(self) -> str | None:
        venues = (self.health or {}).get("orderflow_venues") or {}
        if not venues:
            return None
        live = [venue for venue, info in venues.items()
                if (info or {}).get("status") == "ok"]
        names = {"hyperliquid": "HL", "binance": "BN"}
        return " + ".join(names.get(venue, venue.upper()) for venue in live) or "silent"

    def build_compact_header(self, page: str | None = None) -> Text:
        """One-line header for the density-first layout (no ASCII art)."""
        line = Text()
        line.append("\u26a1 HYPERDATA", style="bold bright_magenta")
        line.append("  MARKET OVERVIEW", style="bright_white")
        if page:
            line.append(f"  [{page.upper()}]", style="bold bright_cyan")
        line.append("  |  ", style="dim white")
        line.append("DEMO" if self.demo else "LIVE",
                    style="bold bright_green" if self.demo else "bold bright_red")
        line.append(f"  |  Assets {len(self.assets)}", style="bold bright_yellow")
        if badge := self._health_badge():
            line.append("  |  ", style="dim white")
            line.append(badge[0], style=badge[1])
        if flow := self._flow_label():
            line.append("  |  Flow: ", style="dim bright_white")
            line.append(flow, style="bright_white")
        line.append("  |  ", style="dim white")
        line.append(datetime.now(timezone.utc).strftime("%H:%M:%S UTC"), style="dim bright_white")
        return line

    def build_header(self) -> Text:
        """ASCII art header using pyfiglet."""
        ascii_art = pyfiglet.figlet_format("MARKET", font="small_slant")
        header = Text()
        for line in ascii_art.splitlines():
            header.append(line + "\n", style="bold bright_magenta")

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        mode_str = "DEMO" if self.demo else "LIVE"
        asset_count = len(self.assets)

        info_line = Text()
        info_line.append(f"  Cycle: #{self.cycle}", style="bright_white")
        info_line.append("  |  ", style="dim white")
        info_line.append(f"Refresh: {self.refresh_rate}s", style="bright_white")
        info_line.append("  |  ", style="dim white")
        info_line.append(
            f"Mode: {mode_str}",
            style="bold bright_green" if self.demo else "bold bright_red",
        )
        info_line.append("  |  ", style="dim white")
        info_line.append(f"Assets: {asset_count}", style="bold bright_yellow")

        if badge := self._health_badge():
            info_line.append("  |  ", style="dim white")
            info_line.append(badge[0], style=badge[1])
        if flow := self._flow_label():
            info_line.append("  |  Flow: ", style="dim bright_white")
            info_line.append(flow, style="bright_white")

        info_line.append("  |  ", style="dim white")
        info_line.append(now_str, style="dim bright_white")

        header.append("\n")
        header.append(info_line)
        return header

    # -- Top assets table -----------------------------------------------------

    def build_assets_table(self, assets: list[AssetInfo], limit: int = 20) -> Table:
        """Top N assets by volume. Columns: symbol, price, 24h change, funding, OI."""
        table = Table(
            title="\U0001f4ca TOP ASSETS BY VOLUME",
            title_style="bold bright_magenta",
            box=box.HEAVY_EDGE,
            border_style="bright_magenta",
            header_style="bold bright_white",
            pad_edge=True,
            expand=True,
        )
        table.add_column("#", style="dim white", justify="right", width=3)
        table.add_column("SYMBOL", style="bold bright_white", justify="center", min_width=7)
        table.add_column("PRICE", style="bright_white", justify="right", min_width=12)
        table.add_column("24H CHG", justify="right", min_width=9)
        table.add_column("FUNDING", justify="right", min_width=10)
        table.add_column("OI", style="bright_white", justify="right", min_width=10)
        table.add_column("24H VOL", style="bright_white", justify="right", min_width=10)

        if self.signal_reader is not None:
            table.add_column("1M AI", min_width=10)
            table.add_column("5M AI", min_width=10)

        for idx, asset in enumerate(assets[:limit], 1):
            # Price change coloring
            if asset.price_change_24h_pct >= 0:
                chg_style = "bold bright_green"
            else:
                chg_style = "bold bright_red"

            # Funding rate coloring (positive = longs pay, green; negative = shorts pay, red)
            if asset.funding_rate >= 0:
                fund_style = "bright_green"
            else:
                fund_style = "bright_red"

            table.add_row(
                str(idx),
                Text(asset.symbol),        # exchange-supplied: never parse as markup
                fmt_price(asset.price),
                Text(fmt_pct(asset.price_change_24h_pct), style=chg_style),
                Text(fmt_funding(asset.funding_rate), style=fund_style),
                fmt_usd(asset.open_interest),
                fmt_usd(asset.volume_24h),
                *(self.signal_reader(asset.symbol) if self.signal_reader else ()),
            )

        if not assets:
            table.add_row(
                "--", "--",
                Text("Fetching...", style="bold yellow"),
                "--", "--", "--", "--",
            )

        return table

    # -- Extreme funding table ------------------------------------------------

    def build_extreme_funding(self, assets: list[AssetInfo], limit: int = 10) -> Table:
        """Assets with extreme funding rates. Show annualized rate with visual bar."""
        # Sort by absolute funding rate descending
        by_funding = sorted(assets, key=lambda a: abs(a.funding_rate), reverse=True)
        # Filter to meaningful funding (annualized > ~10%)
        extreme = [
            a for a in by_funding
            if abs(a.funding_rate) * 8760 >= 0.10
        ][:limit]

        table = Table(
            title="\U0001f525 EXTREME FUNDING RATES",
            title_style="bold bright_magenta",
            box=box.HEAVY_EDGE,
            border_style="bright_magenta",
            header_style="bold bright_white",
            pad_edge=True,
            expand=True,
        )
        table.add_column("SYMBOL", style="bold bright_white", justify="center", min_width=7)
        table.add_column("HOURLY", justify="right", min_width=11)
        table.add_column("ANNUALIZED", justify="right", min_width=12)
        table.add_column("VISUAL", justify="left", min_width=22)

        if not extreme:
            table.add_row(
                "--",
                Text("No extreme rates", style="dim white"),
                "--", "--",
            )
            return table

        max_annualized = max(abs(a.funding_rate) * 8760 for a in extreme) if extreme else 1.0

        for asset in extreme:
            annualized = asset.funding_rate * 8760
            abs_ann = abs(annualized)

            if asset.funding_rate >= 0:
                rate_style = "bold bright_green"
            else:
                rate_style = "bold bright_red"

            bar = make_bar(abs_ann, max_annualized, width=20)

            table.add_row(
                Text(asset.symbol),
                Text(fmt_funding(asset.funding_rate), style=rate_style),
                Text(fmt_pct(annualized, include_sign=True), style=rate_style),
                Text(bar, style=rate_style),
            )

        return table

    # -- Stats bar ------------------------------------------------------------

    def build_stats_bar(self) -> Text:
        """One-line summary of market stats."""
        if not self.assets:
            return Text("  Loading market data...", style="dim bright_white")

        total_oi = sum(a.open_interest for a in self.assets)
        total_vol = sum(a.volume_24h for a in self.assets)
        avg_funding = (
            sum(a.funding_rate for a in self.assets) / len(self.assets)
            if self.assets else 0.0
        )
        positive_count = sum(1 for a in self.assets if a.price_change_24h_pct >= 0)
        negative_count = len(self.assets) - positive_count

        bar = Text()
        bar.append("  TOTAL OI: ", style="dim bright_white")
        bar.append(fmt_usd(total_oi), style="bold bright_yellow")
        bar.append("  |  ", style="dim white")
        bar.append("24H VOL: ", style="dim bright_white")
        bar.append(fmt_usd(total_vol), style="bold bright_yellow")
        bar.append("  |  ", style="dim white")
        bar.append("AVG FUNDING: ", style="dim bright_white")
        avg_fund_style = "bright_green" if avg_funding >= 0 else "bright_red"
        bar.append(fmt_funding(avg_funding), style=avg_fund_style)
        bar.append("  |  ", style="dim white")
        bar.append(f"{positive_count} ", style="bright_green")
        bar.append("green", style="dim bright_white")
        bar.append("  ", style="dim white")
        bar.append(f"{negative_count} ", style="bright_red")
        bar.append("red", style="dim bright_white")

        if self.update_error:
            bar.append("  |  ", style="dim white")
            bar.append(f"ERR: {self.update_error}", style="bold red")

        return bar

    # -- Native market context ------------------------------------------------

    def build_context_panel(self, context: dict) -> Table | None:
        """Risk-appetite block from the native hub/API.

        Renders only what is present: the in-process hub and the remote
        screener populate different subsets (e.g. demo has no liquidations),
        so a missing key is omitted rather than shown as a zero.
        """
        if not context:
            return None
        table = Table(
            title="\U0001f9ed NATIVE CONTEXT \u00b7 RISK APPETITE",
            title_style="bold bright_cyan",
            box=box.SIMPLE_HEAVY,
            border_style="bright_cyan",
            header_style="bold bright_white",
            expand=True,
            padding=(0, 1),
        )
        table.add_column("Signal", style="bold bright_cyan", min_width=16)
        table.add_column("BTC", justify="right")
        table.add_column("ETH", justify="right")
        table.add_column("SOL", justify="right")
        table.add_column("Note", style="dim bright_white")

        def cell(value, digits=2, suffix=""):
            if value is None:
                return Text("—", style="dim white")
            return Text(f"{value:.{digits}f}{suffix}")

        lsr = context.get("lsr") or {}
        if lsr:
            table.add_row(
                "Long/Short",
                cell(lsr.get("BTC", {}).get("long_short_ratio")),
                cell(lsr.get("ETH", {}).get("long_short_ratio")),
                cell(lsr.get("SOL", {}).get("long_short_ratio")),
                Text(", ".join(sorted({v.get("source", "?") for v in lsr.values()})), style="dim"),
            )

        basis = context.get("basis") or {}
        if basis:
            table.add_row(
                "Spot-perp basis",
                cell(basis.get("BTC", {}).get("basis_pct"), suffix="%"),
                cell(basis.get("ETH", {}).get("basis_pct"), suffix="%"),
                cell(basis.get("SOL", {}).get("basis_pct"), suffix="%"),
                Text(", ".join(sorted({v.get("source", "?") for v in basis.values()})), style="dim"),
            )

        iv = context.get("iv") or {}
        if iv:
            table.add_row(
                "Deribit IV",
                cell(iv.get("BTC", {}).get("mark_iv"), digits=1, suffix="%"),
                cell(iv.get("ETH", {}).get("mark_iv"), digits=1, suffix="%"),
                Text("—", style="dim white"),
                Text("implied vol", style="dim"),
            )

        liquidations = context.get("liquidations") or {}
        if liquidations:
            count = liquidations.get("confirmed_count")
            volume = liquidations.get("confirmed_volume_usd")
            note = Text(f"{int(volume):,} USD · 1h".replace(",", " "), style="dim")
            table.add_row(
                "Liquidations",
                Text(str(count) if count is not None else "—"),
                Text("", style="dim"),
                Text("", style="dim"),
                note,
            )

        whales = context.get("whales") or []
        if whales:
            largest = max(whales, key=lambda item: item.get("size_usd") or 0)
            table.add_row(
                "Whales",
                Text(f"{len(whales)} tracked", style="bright_white"),
                Text("", style="dim"),
                Text("", style="dim"),
                Text(f"largest {largest.get('symbol', '?')} {fmt_usd(largest.get('size_usd') or 0)}",
                     style="dim"),
            )

        extreme = context.get("extreme_funding") or []
        if extreme:
            top = ", ".join(
                f"{item['symbol']} {item['annualized_pct']:+.0f}%" for item in extreme[:5]
            )
            table.add_row(
                "Extreme funding", Text("", style="dim"), Text("", style="dim"), Text("", style="dim"),
                Text(top, style="dim"),
            )

        return table

    # -- Full dashboard -------------------------------------------------------

    @staticmethod
    def _panel_grid(panels: list, per_row: int = 3) -> list[Table]:
        """Lay panels out in fixed equal-width rows (no width balancing)."""
        rows = []
        for start in range(0, len(panels), per_row):
            chunk = panels[start:start + per_row]
            grid = Table.grid(expand=True, padding=(0, 1))
            for _ in chunk:
                grid.add_column(ratio=1)
            grid.add_row(*chunk)
            rows.append(grid)
        return rows

    def _current_page(self, height: int) -> str:
        if self.page in ("all", "market", "risk", "vault"):
            return self.page
        if height >= 75:
            return "all"
        slot = int(time.time() / self.rotate_seconds) % 3
        return ("market", "risk", "vault")[slot]

    def build_dashboard(self) -> Panel:
        """Compose the dashboard, sized to the terminal (no scrolling)."""
        # Rich's screen mode cannot scroll, so fit the content to the height
        # we actually have: drop the ASCII art, shrink tables and, on short
        # terminals, rotate between two fitted pages.
        height = max(24, self.console.size.height or 50)
        page = self._current_page(height)
        compact = bool(self.extra) or page != "all"
        header = (self.build_compact_header(page if page != "all" else None)
                  if compact else self.build_header())

        if not self.assets:
            scanning = Text()
            scanning.append("\n\n")
            scanning.append(
                "    \U0001f50d  Fetching market data from Hyperliquid...\n\n",
                style="bold bright_yellow",
            )
            scanning.append(
                "    Loading prices, funding rates, and open interest.\n",
                style="dim bright_white",
            )
            scanning.append(
                "    This may take a moment on the first run.\n\n",
                style="dim bright_white",
            )
            content = Group(Align.center(header), Text(""), scanning)
        else:
            extra = self.extra or {}
            small = 3 if height >= 48 else 2
            assets_limit = 12 if height >= 50 else 8 if height >= 42 else 6
            if height < 36:
                assets_limit = 4
            funding_limit = 5 if height >= 50 else 3
            order_limit = 6 if height >= 50 else 4 if height >= 40 else 3
            heat_limit = 6 if height >= 50 else 4 if height >= 40 else 3
            if height < 36:
                heat_limit = 2

            orderflow = orderflow_panel(extra.get("orderflow") or [], limit=order_limit)
            context_panel = self.build_context_panel(self.context)
            liquidations = liquidations_panel(extra.get("liquidations"), limit=small)
            danger = danger_zone_panel(extra.get("danger_zone"), limit=small)
            whales = whales_panel(extra.get("whales"), limit=small)
            hlp = hlp_panel(extra.get("hlp"), limit=small)
            heatmap = heatmap_panel(extra.get("heatmap"), limit=heat_limit)
            smart = smart_money_panel(extra.get("smart_money"), limit=small)
            stats = self.build_stats_bar()

            if page == "market":
                sections = [header,
                            self.build_assets_table(self.assets, limit=assets_limit)]
                if orderflow is not None:
                    sections.append(orderflow)
                if context_panel is not None:
                    sections.append(context_panel)
                if height >= 46:
                    sections.append(self.build_extreme_funding(self.assets, limit=funding_limit))
                sections.append(stats)
            elif page == "risk":
                sections = [header]
                grid = _compact_panels([liquidations, danger, whales])
                if grid:
                    sections.append(self._panel_grid(grid, per_row=3)[0])
                sections.append(stats)
            elif page == "vault":
                sections = [header]
                for panel in (hlp, heatmap, smart):
                    if panel is not None:
                        sections.append(panel)
                if height >= 36:
                    sections.append(stats)
            else:
                sections = [Align.center(header), Text(""),
                            self.build_assets_table(self.assets, limit=20), Text("")]
                if orderflow is not None:
                    sections.extend([orderflow, Text("")])
                if context_panel is not None:
                    sections.extend([context_panel, Text("")])
                sections.extend(self._panel_grid(
                    _compact_panels([liquidations, danger, whales, hlp, heatmap, smart]),
                    per_row=3))
                sections.extend([
                    self.build_extreme_funding(self.assets, limit=10),
                    Text(""), stats, Text(""),
                ])
            content = Group(*sections)

        return Panel(
            content,
            title="[bold bright_magenta]\U0001f4ca HYPERDATA \u2014 MARKET OVERVIEW \U0001f4ca[/]",
            subtitle="[dim bright_white]Ctrl+C to exit[/]",
            border_style="bright_magenta",
            box=box.DOUBLE_EDGE,
            padding=(1, 2),
        )

    # -- Compact build (for combined dashboard) --------------------------------

    def build_compact(self) -> Panel:
        """Compact version for the multi-panel combined view."""
        if not self.assets:
            content = Text("  Loading market data...", style="dim bright_yellow")
            return Panel(
                content,
                title="[bold bright_magenta]\U0001f4ca MARKET[/]",
                border_style="bright_magenta",
                box=box.ROUNDED,
                padding=(0, 1),
            )

        # Compact table: top 8 assets, fewer columns
        table = Table(
            box=box.SIMPLE_HEAVY,
            border_style="bright_magenta",
            header_style="bold bright_white",
            expand=True,
            padding=(0, 1),
        )
        table.add_column("SYM", style="bold bright_white", justify="center", width=5)
        table.add_column("PRICE", justify="right", min_width=10)
        table.add_column("CHG", justify="right", min_width=8)
        table.add_column("FUND", justify="right", min_width=9)
        table.add_column("OI", justify="right", min_width=8)

        for asset in self.assets[:8]:
            chg_style = "bright_green" if asset.price_change_24h_pct >= 0 else "bright_red"
            fund_style = "bright_green" if asset.funding_rate >= 0 else "bright_red"

            table.add_row(
                Text(asset.symbol),
                fmt_price(asset.price),
                Text(fmt_pct(asset.price_change_24h_pct), style=chg_style),
                Text(fmt_funding(asset.funding_rate), style=fund_style),
                fmt_usd(asset.open_interest),
            )

        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        mode = "DEMO" if self.demo else "LIVE"

        return Panel(
            table,
            title=f"[bold bright_magenta]\U0001f4ca MARKET ({mode})[/]",
            subtitle=f"[dim]{now_str}  #{self.cycle}[/]",
            border_style="bright_magenta",
            box=box.ROUNDED,
            padding=(0, 1),
        )

    # -- Data update ----------------------------------------------------------

    def _refresh_context(self) -> None:
        if self.context_reader is None:
            return
        try:
            self.context = self.context_reader() or {}
        except Exception:
            # Context is supplementary: never let it break the market table.
            self.context = {}

    def _refresh_health(self) -> None:
        if self.health_reader is None:
            return
        try:
            self.health = self.health_reader() or {}
        except Exception:
            self.health = {}

    async def _refresh_extra(self) -> None:
        if self.extra_reader is None:
            return
        try:
            value = self.extra_reader()
            if inspect.isawaitable(value):
                value = await value
            self.extra = value or {}
        except Exception:
            # Native panels are supplementary; never blank the market table.
            self.extra = {}

    async def update_data(self) -> None:
        """Fetch new data or generate mock data."""
        self._refresh_context()
        self._refresh_health()
        await self._refresh_extra()
        if self.asset_reader is not None:
            self.assets = self.asset_reader()
            self.last_update = time.time()
            return

        if self.demo:
            self.assets = generate_mock_assets(n=25)
            self.last_update = time.time()
            return

        if self.market_data is None:
            return

        try:
            self.update_error = None
            self.assets = await self.market_data.get_all()
            self.last_update = time.time()
        except Exception as exc:
            self.update_error = str(exc)[:60]

    # -- Main loop ------------------------------------------------------------

    async def run(self) -> None:
        """Main loop: refresh market data and update display."""
        self.console.clear()
        with Live(
            self.build_dashboard(),
            console=self.console,
            refresh_per_second=2,
            screen=True,
            vertical_overflow="crop",
        ) as live:
            try:
                while True:
                    self.cycle += 1
                    await self.update_data()
                    live.update(self.build_dashboard())
                    await asyncio.sleep(self.refresh_rate)
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass


# -- CLI entrypoint ---------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market Overview -- Terminal Dashboard",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="Connect to live Hyperliquid API (default: demo mode with mock data)",
    )
    parser.add_argument(
        "--refresh",
        type=int,
        default=10,
        help="Refresh interval in seconds (default: 10)",
    )
    args = parser.parse_args()

    demo = not args.live
    market_data = None if demo else MarketData()

    dashboard = MarketOverviewDashboard(
        market_data=market_data,
        demo=demo,
        refresh_rate=args.refresh,
    )

    try:
        asyncio.run(dashboard.run())
    except KeyboardInterrupt:
        Console().print("\n[bold bright_magenta]Dashboard stopped.[/]")


if __name__ == "__main__":
    main()
