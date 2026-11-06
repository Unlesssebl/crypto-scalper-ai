"""P3 acceptance report over closed UTC minutes.

The report is deliberately explicit about what it can and cannot prove.  It
never treats a missing minute as healthy, accounts for known gap intervals even
when they produced no window row, and refuses to pass a long run that was only
observed for a small part of the requested interval.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .collectors.specs import market_for_venue
from .contracts import WINDOW_MS, QualityState, known_channel
from .gaps import reconciled_covers_interval
from .paging import page_windows
from .storage import ChannelCoverage, MarketPulseStore

REPORT_SCHEMA = "market-pulse.acceptance.v1"


def required_channels(markets: Sequence[str] = ("BTC", "XRP")) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (venue, market_for_venue(venue, market), channel)
        for venue, channel in (("hyperliquid", "trades"), ("okx", "trades-all"))
        for market in markets
    )


USABLE_STATES = {
    QualityState.OBSERVED_NO_DETECTED_GAP.value,
    QualityState.RECONCILED.value,
}
EXCLUDED_STATES = {QualityState.DECLARED_OUTAGE.value}
UNUSABLE_STATES = {
    QualityState.PARTIAL.value,
    QualityState.GAP_DETECTED.value,
    QualityState.RECOVERING.value,
    QualityState.UNAVAILABLE.value,
    QualityState.WARMING_UP.value,
}
ACTIVE_GAP_STATES = {
    QualityState.GAP_DETECTED.value,
    QualityState.RECOVERING.value,
}


@dataclass(frozen=True, slots=True)
class ChannelAcceptance:
    venue: str
    native_market_id: str
    channel: str
    aggregation_kind: str
    coverage_state: str | None
    expected_minutes: int
    effective_minutes: int
    observed_span_start_ms: int | None
    observed_span_end_ms: int | None
    outside_coverage_minutes: int
    closed_windows: int
    provisional_windows: int
    missing_windows: int
    usable_windows: int
    declared_outage_windows: int
    unusable_windows: int
    quiet_minutes: int
    qualities: dict[str, int]
    gap_states: dict[str, int]
    conflicts: int
    required_pct: float

    @property
    def denominator(self) -> int:
        return self.effective_minutes - self.declared_outage_windows

    @property
    def usable_pct(self) -> float | None:
        if self.denominator <= 0:
            return None
        return self.usable_windows / self.denominator * 100

    @property
    def coverage_span_pct(self) -> float | None:
        if self.expected_minutes <= 0:
            return None
        return self.effective_minutes / self.expected_minutes * 100

    @property
    def passed(self) -> bool:
        # A pass means the measured usable minutes met the target over a span
        # that actually covers the requested interval.  Public trade
        # websockets still cannot prove that nothing was missed.
        return (
            self.usable_pct is not None
            and self.coverage_span_pct is not None
            and self.usable_pct >= self.required_pct
            and self.coverage_span_pct >= self.required_pct
            and self.provisional_windows == 0
            and self.conflicts == 0
            and self.missing_windows == 0
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "native_market_id": self.native_market_id,
            "channel": self.channel,
            "aggregation_kind": self.aggregation_kind,
            "coverage_state": self.coverage_state,
            "expected_minutes": self.expected_minutes,
            "effective_minutes": self.effective_minutes,
            "observed_span_start_ms": self.observed_span_start_ms,
            "observed_span_end_ms": self.observed_span_end_ms,
            "outside_coverage_minutes": self.outside_coverage_minutes,
            "closed_windows": self.closed_windows,
            "provisional_windows": self.provisional_windows,
            "missing_windows": self.missing_windows,
            "usable_windows": self.usable_windows,
            "declared_outage_windows": self.declared_outage_windows,
            "unusable_windows": self.unusable_windows,
            "quiet_minutes": self.quiet_minutes,
            "usable_pct": None if self.usable_pct is None else round(self.usable_pct, 3),
            "coverage_span_pct": (
                None if self.coverage_span_pct is None else round(self.coverage_span_pct, 3)
            ),
            "qualities": dict(sorted(self.qualities.items())),
            "gap_states": dict(sorted(self.gap_states.items())),
            "conflicts": self.conflicts,
            "required_pct": self.required_pct,
            "passed": self.passed,
        }


def _overlaps(gap: Any, window_start_ms: int) -> bool:
    window_end_ms = window_start_ms + WINDOW_MS
    return gap.start_ms < window_end_ms and (
        gap.end_ms is None or gap.end_ms > window_start_ms
    )


def _count(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


async def _channel_windows(
    store: MarketPulseStore,
    venue: str,
    native_market_id: str,
    channel: str,
    *,
    start_ms: int,
    end_ms: int,
) -> list[Any]:
    rows: list[Any] = []
    cursor = start_ms
    while cursor < end_ms:
        page, next_cursor = await page_windows(
            store,
            venue=venue,
            native_market_id=native_market_id,
            from_ms=cursor,
            to_ms=end_ms,
            limit=5_000,
        )
        if not page:
            break
        rows.extend(item for item in page if item.channel == channel)
        if next_cursor is None:
            break
        cursor = next_cursor
    return rows


async def build_acceptance_report(
    store: MarketPulseStore,
    *,
    start_ms: int,
    end_ms: int,
    required_pct: float = 99.0,
    expected_channels: Sequence[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    if end_ms <= start_ms:
        raise ValueError("end_ms must be after start_ms")
    start_ms = start_ms // WINDOW_MS * WINDOW_MS
    end_ms = end_ms // WINDOW_MS * WINDOW_MS
    expected = (end_ms - start_ms) // WINDOW_MS
    if expected <= 0:
        raise ValueError("acceptance requires at least one complete UTC minute")
    if not 0 < required_pct <= 100:
        raise ValueError("required_pct must be in (0, 100]")
    selected = tuple(dict.fromkeys(required_channels() if expected_channels is None else expected_channels))
    if not selected:
        raise ValueError("expected_channels must not be empty")

    coverage = await store.list_coverage()
    by_channel = {(item.venue, item.native_market_id, item.channel): item for item in coverage}
    missing_channels = [key for key in selected if key not in by_channel]
    conflicts = await store.list_conflicts(limit=100_000)
    gaps = await store.list_gaps(limit=100_000)

    channels: list[ChannelAcceptance] = []
    for key in selected:
        item = by_channel.get(key) or ChannelCoverage(
            venue=key[0], native_market_id=key[1], channel=key[2], source_epoch="",
            state=QualityState.UNAVAILABLE.value, updated_at_ms=0,
            reason="required channel never observed",
        )
        kind = known_channel(item.venue, item.channel).value
        windows = await _channel_windows(
            store,
            item.venue,
            item.native_market_id,
            item.channel,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        channel_conflicts = [
            conflict
            for conflict in conflicts
            if conflict.venue == item.venue
            and conflict.native_market_id == item.native_market_id
            and conflict.channel == item.channel
        ]
        channel_gaps = [
            gap
            for gap in gaps
            if gap.venue == item.venue
            and gap.native_market_id == item.native_market_id
            and gap.channel == item.channel
            and gap.start_ms < end_ms
            and (gap.end_ms is None or gap.end_ms > start_ms)
        ]

        # The observed span includes known gaps, so an outage is never trimmed
        # away as "outside coverage".
        span_points_start: list[int] = []
        span_points_end: list[int] = []
        if windows:
            span_points_start.append(min(window.window_start_ms for window in windows))
            span_points_end.append(max(window.window_start_ms for window in windows) + WINDOW_MS)
        for gap in channel_gaps:
            span_points_start.append(max(start_ms, gap.start_ms))
            span_points_end.append(
                end_ms if gap.end_ms is None else min(end_ms, gap.end_ms)
            )
        if span_points_start and span_points_end:
            span_start: int | None = min(span_points_start)
            span_end: int | None = max(span_points_end)
            span_start = max(start_ms, span_start) // WINDOW_MS * WINDOW_MS
            span_end = min(
                end_ms, ((span_end + WINDOW_MS - 1) // WINDOW_MS) * WINDOW_MS
            )
            effective = max(0, (span_end - span_start) // WINDOW_MS)
            outside = max(0, (span_start - start_ms) // WINDOW_MS) + max(
                0, (end_ms - span_end) // WINDOW_MS
            )
        else:
            span_start = span_end = None
            effective = 0
            outside = expected

        by_start = {window.window_start_ms: window for window in windows}
        qualities: dict[str, int] = {}
        closed_windows = 0
        provisional = 0
        missing = 0
        usable = 0
        declared = 0
        unusable = 0
        quiet = 0
        for window_start in range(
            0 if span_start is None else span_start,
            0 if span_end is None else span_end,
            WINDOW_MS,
        ):
            states = {
                gap.state
                for gap in channel_gaps
                if _overlaps(gap, window_start)
            }
            window = by_start.get(window_start)
            if window is not None and window.provisional:
                provisional += 1
                continue
            if window is not None:
                closed_windows += 1
            if QualityState.DECLARED_OUTAGE.value in states:
                declared += 1
                qualities[QualityState.DECLARED_OUTAGE.value] = (
                    qualities.get(QualityState.DECLARED_OUTAGE.value, 0) + 1
                )
                continue
            if states & ACTIVE_GAP_STATES:
                unusable += 1
                qualities[QualityState.GAP_DETECTED.value] = (
                    qualities.get(QualityState.GAP_DETECTED.value, 0) + 1
                )
                continue
            if window is not None:
                qualities[window.quality] = qualities.get(window.quality, 0) + 1
                if window.quality in USABLE_STATES:
                    usable += 1
                elif window.quality in EXCLUDED_STATES:
                    declared += 1
                else:
                    unusable += 1
                continue
            if reconciled_covers_interval(channel_gaps, window_start, window_start + WINDOW_MS):
                usable += 1
            elif (
                item.state in USABLE_STATES
                and item.connected_since_ms is not None
                and item.connected_since_ms <= window_start
                and max(item.heartbeat_ms or 0, item.last_receive_ms or 0) >= window_start + WINDOW_MS
            ):
                # A live, gap-free channel can vouch for a minute without
                # trades: it is quiet, not missing.
                quiet += 1
                usable += 1
            else:
                missing += 1

        channels.append(
            ChannelAcceptance(
                venue=item.venue,
                native_market_id=item.native_market_id,
                channel=item.channel,
                aggregation_kind=kind,
                coverage_state=item.state,
                expected_minutes=expected,
                effective_minutes=effective,
                observed_span_start_ms=span_start,
                observed_span_end_ms=span_end,
                outside_coverage_minutes=outside,
                closed_windows=closed_windows,
                provisional_windows=provisional,
                missing_windows=missing,
                usable_windows=usable,
                declared_outage_windows=declared,
                unusable_windows=unusable,
                quiet_minutes=quiet,
                qualities=qualities,
                gap_states=_count(gap.state for gap in channel_gaps),
                conflicts=len(channel_conflicts),
                required_pct=required_pct,
            )
        )

    payloads = [channel.to_payload() for channel in channels]
    return {
        "schema_version": REPORT_SCHEMA,
        "from_ms": start_ms,
        "to_ms": end_ms,
        "expected_minutes": expected,
        "required_pct": required_pct,
        "channels": payloads,
        "expected_channels": [list(key) for key in selected],
        "missing_channels": [list(key) for key in missing_channels],
        "passed": not missing_channels and bool(channels) and all(row["passed"] for row in payloads),
        "limits": {
            "completeness_proof": "not claimed",
            "note": (
                "A pass means measured usable minutes met the target over a span "
                "that covers the requested interval; public trade websockets "
                "cannot prove that nothing was missed."
            ),
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Market Pulse P3 acceptance",
        "",
        f"- Window: `{report['from_ms']}` .. `{report['to_ms']}` "
        f"({report['expected_minutes']} minutes)",
        f"- Required usable: {report['required_pct']}%",
        f"- Overall: **{'PASS' if report['passed'] else 'FAIL'}**",
        f"- Missing required channels: {len(report.get('missing_channels', []))}",
        "",
        "| venue | market | channel | span % | effective | outside | closed | open "
        "| quiet | usable | declared | unusable | missing | usable % "
        "| gaps | conflicts | pass |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:"
        "|---|---|---|",
    ]
    for row in report["channels"]:
        pct = "n/a" if row["usable_pct"] is None else f"{row['usable_pct']:.2f}"
        span = "n/a" if row["coverage_span_pct"] is None else f"{row['coverage_span_pct']:.2f}"
        gaps = ", ".join(f"{key}={value}" for key, value in row["gap_states"].items()) or "-"
        lines.append(
            f"| {row['venue']} | {row['native_market_id']} | {row['channel']} "
            f"| {span} | {row['effective_minutes']} | {row['outside_coverage_minutes']} "
            f"| {row['closed_windows']} | {row['provisional_windows']} "
            f"| {row['quiet_minutes']} | {row['usable_windows']} "
            f"| {row['declared_outage_windows']} | {row['unusable_windows']} "
            f"| {row['missing_windows']} | {pct} | {gaps} "
            f"| {row['conflicts']} | {'yes' if row['passed'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "Declared outage minutes are excluded from the denominator. "
            "They are known losses, not recovered data.",
            "",
            "Provisional minutes block PASS; missing required channels also block PASS.",
            "",
            "A channel must be observed for at least the required share of the "
            "requested window; one healthy minute cannot pass a 24-hour report.",
            "",
            "Completeness proof is not claimed: `observed_no_detected_gap` only "
            "means no gap was detected on the trade channel.",
        ]
    )
    return "\n".join(lines) + "\n"
