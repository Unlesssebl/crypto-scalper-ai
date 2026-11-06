"""P3 acceptance report over closed UTC minutes.

The report is deliberately explicit about what it can and cannot prove.  It
counts only closed windows, never treats a missing window as a healthy minute,
and separates declared outages and startup warming from real losses.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from .contracts import WINDOW_MS, QualityState, known_channel
from .storage import MarketPulseStore

REPORT_SCHEMA = "market-pulse.acceptance.v1"

USABLE_STATES = {
    QualityState.OBSERVED_NO_DETECTED_GAP.value,
    QualityState.RECONCILED.value,
}
EXCLUDED_STATES = {QualityState.DECLARED_OUTAGE.value}
UNUSABLE_STATES = {
    QualityState.GAP_DETECTED.value,
    QualityState.RECOVERING.value,
    QualityState.UNAVAILABLE.value,
    QualityState.WARMING_UP.value,
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
        return (
            self.effective_minutes
            - self.provisional_windows
            - self.declared_outage_windows
        )

    @property
    def usable_pct(self) -> float | None:
        if self.denominator <= 0:
            return None
        return self.usable_windows / self.denominator * 100

    @property
    def passed(self) -> bool:
        # Public trade websockets cannot prove completeness; a pass means the
        # measured usable minutes met the target, not that nothing was missed.
        return (
            self.usable_pct is not None
            and self.usable_pct >= self.required_pct
            and self.conflicts == 0
            and self.unusable_windows == 0
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
            "qualities": dict(sorted(self.qualities.items())),
            "gap_states": dict(sorted(self.gap_states.items())),
            "conflicts": self.conflicts,
            "required_pct": self.required_pct,
            "passed": self.passed,
        }


async def _windows_for_channel(
    store: MarketPulseStore,
    venue: str,
    native_market_id: str,
    channel: str,
    *,
    start_ms: int,
    end_ms: int,
) -> list[Any]:
    """Page through windows by ``window_start_ms`` without an OFFSET scan."""
    rows: list[Any] = []
    cursor = start_ms
    while cursor < end_ms:
        batch = await store.list_windows(
            venue=venue,
            native_market_id=native_market_id,
            from_ms=cursor,
            to_ms=end_ms,
            limit=5_000,
        )
        if not batch:
            break
        rows.extend(item for item in batch if item.channel == channel)
        next_cursor = max(item.window_start_ms for item in batch) + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor
    return rows


def _count(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


async def build_acceptance_report(
    store: MarketPulseStore,
    *,
    start_ms: int,
    end_ms: int,
    required_pct: float = 99.0,
) -> dict[str, Any]:
    if end_ms <= start_ms:
        raise ValueError("end_ms must be after start_ms")
    start_ms = start_ms // WINDOW_MS * WINDOW_MS
    end_ms = end_ms // WINDOW_MS * WINDOW_MS
    expected = (end_ms - start_ms) // WINDOW_MS

    coverage = await store.list_coverage()
    conflicts = await store.list_conflicts(limit=100_000)
    gaps = await store.list_gaps(limit=100_000)

    channels: list[ChannelAcceptance] = []
    for item in coverage:
        kind = known_channel(item.venue, item.channel).value
        windows = await _windows_for_channel(
            store,
            item.venue,
            item.native_market_id,
            item.channel,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        closed = [window for window in windows if not window.provisional]
        provisional = len(windows) - len(closed)
        starts = sorted({window.window_start_ms for window in windows})
        if starts:
            span_start: int | None = max(start_ms, starts[0])
            span_end: int | None = min(end_ms, starts[-1] + WINDOW_MS)
            outside = max(0, (span_start - start_ms) // WINDOW_MS) + max(
                0, (end_ms - span_end) // WINDOW_MS
            )
            effective = max(0, (span_end - span_start) // WINDOW_MS)
        else:
            span_start = span_end = None
            outside = expected
            effective = 0
        qualities = _count(window.quality for window in closed)
        channel_gaps = [
            gap
            for gap in gaps
            if gap.venue == item.venue
            and gap.native_market_id == item.native_market_id
            and gap.channel == item.channel
            and gap.start_ms < end_ms
            and (gap.end_ms is None or gap.end_ms > start_ms)
        ]
        channel_conflicts = [
            conflict
            for conflict in conflicts
            if conflict.venue == item.venue
            and conflict.native_market_id == item.native_market_id
            and conflict.channel == item.channel
        ]
        usable = sum(qualities.get(state, 0) for state in USABLE_STATES)
        declared = sum(qualities.get(state, 0) for state in EXCLUDED_STATES)
        unusable = sum(qualities.get(state, 0) for state in UNUSABLE_STATES)
        # Minutes with no window row are either genuinely quiet or unobserved.
        # A live, gap-free channel can vouch for quiet minutes; anything else
        # must be reported as a missing minute, never silently assumed healthy.
        unaccounted = max(0, effective - len(closed) - provisional)
        if item.state in USABLE_STATES:
            quiet = unaccounted
            missing = 0
        else:
            quiet = 0
            missing = unaccounted
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
                closed_windows=len(closed),
                provisional_windows=provisional,
                missing_windows=missing,
                usable_windows=usable + quiet,
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
        "passed": bool(channels) and all(row["passed"] for row in payloads),
        "limits": {
            "completeness_proof": "not claimed",
            "note": (
                "A pass means measured usable minutes met the target; public "
                "trade websockets cannot prove that nothing was missed."
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
        "",
        "| venue | market | channel | effective | outside | closed | open "
        "| quiet | usable | declared | unusable | missing | usable % "
        "| gaps | conflicts | pass |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:"
        "|---|---|---|",
    ]
    for row in report["channels"]:
        pct = "n/a" if row["usable_pct"] is None else f"{row['usable_pct']:.2f}"
        gaps = ", ".join(f"{key}={value}" for key, value in row["gap_states"].items()) or "-"
        lines.append(
            f"| {row['venue']} | {row['native_market_id']} | {row['channel']} "
            f"| {row['effective_minutes']} | {row['outside_coverage_minutes']} "
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
            "Completeness proof is not claimed: `observed_no_detected_gap` only "
            "means no gap was detected on the trade channel.",
        ]
    )
    return "\n".join(lines) + "\n"
