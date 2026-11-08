"""End-to-end liveness of the bot observation pipeline.

Every failure found so far broke the pipeline silently: a one-second clock
skew dropped every L2 book, a too-narrow entry window lost marks, and control
sets stayed empty, all while every service reported "ok".  These checks look
at the journal's output (what the pipeline actually produced), not at the
health of individual components.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import time
from dataclasses import dataclass

LOG = logging.getLogger(__name__)

# Exchange server clocks, tried in order. Binance is geoblocked in some regions
# (451) unless HYPERDATA_EXCHANGE_PROXY is set; OKX is the project's keyless
# fallback elsewhere too.
CLOCK_SOURCES = (
    ("Binance", "https://fapi.binance.com/fapi/v1/time", lambda body: body["serverTime"]),
    ("OKX", "https://www.okx.com/api/v5/public/time", lambda body: int(body["data"][0]["ts"])),
)


@dataclass(frozen=True)
class LivenessPolicy:
    # Observations arrive a few per hour on a normal market; two silent hours
    # after the runtime has been up that long means the pipeline is broken.
    silence_s: float = 2 * 3600
    # no_entry_mark share over recent observations.
    unscored_window_s: float = 6 * 3600
    unscored_min_n: int = 10
    unscored_max_share: float = 0.25
    # Consecutive empty control sets (newest first) that count as broken.
    empty_controls_n: int = 3
    clock_max_offset_s: float = 1.0
    clock_check_s: float = 600.0
    check_s: float = 60.0
    # An issue must be seen on two consecutive checks before it is announced,
    # and is repeated while it lasts.
    confirm_checks: int = 2
    repeat_s: float = 6 * 3600

    @classmethod
    def from_env(cls) -> LivenessPolicy:
        minutes = os.getenv("SCALPER_LIVENESS_SILENCE_MINUTES", "").strip()
        return cls(silence_s=float(minutes) * 60) if minutes else cls()


def evaluate(
    db: sqlite3.Connection,
    now: float,
    started_at: float,
    policy: LivenessPolicy,
    clock_offset_s: float | None = None,
) -> dict[str, str]:
    """Current pipeline issues as ``{issue_id: human message}``; empty when healthy."""
    issues = {}

    if now - started_at >= policy.silence_s:
        last = db.execute(
            "SELECT MAX(decided_at) FROM bot_observations WHERE source='live'"
        ).fetchone()[0]
        if last is None or now - float(last) >= policy.silence_s:
            hours = policy.silence_s / 3600
            since = "ни одного за всё время" if last is None else f"последнее {(now - float(last)) / 3600:.1f} ч назад"
            issues["observations_silent"] = (
                f"Наблюдатели молчат: нет новых наблюдений {hours:g} ч ({since})."
            )

    # Settled entries only: an observation is "unscored/no_entry_mark" at the
    # earliest ~20s after its decision.
    row = db.execute(
        "SELECT COUNT(*), SUM(unscored_reason='no_entry_mark') FROM bot_observations "
        "WHERE source='live' AND decided_at>=? AND decided_at<=? AND decided_at>=?",
        (now - policy.unscored_window_s, now - 60, started_at),
    ).fetchone()
    total, missing = int(row[0] or 0), int(row[1] or 0)
    if total >= policy.unscored_min_n and missing / total > policy.unscored_max_share:
        issues["entry_marks_missing"] = (
            f"Нет цены входа у {missing} из {total} последних наблюдений "
            f"({missing / total:.0%}, порог {policy.unscored_max_share:.0%})."
        )

    recent = db.execute(
        "SELECT c.symbols FROM bot_control_sets c JOIN bot_observations o ON o.control_set_id=c.id "
        "WHERE o.source='live' AND c.created_at>=? ORDER BY c.created_at DESC LIMIT ?",
        (started_at, policy.empty_controls_n),
    ).fetchall()
    if len(recent) >= policy.empty_controls_n and all(r[0] in (None, "", "[]") for r in recent):
        issues["controls_empty"] = (
            f"Пустые контрольные группы у {len(recent)} последних наблюдений подряд."
        )

    if clock_offset_s is not None and abs(clock_offset_s) > policy.clock_max_offset_s:
        side = "отстают" if clock_offset_s > 0 else "спешат"
        issues["clock_skew"] = (
            f"Локальные часы {side} на {abs(clock_offset_s):.2f} с относительно биржи "
            f"(порог {policy.clock_max_offset_s:g} с). Синхронизируйте время Windows."
        )
    return issues


async def measure_clock_offset(session, sources=CLOCK_SOURCES) -> tuple[float, str] | None:
    """Exchange minus local clock, seconds (positive: local clock is behind).

    Half the round trip is the uncertainty; a slow request is discarded rather
    than reported as skew.  Returns ``(offset, source)`` from the first source
    that answers in time.
    """
    for name, url, parse in sources:
        started = time.time()
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                server_ms = parse(await response.json())
        except Exception as exc:
            LOG.debug("Clock offset check via %s failed: %s", name, type(exc).__name__)
            continue
        finished = time.time()
        if finished - started > 1.0 or type(server_ms) is not int:
            continue
        return server_ms / 1000 - (started + finished) / 2, name
    return None


class PipelineLiveness:
    """Periodic checks with debounced Telegram alerts and recovery notices."""

    def __init__(self, db, alerts=None, *, started_at: float | None = None, policy: LivenessPolicy | None = None):
        self.db = db
        self.alerts = alerts
        self.policy = policy or LivenessPolicy.from_env()
        self.started_at = time.time() if started_at is None else started_at
        self.clock_offset_s: float | None = None
        self.clock_source: str | None = None
        self.clock_checked_at: float | None = None
        self.last_check_at: float | None = None
        self.last_error: str | None = None
        self.pending: dict[str, int] = {}
        self.active: dict[str, dict] = {}

    async def step(self, now: float, clock_offset_s: float | None = None) -> list[str]:
        """One check; returns the messages it sent (or would send without alerts)."""
        if clock_offset_s is not None:
            self.clock_offset_s, self.clock_checked_at = clock_offset_s, now
        offset = self.clock_offset_s
        if self.clock_checked_at is not None and now - self.clock_checked_at > 3 * self.policy.clock_check_s:
            offset = None  # A stale measurement neither raises nor keeps an alarm.
        issues = evaluate(self.db, now, self.started_at, self.policy, offset)
        self.last_check_at = now
        messages = []
        for issue_id, text in issues.items():
            seen = self.pending.get(issue_id, 0) + 1
            self.pending[issue_id] = seen
            state = self.active.get(issue_id)
            if state is None and seen >= self.policy.confirm_checks:
                self.active[issue_id] = dict(text=text, since=now, notified_at=now)
                messages.append("⚠️ Конвейер наблюдений: " + text)
            elif state is not None:
                state["text"] = text
                if now - state["notified_at"] >= self.policy.repeat_s:
                    state["notified_at"] = now
                    messages.append("⚠️ Всё ещё: " + text)
        for issue_id in list(self.pending):
            if issue_id not in issues:
                self.pending.pop(issue_id)
                state = self.active.pop(issue_id, None)
                if state is not None:
                    minutes = (now - state["since"]) / 60
                    messages.append(f"✅ Конвейер наблюдений восстановлен: {issue_id} ({minutes:.0f} мин).")
        for message in messages:
            LOG.warning("%s", message)
            if self.alerts is not None:
                try:
                    await self.alerts.send_telegram(message)
                except Exception as exc:
                    LOG.warning("Liveness alert delivery failed: %s", type(exc).__name__)
        return messages

    async def run(self, session_factory=None):
        import aiohttp

        from hyperdata_terminal.network import exchange_session

        factory = session_factory or (lambda: exchange_session(timeout=aiohttp.ClientTimeout(total=5)))
        next_clock_at = 0.0
        async with factory() as session:
            while True:
                offset = None
                if time.time() >= next_clock_at:
                    measured = await measure_clock_offset(session)
                    if measured is not None:
                        offset, self.clock_source = measured
                    # A failed measurement is retried on the next check.
                    next_clock_at = time.time() + (self.policy.clock_check_s if measured is not None else 0)
                try:
                    await self.step(time.time(), offset)
                    self.last_error = None
                except Exception as exc:
                    self.last_error = f"{type(exc).__name__}: {str(exc)[:120]}"
                    LOG.exception("Pipeline liveness check failed")
                await asyncio.sleep(self.policy.check_s)

    def report(self) -> dict:
        return dict(
            healthy=not self.active,
            active={k: v["text"] for k, v in self.active.items()},
            pending=sorted(k for k in self.pending if k not in self.active),
            last_check_at=self.last_check_at,
            clock_offset_s=None if self.clock_offset_s is None else round(self.clock_offset_s, 3),
            clock_checked_at=self.clock_checked_at,
            clock_source=self.clock_source,
            error=self.last_error,
        )
