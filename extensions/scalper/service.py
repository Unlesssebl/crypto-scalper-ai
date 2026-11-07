"""Schedule significant native signals, independent analysis and notifications."""

import asyncio
import hashlib
import html
import logging
import math
import os
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

from hyperdata_terminal.runtime_mode import (
    LEGACY,
    RuntimeModeError,
    compatibility_error,
    runtime_mode,
)
from hyperdata_terminal.utils.helpers import format_price, format_usd

from .activity import (
    CANDLE_SOURCE,
    active_anomalies,
    btc_context,
    finding_strength,
    market_risk_context,
    refresh_candles,
    wide_spread,
)
from .analysis import DIRECTIONAL, compare, snapshot_key
from .audit import CONTROL_INTERVAL, STRONG_RANK
from .consistency_store import ConsistencyStore
from .context_auto import REASONS as CONTEXT_REASONS
from .context_auto import ContextMemory, decide, enrich
from .fact_packet import build_packet, packet_ready
from .fact_review import FactReviewer, validate_review
from .journal import dump
from .market import WINDOWS, capture, metric
from .opportunity import MAX_AGE, REASON_LABELS, OpportunityPolicy
from .phase import PhaseTracker
from .profile import DAY, calculate_profiles, fresh, profile_context, sample_window, situation_facts
from .pulse import pulse_text
from .quality import calculate_quality
from .shadow import ShadowDetector
from .unified_attention import UnifiedAttention as Attention

LOG = logging.getLogger(__name__)
AI_INTERVAL = 12
ALERT_COOLDOWN = 300
MAX_EVENT_AGE = MAX_AGE
QUEUE_LIMIT = 20
PRIORITY_STEP_SECONDS = 15
STRENGTH_STEP_SECONDS = 10
CONTEXT_WINDOW_SECONDS = 900
CONTEXT_TRIAL_INTERVAL = 10
ALERT_DIRECTIONS = {
    "BULLISH": "🟢 вверх",
    "BEARISH": "🔴 вниз",
    "NEUTRAL": "⚪ нейтрально",
    "NOT_AVAILABLE": "оценка недоступна",
    "INSUFFICIENT_DATA": "недостаточно данных",
}
MOSCOW = timezone(timedelta(hours=3), "МСК")


def alert_min_rank() -> int:
    """Minimum finding strength (rank 1-4) that triggers a Telegram alert.

    Defaults to STRONG_RANK so parity-era behaviour is unchanged; an operator
    can lower it (e.g. 2) via HYPERDATA_ALERT_MIN_RANK to receive more
    notifications.
    """
    try:
        return int(os.getenv("HYPERDATA_ALERT_MIN_RANK", STRONG_RANK))
    except ValueError:
        return STRONG_RANK


def futures_ticker(snapshot):
    return f"HYPERLIQUID:{snapshot['symbol'].upper()}USDC.P"


def alert_market_facts(snapshot):
    metrics = snapshot["metrics"]
    facts = []
    start, end = (fresh(metrics, key) for key in ("first_trade_price", "last_trade_price"))
    if start and end is not None:
        change = 100 * (end / start - 1)
        if abs(change) < 0.005:
            change = 0.0
        facts.append(("Цена", f"<b>{change:+.2f}%</b> за наблюдаемое окно"))
    count, pace, rate = (fresh(metrics, key) for key in ("trade_count", "activity_ratio", "trade_rate"))
    activity = []
    if count is not None:
        amount = int(count)
        word = (
            "сделок"
            if 11 <= amount % 100 <= 14
            else ("сделка" if amount % 10 == 1 else "сделки" if amount % 10 in (2, 3, 4) else "сделок")
        )
        activity.append(f"{amount} {word}")
    if rate is not None:
        activity.append(f"{rate:.1f}/с")
    if pace is not None:
        baseline = snapshot.get("activity_baseline") or {}
        description = "к локальной базе; состав не сохранён"
        if baseline.get("kind") == "median_trade_count":
            description = (
                f"к медиане {int(baseline['samples'])} закрытых свечей по {int(baseline['window_seconds']) // 60} мин"
            )
        activity.append(f"<b>{pace:.1f}×</b> {description}")
    if activity:
        facts.append(("Активность", " · ".join(activity)))

    volume, cvd, ofi = (fresh(metrics, key) for key in ("volume", "cvd", "ofi"))
    flow = []
    if ofi is not None and -1 <= ofi <= 1:
        buy = (1 + ofi) * 50
        flow.append(f"покупки <b>{buy:.0f}%</b> / продажи <b>{100 - buy:.0f}%</b> объёма")
    if cvd is not None:
        flow.append(f"дельта {'+' if cvd > 0 else ''}{format_usd(cvd)}")
    if volume is not None:
        flow.append(f"оборот {format_usd(volume)}")
    if flow:
        facts.append(("Лента HL", " · ".join(flow)))
    spread, price, imbalance = (fresh(metrics, key) for key in ("spread", "price", "imbalance"))
    book = []
    if imbalance is not None:
        book.append(f"перевес {'bid' if imbalance >= 0 else 'ask'} {abs(imbalance):.0%} по видимой глубине")
    if price and spread is not None:
        book.append(f"спред {100 * spread / price:.3f}%")
    if book:
        facts.append(("Стакан", " · ".join(book)))
    if context := snapshot.get("market_pulse"):
        label = "Исходные межбиржевые данные" if snapshot.get("market_pulse_corrections") else "Межбиржевые данные"
        facts.append((label, html.escape(pulse_text(context))))
    corrections = snapshot.get("market_pulse_corrections", {}).values()
    unique = {(c["venue"], c["window_start_ms"]): c for c in corrections}
    for correction in unique.values():
        minute = datetime.fromtimestamp(correction["window_start_ms"] / 1000, timezone(timedelta(hours=3))).strftime(
            "%H:%M МСК"
        )
        volume = ""
        if not correction.get("normalization_blocked") and any(
            correction.get(side) is not None for side in ("buy_quote", "sell_quote")
        ):
            try:
                buy = Decimal(correction.get("buy_quote") or "0")
                sell = Decimal(correction.get("sell_quote") or "0")
                if buy.is_finite() and sell.is_finite() and buy >= 0 and sell >= 0:
                    currency = "USDC" if correction["venue"] == "hyperliquid" else "USDT"
                    volume = f"; оборот {buy + sell:,.0f} {currency}"
            except (InvalidOperation, TypeError):
                pass
        text = (
            f"{correction['venue']}, {minute}: версия {correction['version']}, "
            f"{correction['execution_count']} исполнений{volume}; качество: {correction['quality']}. "
            "Позднее исправление; нового торгового события нет"
        )
        facts.append(("Исправление данных", html.escape(text)))
    quality = []
    for name, label in (("ofi", "Лента"), ("spread", "Стакан")):
        state = metrics.get(name, {}).get("quality", "unavailable")
        if state != "ok":
            state = {
                "warming_up": "прогрев",
                "stale": "данные устарели",
                "unavailable": "нет данных",
                "clock_skew": "расхождение часов",
                "synthetic": "демо",
            }.get(state, "неполные данные")
            quality.append(f"{label}: {state}")
    if pace is not None:
        quality.append("кратность к локальной базе; статистическая редкость не оценена")
    if quality:
        facts.append(("Качество", "; ".join(quality)))
    return facts


def alert_text(snapshot, native, ai, event, *, rich=False, updates=()):
    stamp = datetime.fromtimestamp(snapshot["captured_at"], MOSCOW).strftime("%H:%M:%S %Z")
    labels = {"tape_burst": "Ускорение ленты", "range_burst": "Расширение диапазона"}
    reason = " + ".join(labels[name] for name in event.get("anomalies", ()) if name in labels)
    ticker = futures_ticker(snapshot)
    market_url = f"https://app.hyperliquid.xyz/trade/{quote(snapshot['symbol'], safe='')}"
    headline = f'<b><a href="{market_url}">{html.escape(ticker)}</a></b> · {html.escape(snapshot["window"])}'
    price = fresh(snapshot["metrics"], "price")
    if price is not None:
        headline += f" · Mark {format_price(price)}"
    strength = event.get("strength")
    trigger = f"<b>Событие</b> · {reason or 'Смена стороны потока'}"
    if strength:
        trigger += (
            f" · {html.escape(strength['icon'])} "
            f"<b>Выраженность: {html.escape(strength['label'])}</b> — оценка скринера"
        )
    phase = event.get("movement_phase")
    if phase and phase["phase"] not in {"unavailable", "untracked"}:
        trigger += (
            f"<br><b>Фаза:</b> {html.escape(phase['label'])}"
            if rich
            else f"\n<b>Фаза:</b> {html.escape(phase['label'])}"
        )
        if phase["distance_natr"] is not None:
            trigger += f" · {phase['distance_natr']:+.1f} NATR от начала"
        trigger += f" · {phase['age_seconds']} с"
    characterization = hypothesis = None
    ai_direction = ALERT_DIRECTIONS["NOT_AVAILABLE"]
    if ai["status"] == "ok":
        ai_direction = ALERT_DIRECTIONS.get(ai.get("direction"), ai_direction)
        characterization = ai.get("summary")
        hypothesis = ai.get("scenario")
    if ai["status"] == "pending":
        ai_direction = "анализ выполняется"
    elif ai["status"] == "queued":
        ai_direction = "в очереди; запрос ещё не отправлен"
    elif ai["status"] != "ok":
        reason = ai.get("reason", ai["status"])
        labels = REASON_LABELS
        ai_direction = "пропущен — " + labels.get(reason, reason)
        if ai["status"] not in {"skipped", "unconfigured", "queued"}:
            ai_direction = "ошибка — " + labels.get(reason, reason)
    native_direction = {
        "BULLISH": "🟢 покупки",
        "BEARISH": "🔴 продажи",
        "NEUTRAL": "баланс",
        "NOT_AVAILABLE": "нет пригодных данных",
    }.get(native["direction"], "нет пригодных данных")
    header = [headline, f"<i>Бессрочный фьючерс · {stamp} · скользящее окно {html.escape(snapshot['window'])}</i>"]
    facts = alert_market_facts(snapshot)
    if btc := snapshot.get("btc_context"):
        facts.append(
            (
                "Фон BTC",
                f"за 5 мин: BTC {btc['btc_last_return_pct']:+.2f}% / "
                f"актив {btc['asset_last_return_pct']:+.2f}% · корреляция "
                f"{btc['correlation']:+.2f} ({btc.get('samples', 36)} окон по 5 мин)",
            )
        )
    order_flow = f"<b>Поток HL</b> {native_direction} · <b>AI</b> {html.escape(ai_direction)}"
    if ai["status"] != "ok":
        order_flow += "\n<i>Наблюдение: гипотеза входа не сформирована.</i>"
    updates_text = []
    for update in updates:
        window = html.escape(update["window"])
        direction = ALERT_DIRECTIONS[update["hyperdata"]["direction"]]
        strength = html.escape(update["event"]["strength"]["label"])
        captured = datetime.fromtimestamp(update["captured_at"], MOSCOW).strftime("%H:%M:%S %Z")
        updates_text.append(f"<b>Обновление {window}</b> · {direction} · {strength} · {captured}")
    if rich:
        lines = [f"<p>{'<br>'.join(header)}</p>", "<hr/>", f"<p>{trigger}</p>"]
        if facts:
            table = "".join(f"<tr><th>{label}</th><td>{value}</td></tr>" for label, value in facts)
            lines.append(f"<table striped compact>{table}</table>")
        else:
            lines.append("<p>Свежие метрики недоступны.</p>")
        lines.append("<p>" + order_flow.replace("\n", "<br/>") + "</p>")
        lines.extend(f"<p>{text}</p>" for text in updates_text)
        if characterization:
            lines.append(f"<p><b>🧠 Характеристика AI</b><br>{html.escape(characterization)}</p>")
        if hypothesis:
            lines.append(f"<blockquote><b>Гипотеза AI</b><br>{html.escape(hypothesis)}</blockquote>")
        lines.append(
            f'<tg-button-row align="left"><tg-button type="copy_text" text="{html.escape(ticker)}">'
            "Копировать фьючерс</tg-button></tg-button-row>"
        )
        return "\n".join(lines)
    market = [trigger, *(f"<b>{label}</b> · {value}" for label, value in facts)]
    if not facts:
        market.append("Свежие метрики недоступны.")
    message = "\n\n".join(("\n".join(header), "\n".join(market), order_flow, *updates_text))
    if characterization:
        message += f"\n\n<b>🧠 Характеристика AI</b>\n{html.escape(characterization)}"
    if hypothesis:
        message += f"\n\n<blockquote><b>Гипотеза AI</b>\n{html.escape(hypothesis)}</blockquote>"
    return message


def minute_gate(snapshot):
    if snapshot["window"] != "1m":
        return "ready"
    metrics = snapshot["metrics"]
    if metrics["ofi"]["quality"] != "ok":
        return "tape_unavailable"
    if metrics["spread"]["quality"] != "ok":
        return "book_unavailable"
    return "wide_spread" if wide_spread(snapshot) else "ready"


def five_minute_relation(native, context):
    if not context:
        return "unavailable"
    other = context["hyperdata"]["direction"]
    if other == "NOT_AVAILABLE":
        return "unavailable"
    direction = native["direction"]
    if direction in DIRECTIONAL and other in DIRECTIONAL:
        return "aligned" if direction == other else "opposed"
    return "neutral"


def with_context(snapshot, context):
    window = "5m" if snapshot["window"] == "1m" else "1m"
    context_key = f"context_{window}"
    if context_key in snapshot:
        return snapshot
    other = context["snapshot"] if context else None
    if (
        not other
        or other["window"] != window
        or other["symbol"] != snapshot["symbol"]
        or other["captured_at"] != snapshot["captured_at"]
    ):
        if window == "1m":
            return snapshot
        return {
            **snapshot,
            context_key: {
                "window": window,
                "captured_at": None,
                "metrics": {},
                "flow_quality": {"ofi": "unavailable", "cvd": "unavailable"},
            },
        }
    keys = (
        "ofi",
        "cvd",
        "trade_count",
        "activity_ratio",
        "natr",
        "range_ratio",
        "first_trade_price",
        "last_trade_price",
        "spread",
        "imbalance",
    )
    metrics = {name: other["metrics"][name] for name in keys if fresh(other["metrics"], name) is not None}
    return {
        **snapshot,
        context_key: {
            "window": window,
            "captured_at": other["captured_at"],
            "metrics": metrics,
            "flow_quality": {name: other["metrics"][name]["quality"] for name in ("ofi", "cvd")},
        },
    }


def with_movement_context(snapshot, phase):
    if not phase:
        return snapshot
    if "started_at" not in phase:
        return {**snapshot, "movement_context": {"status": phase["phase"]}}
    fields = ("age_seconds", "distance_natr", "retracement_natr", "natr_at_start")
    return {
        **snapshot,
        "movement_context": {
            "status": "observed",
            **{field: phase.get(field) for field in fields},
        },
    }


def with_senior_context(snapshot, candles):
    if not candles:
        return snapshot
    closed_at = candles["closed_at"]
    if not 0 <= snapshot["captured_at"] - closed_at <= CONTEXT_WINDOW_SECONDS + 90:
        return snapshot
    units = {"return_4": "%", "range_4": "%", "range_position_4": "ratio", "natr": "%"}
    metrics = {
        name: metric(candles[name], CANDLE_SOURCE, closed_at, unit)
        for name, unit in units.items()
        if candles[name] is not None
    }
    return {**snapshot, "context_15m": {"window": "15m", "captured_at": closed_at, "metrics": metrics}}


def with_market_context(snapshot, hub):
    """Attach the market-wide context (L/S, basis, IV, liquidations, whales) if
    the hub exposes it. `RemoteHub` carries ``/v1/snapshot``'s ``context`` block
    directly; a hub without one is a no-op."""
    context = getattr(hub, "context", None)
    if not context:
        return snapshot
    return {**snapshot, "market_context": context}


class Screener:
    def __init__(self, hub, journal, gemini, notify, recorder=None, pulse=None):
        self.hub, self.journal, self.gemini, self.notify = hub, journal, gemini, notify
        self.recorder = recorder
        self.pulse = pulse
        # The mode is decided by the process environment, never by the
        # currently reachable native API. A degraded native feed must not
        # reopen the legacy directional path.
        self.runtime_mode = runtime_mode()
        self.legacy_ai_enabled = self.runtime_mode == LEGACY
        self.attention = Attention(
            journal if hasattr(journal, "db") else None,
            slots=int(os.getenv("SCALPER_ATTENTION_SLOTS", "20")),
            detail_slots=int(os.getenv("PULSE_ATTENTION_DETAIL_SLOTS", "6")),
            hold_seconds=int(os.getenv("PULSE_ATTENTION_HOLD_SECONDS", "300")),
        )
        if hub is not None:
            hub.attention_selector = self.attention
        saved_policy = journal.load_policy() if journal is not None and hasattr(journal, "load_policy") else None
        self.policy = OpportunityPolicy(**saved_policy) if saved_policy else OpportunityPolicy.from_env()
        self.opportunities = {}
        self.rows = {}
        self.quality_report = None
        self.profiles = {}
        self.phases = PhaseTracker()
        self.shadow = ShadowDetector()
        self.sample_slots = {}
        self.control_slots = {}
        self.last_profile_prune = 0
        self.candles = {}
        self.market_context: list[str] = []
        self.context_events = 0
        self.states = {}
        self.pending = set()
        self.candidate_seen = {}
        self.sequence = 0
        self.jobs = asyncio.PriorityQueue(maxsize=QUEUE_LIMIT)
        self.notifications = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self.consistency = ConsistencyStore(journal.db) if journal is not None and hasattr(journal, "db") else None
        self.context_packets = {}
        self.context_cache = OrderedDict()
        self.context_lock = asyncio.Lock()
        self.context_memory = ContextMemory(journal.db) if self.consistency else None
        self.context_auto_enabled = os.getenv("SCALPER_CONTEXT_AUTO", "true").lower() == "true"
        self.context_auto_error = None
        self.outbox = None
        self.ai_routes = None
        self.telegram_ux = None
        if os.getenv("PULSE_UNIFIED", "0") == "1" and self.consistency:
            from dataclasses import replace

            from .ai_routes import AIRoutes

            self.ai_routes = AIRoutes(journal, gemini)
            from .delivery import CardOutbox

            self.outbox = CardOutbox(journal, hub.alerts)
            self.policy = replace(self.policy, hourly_limit=40, experiments=False)
            if os.getenv("SCALPER_TELEGRAM_UX", "0") == "1":
                from .telegram_ux import TelegramUX

                self.telegram_ux = TelegramUX(self)

    def runtime_diagnostic(self):
        return compatibility_error(self.runtime_mode, getattr(self.hub, "attention", {}))

    def validate_runtime(self):
        """Fail startup explicitly instead of silently running legacy AI."""
        error = self.runtime_diagnostic()
        if error:
            raise RuntimeModeError(error)

    def recover_pending(self):
        """Close persisted legacy work without re-queueing it in unified mode."""
        if self.journal is None:
            return []
        interrupted = self.journal.recover()
        if self.legacy_ai_enabled:
            for key in interrupted:
                self.queue_hypothesis(key)
        else:
            self.journal.close_legacy_notifications()
        return interrupted

    def scan(self, now):
        if self.pulse is not None and hasattr(self.pulse, "desired_plan"):
            self.pulse.desired_plan = self.attention.plan
            self.attention.activity = {r["symbol"]: r for r in getattr(self.pulse, "overview", {}).get("items", [])}
        if self.notify and self.legacy_ai_enabled:
            for event_id in self.journal.pending_pulse_edits():
                if self.notifications.full():
                    break
                self.notifications.put_nowait(("pulse_edit", event_id))
                self.journal.pulse_edit_status(event_id, "queued")
        assets = self.hub.get_all_assets()
        self.market_context = market_risk_context(getattr(self.hub, "context", None))
        rows = {}
        from .activity import observation_assets

        for asset in observation_assets(self.hub):
            for window in WINDOWS:
                key = (asset.symbol, window)
                snapshot, native = capture(self.hub, asset, window, now, self.candles.get(key))
                if self.pulse is not None:
                    snapshot["market_pulse"] = self.pulse.facts(asset.symbol, now)
                native["snapshot"] = snapshot_key(snapshot)
                rows[key] = {"snapshot": snapshot, "hyperdata": native}
        samples, controls, shadow_findings = [], [], []
        control_slot = int(now // CONTROL_INTERVAL) * CONTROL_INTERVAL
        for key, row in rows.items():
            snapshot, native = row["snapshot"], row["hyperdata"]
            context = rows.get((key[0], "5m")) if key[1] == "1m" else None
            profile = self.profiles.get(key)
            row["facts"] = situation_facts(snapshot, profile)
            if self.market_context:
                row["facts"] = [*row["facts"], *self.market_context]
            row["strength"] = finding_strength(snapshot, native)
            row["movement_phase"] = self.phases.observe(key, snapshot, native, WINDOWS[key[1]])
            row["btc_context"] = btc_context(self.candles.get((key[0], "5m")), self.candles.get(("BTC", "5m")), now)
            row["profile"] = profile
            row["analysis_gate"] = minute_gate(snapshot)
            row["context_5m"] = five_minute_relation(native, context)
            if not self.hub.demo:
                shadow_findings.extend(self.shadow.evaluate(key, snapshot, native))
                other = rows.get((key[0], "5m" if key[1] == "1m" else "1m"))
                if self.legacy_ai_enabled:
                    self.detect(
                        key, snapshot, native, now, row["analysis_gate"], row["context_5m"], {**row, "other": other}
                    )
                natr = fresh(snapshot["metrics"], "natr")
                if (
                    now - control_slot <= 10
                    and self.control_slots.get(key) != control_slot
                    and row["strength"]["rank"] == 1
                    and key not in self.pending
                    and row["analysis_gate"] == "ready"
                    and fresh(snapshot["metrics"], "spread") is not None
                    and not wide_spread(snapshot)
                    and natr is not None
                    and natr > 0
                ):
                    controls.append((*key, control_slot, now, natr))
                sample = sample_window(snapshot, WINDOWS[key[1]])
                if sample and self.sample_slots.get(key) != sample[2]:
                    samples.append(sample)
        if shadow_findings:
            self.journal.record_shadow_findings(shadow_findings)
        self.shadow.keep(rows)
        if samples:
            self.journal.record_instrument_samples(samples)
            self.sample_slots.update({(sample[0], sample[1]): sample[2] for sample in samples})
        if controls:
            self.journal.record_quiet_moves(controls)
            self.control_slots.update({(sample[0], sample[1]): sample[2] for sample in controls})
        self.sample_slots = {key: slot for key, slot in self.sample_slots.items() if key in rows}
        self.control_slots = {key: slot for key, slot in self.control_slots.items() if key in rows}
        self.phases.keep(rows)
        self.rows = rows
        if self.recorder is not None:
            self.recorder.record(now, rows)
        self.states = {key: value for key, value in self.states.items() if key in rows}
        self.opportunities = {key: value for key, value in self.opportunities.items() if key in rows}
        self.candidate_seen = {key: value for key, value in self.candidate_seen.items() if key in rows}

        prices = {}
        market_at = self.hub.market_timestamp()
        if not self.hub.demo and market_at is not None and self.hub.market_age() <= 20:
            prices = {
                asset.symbol: (asset.mark_price, market_at)
                for asset in assets
                if "price" in self.hub.market.observed_fields.get(asset.symbol, set())
                and math.isfinite(asset.mark_price)
                and asset.mark_price > 0
            }
        self.journal.observe(now, prices)
        if getattr(self.hub, "attention", {}).get("enabled"):
            scout_prices = {
                s: (p, at)
                for s, (p, at) in getattr(self.hub, "scout_prices", {}).items()
                if isinstance(p, (float, int))
                and math.isfinite(p)
                and p > 0
                and isinstance(at, (float, int))
                and 0 <= now - at <= 20
            }
            self.journal.observe(now, scout_prices)
        self.refresh_context_packets(now)
        if self.context_memory:
            for packet in self.context_packets.values():
                self.context_memory.observe(packet)
        if self.telegram_ux and self.notify:
            self.telegram_ux.observe(now)

    def refresh_context_packets(self, now):
        attention = getattr(self.hub, "attention", {})
        if not attention.get("context_only"):
            self.context_packets = {}
            return
        packets = {}
        for assignment in attention.get("active", []):
            packet = self.build_context_packet(attention, assignment, now)
            packets[packet["symbol"]] = packet
            self.context_cache[packet["packet_id"]] = packet
            if self.consistency:
                self.consistency.observe_episode(packet)
        self.context_packets = packets
        while len(self.context_cache) > 64:
            self.context_cache.popitem(last=False)

    def build_context_packet(self, attention, assignment, now):
        from .analytics_context import extend_packet

        packet = build_packet(attention, assignment, now, frames=[r["snapshot"] for r in self.rows.values()])
        return extend_packet(packet, self)

    def context_state(self, now):
        if not getattr(self.hub, "attention", {}).get("context_only"):
            return {"enabled": False}
        self.refresh_context_packets(now)
        meta = self.consistency.latest_episode("market.meta.v1") if self.consistency else None
        return dict(
            meta=dict(review=meta, packet=self.consistency.packet(meta["packet_id"])) if meta else None,
            enabled=True,
            configured=bool(self.gemini.key),
            automatic=dict(
                enabled=self.context_auto_enabled,
                last_error=self.context_auto_error,
                reason_labels=CONTEXT_REASONS,
                **(self.context_memory.report(now) if self.context_memory else {}),
            ),
            routes=self.ai_routes.summary(now) if self.ai_routes else None,
            telegram_ux=self.telegram_ux.status() if self.telegram_ux else dict(enabled=False),
            notification_quality=self.telegram_ux.quality.report(now) if self.telegram_ux else None,
            delivery=self.outbox.summary() if self.outbox else None,
            packets=[
                dict(
                    packet=p,
                    selection=self.context_memory.latest(p["episode_id"]) if self.context_memory else None,
                    latest=self.consistency.latest_episode(p["episode_id"]) if self.consistency else None,
                )
                for p in self.context_packets.values()
            ],
            report=self.consistency.report(now) if self.consistency else None,
        )

    async def review_context(
        self, symbol, packet_id=None, *, origin="manual", trigger_reason=None, auto_state=None, route="strong"
    ):
        """Explicit manual request; factual review shares the global request budget."""
        if route == "free" and not self.ai_routes:
            return dict(status="unavailable", reason="free_route_disabled")
        async with self.context_lock:
            now = time.time()
            if not getattr(self.hub, "attention", {}).get("context_only") or not self.consistency:
                return {"status": "unavailable", "reason": "context_mode_required"}
            if packet_id is None:
                self.refresh_context_packets(now)
                packet = next((p for s, p in self.context_packets.items() if s.casefold() == symbol.casefold()), None)
            else:
                packet = self.context_cache.get(packet_id) or self.consistency.packet(packet_id)
            active = {r["symbol"]: r for r in self.hub.attention.get("active", [])}
            if (
                packet is None
                or packet["symbol"].casefold() != symbol.casefold()
                or packet["symbol"] not in active
                or not packet_ready(packet, now)
            ):
                return {"status": "skipped", "reason": "stale_or_unknown_packet"}
            current = self.build_context_packet(self.hub.attention, active[packet["symbol"]], now)
            if current["episode_id"] != packet["episode_id"] or not packet_ready(current, now):
                return {"status": "skipped", "reason": "episode_changed_or_unavailable"}
            prior = self.consistency.latest(packet["packet_id"])
            if prior and prior["status"] == "ok" and (not self.ai_routes or prior.get("route") == route):
                return dict(status="ok", cached=True, packet=packet, review=prior)
            client = self.ai_routes.free if self.ai_routes and route == "free" else self.gemini
            if self.ai_routes and route == "free" and not await self.ai_routes.check_free(now):
                return dict(status="skipped", reason=self.ai_routes.catalog_error)
            key = self.consistency.begin(
                packet,
                now,
                client.provider,
                getattr(client, "model", None),
                origin=origin,
                trigger_reason=trigger_reason,
                route=route,
            )
            memory = self.context_memory.observe(packet)
            if auto_state is None:
                _, _, auto_state = decide(packet, memory["samples"])
            auto_state = {**auto_state, "_reason": trigger_reason}
            route_job = None
            if not client.key or self.hub.demo:
                status = "unconfigured" if not client.key else "skipped"
                review = self.consistency.finish(key, status, now, reason="no_api_key" if not client.key else "demo")
                return dict(status=status, packet=packet, review=review)
            if self.ai_routes:
                route_job, reason = self.ai_routes.reserve(packet, route, auto_state, now, self.policy.hourly_limit)
            else:
                reason = self.journal.reserve_request(key, packet["symbol"], now, self.policy)
            if reason:
                review = self.consistency.finish(key, "skipped", now, reason=reason)
                return dict(status="skipped", packet=packet, review=review)
            if not self.ai_routes:
                with self.journal.db:
                    self.journal.db.execute(
                        "UPDATE ai_requests SET purpose=? WHERE candidate_id=?",
                        ("context_auto" if origin == "automatic" else "context", key),
                    )
            self.context_memory.sent(packet, auto_state, now)
            try:
                response = await FactReviewer(client).review(packet)
                status, checked, reason = response["status"], None, response.get("reason")
                if self.ai_routes and route == "free" and (response.get("usage", {}).get("cost") or 0) > 0:
                    self.ai_routes.free_available = False
                    self.ai_routes.catalog_until = time.time() + 3600
                    self.ai_routes.catalog_error = "unexpected_nonzero_cost"
                    response["retry_after"] = 3600
                    status, reason = "billing_mismatch", "free_provider_reported_cost"

                if status == "ok":
                    try:
                        checked = validate_review(response["raw"], packet, time.time())
                    except (ValueError, TypeError, KeyError) as exc:
                        status, reason = "invalid_response", str(exc)
                review = self.consistency.finish(
                    key,
                    status,
                    time.time(),
                    result=checked,
                    raw=response.get("raw"),
                    reason=reason,
                    model=response.get("model"),
                    usage=response.get("usage"),
                )
                if self.ai_routes:
                    self.ai_routes.finish(route_job, status, key, response.get("retry_after"))
                self.context_memory.completed(packet, auto_state, key, status == "ok")
                return dict(status=status, packet=packet, review=review)
            except asyncio.CancelledError:
                self.consistency.finish(key, "interrupted", time.time(), reason="cancelled")
                raise
            except Exception as exc:
                LOG.exception("Context review failed")
                review = self.consistency.finish(key, "error", time.time(), reason=type(exc).__name__)
                return dict(status="error", packet=packet, review=review)

    async def auto_context_once(self):
        if not self.context_memory or self.hub.demo or not getattr(self.hub, "attention", {}).get("context_only"):
            return []
        now = time.time()
        self.refresh_context_packets(now)
        candidates, outcomes = [], []
        for packet in self.context_packets.values():
            memory = self.context_memory.observe(packet)
            rank = max(
                (r["strength"]["rank"] for (symbol, _), r in self.rows.items() if symbol == packet["symbol"]), default=0
            )
            route = "strong" if rank >= 3 or not self.ai_routes else "free"
            baseline = self.ai_routes.last_sent(packet["episode_id"], route) if self.ai_routes else memory["last_sent"]
            priority, reason, state = decide(
                packet,
                memory["samples"],
                baseline,
                economical=False if self.ai_routes else self.policy.mode == "economical" or not self.policy.early,
            )
            state["_route"] = route
            if priority is None:
                self.context_memory.record(packet, reason, "rejected", state)
                continue
            block = (
                "disabled"
                if not self.context_auto_enabled
                else "no_api_key"
                if not (self.ai_routes.free.key if self.ai_routes and route == "free" else self.gemini.key)
                else None
                if self.ai_routes
                else self.journal.request_block(packet["symbol"], now, self.policy)
            )
            if block:
                self.context_memory.record(packet, block, "skipped", state)
                continue
            candidates.append((-priority, packet["symbol"], packet, memory, reason, state))
        # One fresh dispatch per pass. Other candidates are re-evaluated from live context next pass.
        if self.ai_routes and not any(c[-1]["_route"] == "strong" for c in candidates):
            meta = await self.meta_context_once()
            if meta:
                return [meta]
        if candidates:
            _, symbol, packet, memory, reason, state = min(
                candidates, key=lambda c: (c[-1].get("_route") != "strong" if self.ai_routes else False, *c[:2])
            )
            if not packet_ready(packet, time.time()):
                self.context_memory.record(packet, "stale_or_unknown_packet", "skipped", state)
                return []
            selected = enrich(packet, memory["samples"], reason)
            self.context_cache[selected["packet_id"]] = selected
            self.context_memory.record(selected, reason, "queued", state)
            if self.outbox and self.notify and not self.telegram_ux:
                self.queue_context_card(dict(status="pending", packet=selected))
            result = await self.review_context(
                symbol,
                selected["packet_id"],
                origin="automatic",
                trigger_reason=reason,
                auto_state=state,
                route=state.get("_route", "strong"),
            )
            self.context_memory.record(
                selected, reason, result["status"], state, (result.get("review") or {}).get("id")
            )
            outcomes.append(result)
            if self.outbox and self.notify and result.get("packet"):
                self.queue_context_card(result)
            # Telegram is a separate decision: only a checked, current, non-abstaining contradiction/change.
            checked = (result.get("review") or {}).get("result") or {}
            if (
                not self.outbox
                and self.notify
                and result["status"] == "ok"
                and reason in {"flow_price_conflict", "density_changed"}
                and not checked.get("abstention")
                and not checked.get("expired_at_completion", True)
                and any(
                    c["section"] == "interpretation" and c["status"] == "hypothesis" for c in checked.get("claims", [])
                )
            ):
                from .telegram import context_review_text

                try:
                    await self.hub.alerts.send_telegram(
                        context_review_text(result), rich_html=context_review_text(result, rich=True)
                    )
                except Exception:
                    LOG.exception("Context review Telegram delivery failed")
        return outcomes

    def queue_context_card(self, result):
        if self.telegram_ux:
            return self.telegram_ux.publish_analysis(result)
        from .telegram import context_review_text

        packet = result["packet"]
        self.outbox.enqueue(
            packet["episode_id"],
            context_review_text(result),
            context_review_text(result, rich=True),
            packet["captured_at"] + 120,
        )

    async def meta_context_once(self):
        from .meta_review import run_meta

        async with self.context_lock:
            result = await run_meta(self)
            if result and self.notify and result.get("packet"):
                self.queue_context_card(result)
            return result

    async def auto_context(self):
        while True:
            try:
                await self.auto_context_once()
                self.context_auto_error = None
            except Exception as exc:
                self.context_auto_error = type(exc).__name__
                LOG.exception("Automatic context review failed")
            await asyncio.sleep(10)

    def detect(self, key, snapshot, native, now, gate, relation, row=None):
        if not self.legacy_ai_enabled:
            return
        direction = native["direction"]
        anomalies = active_anomalies(snapshot)
        previous, was_strong, previous_anomalies = self.states.get(key, (None, False, frozenset()))
        new_anomalies = anomalies - previous_anomalies
        strong = native["native_signal"] in {"STRONG_BULL", "STRONG_BEAR"}
        trigger = (
            (direction in DIRECTIONAL and direction != previous)
            or (previous in DIRECTIONAL and direction == "NEUTRAL")
            or (strong and not was_strong)
            or bool(new_anomalies)
        )
        kind, filter_reason = self.policy.classify(snapshot, native, now)
        previous_kind = self.opportunities.get(key)
        self.opportunities[key] = kind
        trigger = trigger or (kind is not None and kind != previous_kind)
        if not trigger:
            self.candidate_seen.pop(key, None)
            if direction != "NOT_AVAILABLE" and gate not in {"tape_unavailable", "book_unavailable"}:
                self.states[key] = direction, strong, anomalies
            return

        decision = "queued" if kind else filter_reason
        if kind and not getattr(self.gemini, "key", None):
            decision = "unconfigured"
        elif kind and self.journal is not None:
            decision = self.journal.request_block(key[0], now, self.policy) or "queued"
        if gate != "ready":
            decision = gate
        if not getattr(self, "legacy_ai_enabled", True) or getattr(self.hub, "attention", {}).get("context_only"):
            decision = "context_only"
        if decision == "queued":
            if any(pending[0] == key[0] for pending in self.pending):
                decision = "episode_pending"
            elif self.jobs.full():
                decision = "queue_full"
        # Detection state advances even for rejected events: a stable background
        # situation is one event, not a fresh candidate every five seconds.
        self.states[key] = direction, strong, anomalies

        event = {
            "type": "market_activity" if new_anomalies else "orderflow_transition",
            "anomalies": sorted(new_anomalies),
            "previous_direction": previous,
            "direction": direction,
            "native_signal": native["native_signal"],
            "source": native["source"],
            "detected_at": now,
            "analysis_gate": gate,
            "context_5m": relation,
            "strength": finding_strength(snapshot, native),
            "opportunity_kind": kind,
            "ai_selection": decision,
        }
        signature = (direction, strong, frozenset(new_anomalies), kind)
        previous_candidate = self.candidate_seen.get(key)
        if previous_candidate and previous_candidate[0] == signature:
            candidate_id, detected_at, old_decision = previous_candidate[1:]
            event["detected_at"] = detected_at
            if old_decision != decision and self.journal is not None:
                self.journal.candidate_decision(candidate_id, decision, now)
        else:
            candidate_id = hashlib.sha256(dump(snapshot_key(snapshot)).encode()).hexdigest()
            detected_at = now
            if self.journal is not None:
                self.journal.record_candidate(candidate_id, snapshot, event, decision, now)
        self.candidate_seen[key] = (signature, candidate_id, detected_at, decision)
        event["candidate_id"] = candidate_id
        if (
            not self.ai_routes
            and self.notify
            and self.journal is not None
            and row
            and gate not in {"book_unavailable", "tape_unavailable"}
        ):
            self.enqueue_facts(candidate_id, snapshot, native, event, row, now)
        if self.journal is not None:
            self.journal.select_ai(candidate_id, now, kind, decision)
            self.journal.record(candidate_id, event, snapshot, native)
            if not self.journal.fact_message(candidate_id):
                with self.journal.db:
                    self.journal.db.execute("UPDATE analyses SET delivery='not_applicable' WHERE id=?", (candidate_id,))
            self.journal.candidate_decision(candidate_id, decision, now, candidate_id)
            if decision != "queued":
                self.journal.finish(
                    candidate_id,
                    {"status": "skipped", "reason": decision, "snapshot": snapshot_key(snapshot)},
                    None,
                    now,
                )
        if decision != "queued":
            return
        self.states[key] = direction, strong, anomalies
        self.pending.add(key)
        self.sequence += 1
        # New candidates in one scan have the same age. Let strong events
        # lead that batch, while older candidates retain priority over new ones.
        priority = 0 if kind == "early" else 1
        # Bounded adjustments let agreement and book quality matter without starving old events.
        strength = event["strength"]["rank"]
        order_at = (
            now
            - (30 if kind == "early" else 0)
            + PRIORITY_STEP_SECONDS * ((gate == "wide_spread") - (relation == "aligned"))
            - STRENGTH_STEP_SECONDS * max(0, strength - 1)
        )
        self.jobs.put_nowait((order_at, priority, self.sequence, key, event))

    async def run(self):
        while True:
            try:
                refresh = getattr(self.hub, "refresh_snapshot", None)
                if refresh is not None:
                    await refresh()
                self.scan(time.time())
            except Exception:
                LOG.exception("Screener scan failed")
            await asyncio.sleep(5)

    async def refresh_activity(self):
        await refresh_candles(self.hub, {**WINDOWS, "15m": CONTEXT_WINDOW_SECONDS}, self.candles)

    async def refresh_profiles(self, path):
        while True:
            try:
                now = time.time()
                symbols = sorted({symbol for symbol, _ in self.rows})
                if symbols:
                    self.profiles = await asyncio.to_thread(calculate_profiles, path, now, symbols)
                    if now - self.last_profile_prune > DAY:
                        self.journal.prune_instrument_samples(now - 30 * DAY)
                        self.last_profile_prune = now
            except Exception:
                LOG.exception("Instrument profile refresh failed")
            await asyncio.sleep(300 if self.profiles else 30)

    async def refresh_quality(self, path):
        while True:
            try:
                self.quality_report = await asyncio.to_thread(calculate_quality, path, time.time(), WINDOWS)
            except Exception:
                LOG.exception("AI quality report refresh failed")
            await asyncio.sleep(60)

    def skip_ai(self, key, reason, now):
        self.journal.select_ai(key, now, None, reason)
        self.journal.candidate_decision(key, reason, now)
        row = self.journal.db.execute("SELECT snapshot FROM analyses WHERE id=?", (key,)).fetchone()
        if row:
            import json

            snapshot = json.loads(row[0])
            self.journal.finish(
                key,
                {
                    "status": "error" if reason == "error" else "skipped",
                    "reason": reason,
                    "snapshot": snapshot_key(snapshot),
                },
                None,
                now,
            )
        self.queue_hypothesis(key)

    def request_analysis(self, symbol, window, now):
        """Manual requests bypass significance, but never data quality or budget."""
        attention = getattr(self.hub, "attention", {})
        if not getattr(self, "legacy_ai_enabled", True) or attention.get("context_only"):
            card = next(
                (r["facts"] for r in attention.get("active", []) if r["symbol"].casefold() == symbol.casefold()), None
            )
            if not card or now - card.get("received_at", 0) > 15 or card["status"] != "observing":
                return "Нет свежей карточки наблюдения. Инструмент может быть вне фокуса."
            lines = [html.escape(symbol) + " · факты наблюдения"]
            for d in card["densities"]:
                lines.append(
                    f"{html.escape(d['side'])} {d['price']:g} · ${d['notional_usd']:,.0f}"
                    f" · наблюдается {d['observed_age_seconds']:.0f} с"
                    f" · расстояние {d['distance_bps']:.1f} б.п."
                )
            if not card["densities"]:
                lines.append("Крупных зон по текущему правилу не найдено.")
            lines.append("Возраст указан с начала наблюдения. AI для рекомендации входа не вызывается.")
            return "\n".join(lines)
        matches = [key for key in self.rows if key[0].casefold() == symbol.casefold() and key[1] == window]
        if not matches:
            return "Инструмент или окно отсутствует в текущем наблюдении."
        key = matches[0]
        row = self.rows[key]
        snapshot, native = row["snapshot"], row["hyperdata"]
        _, reason = self.policy.classify(snapshot, native, now)
        if self.hub.demo or reason in {"stale_snapshot", "stale_data", "wide_spread"}:
            return "AI: пропущен — " + ("деморежим" if self.hub.demo else REASON_LABELS.get(reason, reason))
        if not self.gemini.key:
            return "AI: пропущен — нет API-ключа"
        block = self.journal.request_block(key[0], now, self.policy)
        if block or any(p[0] == key[0] for p in self.pending) or self.jobs.full():
            return "AI: пропущен — " + REASON_LABELS.get(block, "эпизод уже в очереди или очередь заполнена")
        if self.notify and self.notifications.full():
            return "AI: пропущен — очередь уведомлений заполнена"
        event_id = hashlib.sha256(f"manual:{key}:{now}".encode()).hexdigest()
        event = {
            "type": "manual",
            "manual": True,
            "candidate_id": event_id,
            "detected_at": now,
            "direction": native["direction"],
            "anomalies": [],
            "strength": row["strength"],
            "ai_selection": "queued",
            "opportunity_kind": "manual",
        }
        self.journal.record_candidate(event_id, snapshot, event, "queued", now)
        self.journal.select_ai(event_id, now, "manual", "queued")
        # Manual invocation explicitly opts this single message into Telegram.
        if self.notify and self.journal.register_fact_message(event_id, snapshot, event, native):
            self.notifications.put_nowait(("facts", event_id))
        else:
            self.journal.record(event_id, event, snapshot, native)
        self.journal.candidate_decision(event_id, "queued", now, event_id)
        self.pending.add(key)
        self.sequence += 1
        self.jobs.put_nowait((now - 30, 0, self.sequence, key, event))
        return "AI: запрос принят в очередь; лимиты и свежесть будут проверены перед отправкой."

    async def analyze(self):
        next_request = 0
        while True:
            if self.gemini.key:
                await asyncio.sleep(max(0, next_request - time.monotonic()))
            _, _, _, candidate, event = await self.jobs.get()
            event_id = None
            candidate_id = event["candidate_id"]
            try:
                now = time.time()
                if not getattr(self, "legacy_ai_enabled", True) or getattr(
                    self.hub, "attention", {}
                ).get("context_only"):
                    self.skip_ai(candidate_id, "context_only", now)
                    continue
                row = self.rows.get(candidate)
                if not row:
                    self.skip_ai(candidate_id, "left_universe", now)
                    continue
                if now - event["detected_at"] > MAX_EVENT_AGE[candidate[1]]:
                    self.skip_ai(candidate_id, "event_expired", now)
                    continue
                snapshot, native = row["snapshot"], row["hyperdata"]
                if (
                    now - snapshot["captured_at"] > 10
                    or snapshot["metrics"]["price"]["quality"] != "ok"
                    or native["direction"] == "NOT_AVAILABLE"
                    or row["analysis_gate"] in {"tape_unavailable", "book_unavailable"}
                ):
                    self.skip_ai(candidate_id, "stale_at_dispatch", now)
                    self.states.pop(candidate, None)
                    continue
                if event["anomalies"] and not active_anomalies(snapshot).intersection(event["anomalies"]):
                    self.skip_ai(candidate_id, "anomaly_ended", now)
                    self.states.pop(candidate, None)
                    continue
                if native["direction"] != event["direction"]:
                    # The original transition is obsolete. Let the next scan
                    # decide whether the new direction merits a fresh event.
                    self.skip_ai(candidate_id, "direction_changed", now)
                    continue
                kind, reason = self.policy.classify(snapshot, native, now)
                if reason in {"stale_snapshot", "stale_data", "wide_spread"} or (not event.get("manual") and not kind):
                    self.skip_ai(candidate_id, reason, now)
                    continue
                if not self.gemini.key:
                    self.skip_ai(candidate_id, "unconfigured", now)
                    continue
                fact = self.journal.fact_message(candidate_id)
                # Analyze the current validated snapshot, even if the fact message
                # was queued earlier. Never spend budget interpreting expired input.
                snapshot = with_movement_context(snapshot, row["movement_phase"])
                other_window = "5m" if candidate[1] == "1m" else "1m"
                baseline = with_context(snapshot, self.rows.get((candidate[0], other_window)))
                profile = profile_context(self.profiles.get(candidate))
                if profile and profile["updated_at"] <= snapshot["captured_at"]:
                    baseline = {**baseline, "instrument_profile": profile}
                if (
                    row["strength"]["rank"] >= STRONG_RANK
                    and row["btc_context"]
                    and row["btc_context"]["updated_at"] <= snapshot["captured_at"]
                ):
                    baseline = {**baseline, "btc_context": row["btc_context"]}
                snapshot = with_senior_context(baseline, self.candles.get((candidate[0], "15m")))
                snapshot = with_market_context(snapshot, self.hub)
                event = {
                    **event,
                    "analyzed_signal": native["native_signal"],
                    "analysis_gate": row["analysis_gate"],
                    "context_5m": row["context_5m"],
                    "facts": row["facts"],
                    "strength": row["strength"],
                    "movement_phase": row["movement_phase"],
                }
                event_id = candidate_id
                self.journal.save_fact_analysis_input(event_id, snapshot)
                # Align the independent native vote with the actual model input.
                with self.journal.db:
                    self.journal.db.execute(
                        "UPDATE analyses SET hyperdata=?,event=? WHERE id=?", (dump(native), dump(event), event_id)
                    )
                block = self.journal.reserve_request(candidate_id, candidate[0], now, self.policy)
                if block:
                    self.skip_ai(candidate_id, block, now)
                    continue
                self.journal.select_ai(candidate_id, now, event.get("opportunity_kind"), "analyzing")
                self.journal.candidate_decision(candidate_id, "analyzing", now, event_id)
                next_request = time.monotonic() + AI_INTERVAL
                self.queue_hypothesis(event_id)
                ai = await self.gemini.analyze(snapshot)
                verdict = compare(snapshot, native, ai)
                self.journal.finish(event_id, ai, verdict, time.time())
                self.journal.select_ai(candidate_id, time.time(), event.get("opportunity_kind"), ai["status"])
                if fact:
                    self.queue_hypothesis(event_id)
                else:
                    self.journal.delivery(event_id, "not_applicable")
                if (
                    self.policy.experiments
                    and self.gemini.key
                    and ai["status"] == "ok"
                    and "context_15m" in snapshot
                    and baseline.get(f"context_{other_window}", {}).get("metrics")
                ):
                    sample = self.context_events % CONTEXT_TRIAL_INTERVAL == 0
                    self.context_events += 1
                    if sample:
                        await asyncio.sleep(max(0, next_request - time.monotonic()))
                        trial_now = time.time()
                        if (
                            not self.policy.experiments
                            or not self.gemini.key
                            or trial_now - snapshot["captured_at"] > MAX_EVENT_AGE[candidate[1]]
                            or self.journal.reserve_request(
                                event_id, candidate[0], trial_now, self.policy, experiment=True
                            )
                        ):
                            continue
                        next_request = time.monotonic() + AI_INTERVAL
                        try:
                            comparison = await self.gemini.analyze(baseline)
                            if comparison.get("snapshot") != snapshot_key(snapshot):
                                raise ValueError("Mismatched comparison snapshot")
                            self.journal.record_context_trial(event_id, comparison, time.time())
                        except Exception:
                            LOG.exception("Context comparison failed")
            except Exception:
                LOG.exception("Event analysis failed")
                self.skip_ai(candidate_id, "error", time.time())
            finally:
                self.pending.discard(candidate)
                self.jobs.task_done()

    @staticmethod
    def alert_reason(snapshot, native, event):
        metrics = snapshot.get("metrics", {})
        start, end = (fresh(metrics, name) for name in ("first_trade_price", "last_trade_price"))
        direction = native["direction"]
        reversal = (
            event.get("previous_direction") in DIRECTIONAL
            and direction in DIRECTIONAL
            and event["previous_direction"] != direction
        )
        price_agrees = (
            start is not None and end is not None and start != end and (end > start) == (direction == "BULLISH")
        )
        return bool(event.get("anomalies")) or (reversal and price_agrees)

    def enqueue_facts(self, key, snapshot, native, event, row, now):
        if not getattr(self, "legacy_ai_enabled", True):
            return
        if (
            (event["strength"]["rank"] < max(3, alert_min_rank()) and event.get("opportunity_kind") != "early")
            or self.notifications.full()
            or now - event["detected_at"] > MAX_EVENT_AGE[snapshot["window"]]
            or not self.alert_reason(snapshot, native, event)
        ):
            return
        event = {**event, "facts": row["facts"], "movement_phase": row["movement_phase"]}
        snapshot = with_context(snapshot, row["other"])
        snapshot = with_movement_context(snapshot, row["movement_phase"])
        if row["btc_context"]:
            snapshot = {**snapshot, "btc_context": row["btc_context"]}
        if self.journal.register_fact_message(key, snapshot, event, native):
            self.notifications.put_nowait(("facts", key))

    def queue_hypothesis(self, key):
        if not self.notify:
            return
        fact = self.journal.fact_message(key)
        if (
            fact
            and fact["message_id"] is not None
            and (fact["ai"] or self.journal.ai_reason(key) == "analyzing")
            and fact["edit_status"] != "queued"
            and not self.notifications.full()
        ):
            self.journal.fact_message_edit(key, "queued")
            self.notifications.put_nowait(("edit", key))

    def ai_display(self, row):
        if row["ai"]:
            return row["ai"]
        reason = self.journal.ai_reason(row["id"])
        if reason == "analyzing":
            return {"status": "pending"}
        if reason == "queued":
            return {"status": "queued", "reason": "queued"}
        return {"status": "skipped", "reason": reason}

    async def edit_alert(self, row, updates, *, include_ai):
        snapshot, native, event = row["snapshot"], row["hyperdata"], row["event"]
        ai = self.ai_display(row)
        result = await self.hub.alerts.edit_telegram(
            row["message_id"],
            alert_text(snapshot, native, ai, event, updates=updates),
            rich_html=alert_text(snapshot, native, ai, event, rich=True, updates=updates) if row["rich"] else None,
            copy_text=futures_ticker(snapshot),
        )
        if hasattr(self.journal, "record_telegram_attempt"):
            self.journal.record_telegram_attempt(
                row["id"], "edit", getattr(self.hub.alerts, "last_telegram_result", {})
            )
        return result

    async def deliver(self):
        while True:
            action, key = await self.notifications.get()
            try:
                row = self.journal.fact_message(key)
                if not row:
                    continue
                snapshot, native, event = row["snapshot"], row["hyperdata"], row["event"]
                ticker = futures_ticker(snapshot)
                if action in {"edit", "pulse_edit"}:
                    display = self.ai_display(row)
                    edited = await self.edit_alert(row, self.journal.fact_updates(key).values(), include_ai=True)
                    self.journal.fact_message_edit(key, "edited" if edited else "failed")
                    if action == "pulse_edit":
                        self.journal.pulse_edit_status(key, "edited" if edited else "failed")
                    latest = self.journal.fact_message(key)
                    if latest and display != self.ai_display(latest):
                        self.queue_hypothesis(key)
                    continue

                now = time.time()
                if now - event["detected_at"] > MAX_EVENT_AGE[snapshot["window"]]:
                    self.journal.delivery(key, "expired")
                    continue
                parent = (
                    None
                    if event.get("manual")
                    else self.journal.recent_fact_message(snapshot["symbol"], now - ALERT_COOLDOWN)
                )
                if parent:
                    updates = self.journal.fact_updates(parent["id"])
                    previous = updates.get(row["window"])
                    if previous is None and parent["window"] == row["window"]:
                        previous = parent
                    if previous and event["strength"]["rank"] <= previous["event"]["strength"]["rank"]:
                        self.journal.delivery(key, "cooldown")
                        continue
                    updates[row["window"]] = row
                    if await self.edit_alert(parent, updates.values(), include_ai=parent["edit_status"] == "edited"):
                        self.journal.merge_fact_message(parent["id"], key, row["window"], time.time())
                    else:
                        self.journal.delivery(key, "merge_failed")
                    continue
                last = self.journal.last_delivery(snapshot["symbol"], snapshot["window"])
                if not event.get("manual") and last and now - last < ALERT_COOLDOWN:
                    self.journal.delivery(key, "cooldown")
                    continue
                self.journal.delivery(key, "sending", now)
                pending = self.ai_display(row)
                result = await self.hub.alerts.send_telegram(
                    alert_text(snapshot, native, pending, event),
                    rich_html=alert_text(snapshot, native, pending, event, rich=True),
                    copy_text=ticker,
                    return_message=True,
                )
                transport = getattr(self.hub.alerts, "last_telegram_result", {})
                if hasattr(self.journal, "record_telegram_attempt"):
                    self.journal.record_telegram_attempt(key, "send", transport)
                failure = transport.get("state", "failed_or_unconfirmed")
                self.journal.delivery(key, "sent" if result else failure, time.time() if result else now)
                if result:
                    self.journal.fact_message_sent(key, *result)
                    self.queue_hypothesis(key)
            except Exception:
                if action == "pulse_edit":
                    self.journal.pulse_edit_status(key, "failed")
                LOG.exception("Notification processing failed")
            finally:
                self.notifications.task_done()
