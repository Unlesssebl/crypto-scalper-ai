"""Read-only Market Pulse context, separate from native rolling-window signals."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import aiohttp

from hyperdata_terminal.market_pulse.models import MINUTE, digest, stream
from hyperdata_terminal.market_pulse.research import fetch_windows

from .pulse_shadow import ShadowJournal

VERSION = "market-pulse.context.v1"
USABLE = {"observed_no_detected_gap", "reconciled"}


class PulseContext:
    def __init__(self, url: str, markets=(), *, journal=None):
        self.url = url.rstrip("/")
        self.markets = tuple(markets)
        self.windows = []
        self.channels = []
        self.gaps = []
        self.updated_at = 0
        self.status = "connecting" if url else "disabled"
        self.error = None
        self.shadow = ShadowJournal(journal.db) if journal is not None else None
        self.history_end = None
        self.assessments = {}
        self.research_error = None
        self.desired_plan = None
        self.attention = {}
        self.last_publish = 0
        self.publisher_status = "disabled"
        self.overview = {}

    async def refresh(self, session, now=None):
        now = time.time() if now is None else now
        end = int(now * 1000) // 60_000 * 60_000
        try:
            await self.refresh_overview(session, now)
            if self.desired_plan:
                try:
                    await self.refresh_control(session, now)
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
                    self.publisher_status = "unavailable"
            async with session.get(self.url + "/coverage", timeout=aiohttp.ClientTimeout(total=4)) as response:
                response.raise_for_status()
                coverage = await response.json()
            async with session.get(self.url + "/windows", params={
                "from_ms": end - 120_000, "to_ms": end, "limit": 1000,
            }, timeout=aiohttp.ClientTimeout(total=4)) as response:
                response.raise_for_status()
                windows = await response.json()
            if (windows.get("schema_version") != "market-pulse.window.v1"
                    or coverage.get("schema_version") != "market-pulse.window.v1"
                    or not isinstance(windows.get("windows"), list)
                    or not isinstance(coverage.get("channels"), list)) :
                raise ValueError("unsupported window schema")
            for rows, fields in ((coverage["channels"], {"venue", "native_market_id", "channel"}),
                                 (windows["windows"], {"venue", "native_market_id", "channel", "aggregation_kind",
                                  "window_start_ms", "quality", "provisional", "version"})):
                if any(not isinstance(row, dict) or not fields.issubset(row) for row in rows):
                    raise ValueError("incomplete source contract")
            self.windows = windows["windows"]
            self.channels = coverage["channels"]
            self.gaps = coverage.get("gaps", [])
            self.updated_at = now
            self.status, self.error = "connected", None
            if self.shadow is not None:
                await self.refresh_shadow(session, now)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError, TypeError) as exc:
            self.status, self.error = "unavailable", type(exc).__name__
            self.windows, self.channels = [], []

    async def refresh_control(self, session, now):
        if self.desired_plan and self.desired_plan.get("generated_at_ms") and now - self.last_publish >= 15:
            token = os.getenv("PULSE_INGEST_TOKEN", "")
            if not token:
                from pathlib import Path

                from dotenv import dotenv_values
                token = dotenv_values(Path(__file__).resolve().parents[2] / "deploy/market-pulse/.env").get(
                    "PULSE_INGEST_TOKEN", "")
            if token:
                try:
                    async with session.post(os.getenv("PULSE_INGEST_URL", "http://127.0.0.1:8430")
                                            + "/ingest/v1/attention", json=self.desired_plan,
                                            headers={"Authorization": "Bearer " + token},
                                            timeout=aiohttp.ClientTimeout(total=4)) as response:
                        response.raise_for_status()
                    self.last_publish, self.publisher_status = now, "published"
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    self.publisher_status = "unavailable"
            else:
                self.publisher_status = "unconfigured"
        if self.desired_plan:
            async with session.get(self.url + "/attention", timeout=aiohttp.ClientTimeout(total=4)) as response:
                response.raise_for_status()
                self.attention = await response.json()
            from hyperdata_terminal.market_pulse.control import desired_symbols
            self.markets = desired_symbols(self.attention.get("plan"), int(now * 1000))

    async def refresh_overview(self, session, now):
        try:
            async with session.get(self.url + "/overview", timeout=aiohttp.ClientTimeout(total=4)) as response:
                response.raise_for_status()
                body = await response.json()
            if body.get("schema_version") != "market-pulse.overview.v1" or not isinstance(body.get("items"), list):
                raise ValueError("unsupported overview")
            self.overview = body
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
            self.overview = {}

    async def refresh_shadow(self, session, now):
        end = (int(now * 1000) - 5000) // MINUTE * MINUTE
        refresh_key = (end, digest(self.windows))
        if self.history_end == refresh_key and not self.shadow.pending_revisions():
            return
        try:
            # Seven-day reference plus one day for bounded historical corrections.
            rows = await fetch_windows(session, self.url, end - 11_520 * MINUTE, end)
            self.shadow.reconcile(rows, now)
            assessments = {}
            for target in rows:
                if target["window_start_ms"] != end - MINUTE:
                    continue
                symbol = target["native_market_id"].removesuffix("-USDT-SWAP")
                if symbol not in self.markets:
                    continue
                assessments[stream(target)] = {
                    "target_hash": digest(target), "window_start_ms": end - MINUTE,
                    **self.shadow.evaluate(rows, target, now),
                }
            self.assessments, self.history_end = assessments, refresh_key
            self.research_error = None
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
            self.assessments = {}
            self.research_error = type(exc).__name__

    async def run(self):
        if not self.url:
            return
        async with aiohttp.ClientSession() as session:
            while True:
                await self.refresh(session)
                await asyncio.sleep(5)

    def summary(self, now=None):
        now = time.time() if now is None else now
        status = self.status if not self.updated_at or now - self.updated_at <= 15 else "stale"
        return {"status": status, "updated_at": self.updated_at, "error": self.error,
                "markets": list(self.markets), "mode": "descriptive_shadow", "version": VERSION,
                "historical_gaps": sum(g.get("state") == "gap_detected" for g in self.gaps),
                "shadow": "enabled" if self.shadow else "disabled", "research_error": self.research_error,
                "overview": {"symbols": len(self.overview.get("items", [])),
                             "ready": sum(r.get("status") == "ready" for r in self.overview.get("items", []))},
                "attention": self.attention, "attention_publisher": self.publisher_status,
                "pending_revisions": self.shadow.pending_revisions() if self.shadow else 0,
                "fresh_channels": sum(not c.get("stale", True) for c in self.channels)}

    def facts(self, symbol, now):
        result = {**self.summary(now), "symbol": symbol, "activity": "threshold_not_calibrated",
                  "flow": "unavailable", "venues": [],
                  "limits": "Closed UTC minute; not the native rolling window. No rarity or entry claim."}
        if not self.url:
            return result
        if symbol not in self.markets:
            result["status"] = "not_covered"
            return result
        if result["status"] != "connected":
            return result
        # Same UTC minute on both venues; do not backfill from older usable data.
        start = (int(now * 1000) - 5_000) // 60_000 * 60_000 - 60_000
        result["window_start_ms"], result["window_end_ms"] = start, start + 60_000
        from hyperdata_terminal.instruments import identity, is_binance

        sources = (("binance", identity(symbol)[1], "aggTrade"),) if is_binance(symbol) else (
            ("hyperliquid", symbol, "trades"), ("okx", symbol + "-USDT-SWAP", "trades-all"))
        for venue, market, channel in sources:
            kind = "aggregated" if venue == "binance" else "execution"
            source = next((c for c in self.channels if (c["venue"], c["native_market_id"], c["channel"])
                           == (venue, market, channel)), None)
            window = next((w for w in self.windows if (w["venue"], w["native_market_id"], w["channel"],
                          w["aggregation_kind"], w["window_start_ms"])
                          == (venue, market, channel, kind, start)), None)
            quality = "missing"
            if source and not source.get("stale", True) and window:
                quality = window["quality"] if not window["provisional"] else "warming_up"
            elif source and source.get("stale", True):
                quality = "stale"
            item = {"venue": venue, "market": market, "channel": channel, "quality": quality,
                    "aggregation_kind": kind,
                    "currency": "USDC" if venue == "hyperliquid" else "USDT",
                    "version": None if window is None else window["version"],
                    "baseline_version": None if window is None else window.get("baseline_version"),
                    "flow": "unavailable", "ofi": None}
            assessment = self.assessments.get((venue, market, channel, kind))
            if (assessment and window and assessment["window_start_ms"] == start
                    and assessment["target_hash"] == digest(window) and quality in USABLE):
                item["shadow"] = assessment
            if quality in USABLE:
                try:
                    # A missing side with known execution counts means zero;
                    # unknown normalization blocks both totals and comparison.
                    if window.get("coverage", {}).get("notional_blocked_events", 0):
                        raise ValueError("unknown normalization")
                    buy = Decimal(window["buy_quote"] or "0")
                    sell = Decimal(window["sell_quote"] or "0")
                    if not buy.is_finite() or not sell.is_finite() or buy < 0 or sell < 0:
                        raise ValueError("invalid volume")
                    ofi = (buy - sell) / (buy + sell) if buy + sell else Decimal(0)
                    item.update(execution_count=window["execution_count"] if kind == "execution" else None,
                                aggregate_count=window["event_count"] if kind == "aggregated" else None,
                                quote_volume=str(buy + sell),
                                ofi=float(ofi), flow="buy" if ofi > Decimal("0.15") else
                                "sell" if ofi < Decimal("-0.15") else "neutral")
                except (InvalidOperation, ValueError, TypeError):
                    item["quality"] = "normalization_unavailable"
            result["venues"].append(item)
        usable = [item for item in result["venues"] if item["quality"] in USABLE]
        result["status"] = "ready" if len(usable) == 2 else "partial" if usable else "unavailable"
        if len(usable) == 2:
            flows = {item["flow"] for item in usable}
            result["flow"] = ("aligned_buy" if flows == {"buy"} else "aligned_sell" if flows == {"sell"}
                              else "opposed" if flows == {"buy", "sell"} else "neutral_or_mixed")
        elif is_binance(symbol) and usable:
            result["status"] = "ready"
            result["flow"] = "single_venue_" + usable[0]["flow"]
        result["evidence_id"] = hashlib.sha256(json.dumps(
            [VERSION, symbol, start, [(v["venue"], v["version"], v["quality"]) for v in result["venues"]]],
            sort_keys=True,
        ).encode()).hexdigest()
        return result


def pulse_text(facts):
    if facts["status"] == "not_covered":
        return "Инструмент вне текущего подробного наблюдения attention"
    if not facts["venues"]:
        return "Данные недоступны; межбиржевое сравнение не выполнено"
    parts = []
    for item in facts["venues"]:
        if item["quality"] not in USABLE:
            quality = {"missing": "нет закрытого окна", "stale": "данные устарели",
                       "partial": "неполная минута", "gap_detected": "пропуск данных",
                       "recovering": "восстановление", "warming_up": "прогрев",
                       "normalization_unavailable": "единицы объёма не определены",
                       "unavailable": "нет данных"}.get(item["quality"], item["quality"])
            parts.append(item["venue"] + ": " + quality)
        else:
            parts.append(f"{item['venue']}: {item['execution_count']} исп., "
                         f"{Decimal(item['quote_volume']):,.0f} {item['currency']}, OFI {item['ofi']:+.0%}")
    interval = "Закрытая минута"
    if facts.get("window_start_ms") is not None and facts.get("window_end_ms") is not None:
        zone = timezone(timedelta(hours=3))
        start = datetime.fromtimestamp(facts["window_start_ms"] / 1000, zone).strftime("%H:%M")
        end = datetime.fromtimestamp(facts["window_end_ms"] / 1000, zone).strftime("%H:%M")
        interval += f" {start}–{end} МСК"
    else:
        interval += " UTC"
    relation = {"aligned_buy": "обе биржи: покупки", "aligned_sell": "обе биржи: продажи",
                "opposed": "стороны потока расходятся", "neutral_or_mixed": "единой стороны потока нет",
                "unavailable": "сопоставление потока недоступно"}.get(facts.get("flow"), "")
    samples = []
    for venue in facts["venues"]:
        metric = venue.get("shadow", {}).get("metrics", {}).get("execution_count")
        if metric:
            samples.append(f"{venue['venue']}: {metric['samples']} пригодных минут, база {metric['coverage']:.0%}")
    rarity = ". Редкость не оценена"
    if samples:
        rarity += " · теневая база: " + "; ".join(samples) + "; пороги не утверждены"
    return interval + " · " + "; ".join(parts) + (". " + relation if relation else "") + rarity
