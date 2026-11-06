"""
Alert system — sends notifications for significant market events.
Supports Telegram and Discord webhooks.

Usage:
    alerts = AlertManager()
    alerts.attach(hub)  # Automatically monitors for alert conditions

Environment variables:
    TELEGRAM_BOT_TOKEN - Telegram bot token
    TELEGRAM_CHAT_ID - Chat/channel ID to send to
    DISCORD_WEBHOOK_URL - Discord webhook URL
"""

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    """Thresholds for triggering alerts."""

    min_liquidation_usd: float = 500_000  # Alert on liqs > $500K
    min_whale_position_usd: float = 5_000_000  # Alert on whale positions > $5M
    extreme_funding_annual: float = 1.0  # Alert on >100% annualized funding
    liquidation_cascade_count: int = 50  # Alert if >50 liqs in 5 minutes
    liquidation_cascade_volume_usd: float = 500_000  # Alert if >$500K volume in 5 minutes
    liquidation_cascade_window: int = 300  # 5 minutes
    smart_money_max_rank: int = 5  # Alert on top N smart money wallets
    hlp_zscore_threshold: float = 2.0  # Alert on HLP Z-score extremes
    cooldown_seconds: float = 300.0  # 5 min cooldown between similar alerts (was 60s — too noisy)


class AlertManager:
    INTEL_INTERVAL = 4 * 3600  # 4 hours

    def __init__(self, config: AlertConfig | None = None):
        self.config = config or AlertConfig()
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.discord_webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
        self._session: aiohttp.ClientSession | None = None
        self._last_alert_times: dict[str, float] = {}
        self._recent_liq_count = 0
        self._recent_liq_volume = 0.0
        self._recent_liq_long_count = 0
        self._recent_liq_long_volume = 0.0
        self._recent_liq_short_count = 0
        self._recent_liq_short_volume = 0.0
        self._recent_liq_top_event = None  # largest single liq in the window
        self._recent_liq_reset = time.time()
        self.enabled = bool(self.telegram_token and self.telegram_chat_id) or bool(self.discord_webhook)
        self.alerts_sent = 0

        # Market intelligence summaries
        self._hub = None
        self._intel_task: asyncio.Task | None = None

        if not self.enabled:
            logger.info("AlertManager: No TELEGRAM_BOT_TOKEN/CHAT_ID or DISCORD_WEBHOOK_URL set. Alerts disabled.")
            logger.info("AlertManager: Set env vars to enable. See .env.example")

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        if self._hub:
            self._intel_task = asyncio.create_task(self._intel_loop())

    async def stop(self) -> None:
        if self._intel_task:
            self._intel_task.cancel()
            try:
                await self._intel_task
            except asyncio.CancelledError:
                pass
        self._intel_task = None
        if self._session and not self._session.closed:
            await self._session.close()

    def attach(self, hub) -> None:
        """Attach to hub event bus."""
        self._hub = hub
        hub.on_liquidation(self._check_liquidation)
        hub.on_signal(self._check_smart_money)
        # Start loops if we're already running (attach called after start)
        if self._session and not self._intel_task:
            self._intel_task = asyncio.create_task(self._intel_loop())

    # Per-key cooldown overrides (seconds)
    _COOLDOWN_OVERRIDES = {
        "cascade_alert": 600,  # 10 minutes between cascade alerts
    }

    def _should_alert(self, key: str) -> bool:
        """Check cooldown for a specific alert type."""
        now = time.time()
        last = self._last_alert_times.get(key, 0)
        cooldown = self._COOLDOWN_OVERRIDES.get(key, self.config.cooldown_seconds)
        if now - last < cooldown:
            return False
        self._last_alert_times[key] = now
        return True

    def _reset_cascade_window(self) -> None:
        """Reset all cascade tracking counters."""
        self._recent_liq_count = 0
        self._recent_liq_volume = 0.0
        self._recent_liq_long_count = 0
        self._recent_liq_long_volume = 0.0
        self._recent_liq_short_count = 0
        self._recent_liq_short_volume = 0.0
        self._recent_liq_top_event = None
        self._recent_liq_reset = time.time()

    def _check_liquidation(self, event) -> None:
        """Check if a liquidation event warrants an alert."""
        # Track cascade window — reset if window expired
        now = time.time()
        if now - self._recent_liq_reset > self.config.liquidation_cascade_window:
            self._reset_cascade_window()

        # Accumulate cascade stats
        self._recent_liq_count += 1
        self._recent_liq_volume += event.size_usd

        if event.side == "long":
            self._recent_liq_long_count += 1
            self._recent_liq_long_volume += event.size_usd
        else:
            self._recent_liq_short_count += 1
            self._recent_liq_short_volume += event.size_usd

        # Track top liquidation in the window
        if self._recent_liq_top_event is None or event.size_usd > self._recent_liq_top_event.size_usd:
            self._recent_liq_top_event = event

        # Large single liquidation — DISABLED (too noisy, use trade alerts instead)
        # if event.size_usd >= self.config.min_liquidation_usd:
        #     if self._should_alert(f"large_liq_{event.symbol}"):
        #         ...
        pass

        # Liquidation cascade — trigger on count OR volume threshold
        cascade_triggered = (
            self._recent_liq_count >= self.config.liquidation_cascade_count
            or self._recent_liq_volume >= self.config.liquidation_cascade_volume_usd
        )
        # Liquidation cascade — DISABLED (too noisy, use trade alerts instead)
        # if cascade_triggered:
        #     ...
        if cascade_triggered:
            self._reset_cascade_window()  # still reset the window, just don't send

    def _check_smart_money(self, signal) -> None:
        """Check if a smart money signal warrants an alert (top-5 ranked, follow type)."""
        if signal.wallet_rank > self.config.smart_money_max_rank or signal.wallet_rank <= 0:
            return
        if signal.signal_type != "follow":
            return
        if not self._should_alert(f"smart_money_{signal.address[:10]}"):
            return

        # Smart money alerts — DISABLED (use trade alerts instead)
        pass

    async def _check_hlp_zscore(self, hlp_stats: dict) -> None:
        """Check if HLP Z-score is at an extreme (>2.0 or <-2.0)."""
        zscore = hlp_stats.get("delta_zscore", 0)
        if abs(zscore) <= self.config.hlp_zscore_threshold:
            return
        if not self._should_alert("hlp_zscore"):
            return

        # HLP Z-score alerts — DISABLED (too noisy)
        pass

    # ── Market Intelligence ─────────────────────────────────────────

    async def _intel_loop(self) -> None:
        """Market intelligence summary — DISABLED (use daily trade summary instead)."""
        while True:
            await asyncio.sleep(self.INTEL_INTERVAL)
            # Disabled — trade alerts handle notifications now
            pass

    async def send_intel_now(self) -> bool:
        """Send an immediate market intelligence report (for testing)."""
        if not self.enabled or not self._hub:
            return False
        report = self._build_intel_report()
        await self._send(report)
        return True

    @staticmethod
    def _fmt_usd(v: float) -> str:
        """Format a USD value with appropriate suffix."""
        if abs(v) >= 1_000_000_000:
            return f"${v / 1_000_000_000:,.1f}B"
        if abs(v) >= 1_000_000:
            return f"${v / 1_000_000:,.1f}M"
        if abs(v) >= 1_000:
            return f"${v / 1_000:,.1f}K"
        return f"${v:,.0f}"

    @staticmethod
    def _fmt_usd_signed(v: float) -> str:
        """Format a signed USD value with appropriate suffix."""
        sign = "+" if v >= 0 else ""
        if abs(v) >= 1_000_000_000:
            return f"{sign}${v / 1_000_000_000:,.1f}B"
        if abs(v) >= 1_000_000:
            return f"{sign}${v / 1_000_000:,.1f}M"
        if abs(v) >= 1_000:
            return f"{sign}${v / 1_000:,.1f}K"
        return f"{sign}${v:,.0f}"

    def _build_intel_report(self) -> str:
        """Build the 4-hour market intelligence summary."""
        hub = self._hub
        lines = []
        lines.append("📊 HYPERDATA MARKET INTELLIGENCE")
        lines.append("━" * 30)
        lines.append("")

        # ── 1. Top 3 CVD Momentum ──────────────────────────────────
        lines.append("⚡ TOP 3 CVD MOMENTUM")
        try:
            cvd_data = []
            for sym in hub.orderflow.symbols:
                if sym not in hub.orderflow.buckets:
                    continue
                snap = hub.orderflow.get_snapshot(sym, "4h")
                cvd_data.append((sym, snap.cvd, snap.ofi, snap.signal))
            # Sort by absolute OFI descending
            cvd_data.sort(key=lambda x: abs(x[2]), reverse=True)
            for i, (sym, cvd, ofi, signal) in enumerate(cvd_data[:3], 1):
                cvd_str = self._fmt_usd_signed(cvd)
                lines.append(f"{i}. {sym} — CVD: {cvd_str} | OFI: {ofi:+.2f} | {signal}")
            if not cvd_data:
                lines.append("   No data available")
        except Exception:
            lines.append("   Data unavailable")
        lines.append("")

        # ── 2. Liquidations (4h window) ────────────────────────────
        lines.append("💥 LIQUIDATIONS (4h)")
        try:
            liq_stats = hub.liquidations.get_stats(window_minutes=240, include_estimated=False)
            total_count = liq_stats["total_count"]
            total_vol = liq_stats["total_volume_usd"]
            long_c = liq_stats["long_count"]
            long_v = liq_stats["long_volume_usd"]
            short_c = liq_stats["short_count"]
            short_v = liq_stats["short_volume_usd"]

            lines.append(f"Total: {total_count:,} events | {self._fmt_usd(total_vol)} volume")
            lines.append(f"Long: {long_c:,} ({self._fmt_usd(long_v)}) | Short: {short_c:,} ({self._fmt_usd(short_v)})")

            # Find the top single liquidation in the window
            cutoff = time.time() - 240 * 60
            top_liq = None
            for ev in hub.liquidations.events:
                if ev.timestamp < cutoff or not getattr(ev, "confirmed", True):
                    continue
                if top_liq is None or ev.size_usd > top_liq.size_usd:
                    top_liq = ev
            if top_liq:
                lines.append(
                    f"Top: {top_liq.symbol} {top_liq.side.upper()} "
                    f"{self._fmt_usd(top_liq.size_usd)} on {top_liq.exchange.capitalize()}"
                )

            # Exchange breakdown (abbreviated)
            by_ex = liq_stats.get("by_exchange", {})
            if by_ex:
                ex_abbrev = {"binance": "BIN", "bybit": "BYB", "okx": "OKX", "hyperliquid": "HYP"}
                ex_parts = []
                for ex_name, ex_data in sorted(by_ex.items(), key=lambda x: -x[1]["count"]):
                    abbr = ex_abbrev.get(ex_name, ex_name[:3].upper())
                    ex_parts.append(f"{abbr}:{ex_data['count']}")
                lines.append(f"By exchange: {' '.join(ex_parts)}")
        except Exception:
            lines.append("   Data unavailable")
        lines.append("")

        # ── 3. Smart Money ─────────────────────────────────────────
        lines.append("🧠 SMART MONEY")
        try:
            sm_stats = hub.smart_money.get_stats()
            total_wallets = sm_stats["total_wallets"]
            ranked_wallets = sm_stats["ranked_wallets"]
            lines.append(f"Tracked: {total_wallets:,} wallets | Ranked: {ranked_wallets:,}")

            # Top signal from a ranked wallet recently
            recent_signals = hub.smart_money.get_recent_signals(50)
            follow_signals = [s for s in recent_signals if s.signal_type == "follow" and s.wallet_rank > 0]
            if follow_signals:
                # Get the most notable one (lowest rank = best wallet)
                top_sig = min(follow_signals, key=lambda s: s.wallet_rank)
                action_str = top_sig.action.replace("_", " ")
                lines.append(
                    f"Top signal: #{top_sig.wallet_rank} {action_str} "
                    f"{top_sig.symbol} {self._fmt_usd(top_sig.size_usd)}"
                )

            # Compute sentiment: % of follow signals that are bullish
            if follow_signals:
                bullish = sum(1 for s in follow_signals if s.action in ("OPEN_LONG", "CLOSE_SHORT"))
                pct_bull = bullish / len(follow_signals) * 100 if follow_signals else 50
                if pct_bull >= 60:
                    sentiment = "bullish (smart money net long)"
                elif pct_bull <= 40:
                    sentiment = "bearish (smart money net short)"
                else:
                    sentiment = "neutral"
                lines.append(f"Sentiment: {pct_bull:.0f}% bullish ({sentiment})")
            else:
                lines.append("No recent follow signals")
        except Exception:
            lines.append("   Data unavailable")
        lines.append("")

        # ── 4. HLP Vault ──────────────────────────────────────────
        lines.append("🏛️ HLP VAULT")
        try:
            hlp_stats = hub.hlp.get_stats()
            aum = hlp_stats["account_value"]
            delta = hlp_stats["net_delta"]
            zscore = hlp_stats["delta_zscore"]
            session_pnl = hlp_stats["session_pnl"]
            partial = " (partial)" if hlp_stats.get("aum_source") != "vaultDetails" else ""

            lines.append(
                f"AUM: {self._fmt_usd(aum)}{partial} | Delta: {self._fmt_usd_signed(delta)} | Z: {zscore:+.1f}"
            )
            # Unknown is not zero: say so rather than report "+$0".
            pnl_text = self._fmt_usd_signed(session_pnl) if hlp_stats.get("pnl_known") else "unknown"
            lines.append(f"Session PnL: {pnl_text}")

            # Count absorptions in the 4h window
            recent_absorptions = hub.hlp.get_liquidation_absorptions(minutes=240)
            lines.append(f"Absorptions: {len(recent_absorptions)} in 4h")
        except Exception:
            lines.append("   Data unavailable")

        # ── Footer ─────────────────────────────────────────────────
        lines.append("")
        lines.append("━" * 30)
        lines.append("🤖 HyperData Terminal | hyperdata.dev")

        return "\n".join(lines)

    # Deadline for webhook posts: a stalled Telegram/Discord endpoint must
    # not wedge whatever task is delivering the alert.
    _SEND_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=3, sock_connect=3, sock_read=5)

    async def _send(self, message: str) -> None:
        """Send alert to all configured channels."""
        if not self._session:
            self._session = aiohttp.ClientSession()

        self.alerts_sent += 1

        await self.send_telegram(message)

        # Discord
        if self.discord_webhook:
            try:
                async with self._session.post(
                    self.discord_webhook,
                    json={
                        "content": message,
                    },
                    timeout=self._SEND_TIMEOUT,
                ) as resp:
                    if resp.status in (200, 204):
                        logger.info("Discord alert sent")
                    else:
                        logger.warning("Discord send returned %d", resp.status)
            except Exception as exc:
                # Exception type only — error messages can embed the webhook URL.
                logger.warning("Discord send failed: %s", type(exc).__name__)

        # Log that an alert fired, not its payload — alert bodies can contain
        # wallet addresses and position intelligence that must not sit in
        # rotating plaintext logs. Wallet addresses are masked even on the
        # first line in case a future alert format leads with one.
        first_line = message.strip().splitlines()[0] if message.strip() else ""
        redacted = re.sub(r"0x[0-9a-fA-F]{40}", "0x…[redacted]", first_line)
        logger.warning("ALERT sent (%d total): %.80s", self.alerts_sent, redacted)

    async def _telegram_serialized(self, method, *args, **kwargs):
        if not hasattr(self, "_telegram_lock"):
            self._telegram_lock = asyncio.Lock()
        async with self._telegram_lock:
            while time.time() < getattr(self, "_telegram_not_before", 0):
                await asyncio.sleep(min(1, self._telegram_not_before - time.time()))
            result = await method(*args, **kwargs)
            detail = getattr(self, "last_telegram_result", {})
            if detail.get("http_status") == 429:
                self._telegram_not_before = time.time() + max(1, int(detail.get("retry_after") or 60))
            return result

    async def send_telegram(self, message, *, rich_html=None, copy_text=None, return_message=False):
        return await self._telegram_serialized(
            self._send_telegram_unlocked,
            message,
            rich_html=rich_html,
            copy_text=copy_text,
            return_message=return_message,
        )

    async def edit_telegram(self, message_id, message, *, rich_html=None, copy_text=None):
        return await self._telegram_serialized(
            self._edit_telegram_unlocked, message_id, message, rich_html=rich_html, copy_text=copy_text
        )

    async def _send_telegram_unlocked(
        self, message: str, *, rich_html: str | None = None, copy_text: str | None = None, return_message: bool = False
    ):
        """Send a Telegram alert, with HTML fallback on explicit Rich Message rejection."""
        if not self.telegram_token or not self.telegram_chat_id:
            self.last_telegram_result = {"state": "unconfigured"}
            return False
        self.last_telegram_result = {"state": "send_pending"}
        if not self._session:
            self._session = aiohttp.ClientSession()
        plain = {"text": message, "parse_mode": "HTML"}
        if copy_text:
            plain["link_preview_options"] = {"is_disabled": True}
            plain["reply_markup"] = {
                "inline_keyboard": [
                    [
                        {
                            "text": "Копировать фьючерс",
                            "copy_text": {"text": copy_text},
                        }
                    ]
                ]
            }
        attempts = [("sendMessage", plain)]
        if rich_html is not None:
            attempts.insert(0, ("sendRichMessage", {"rich_message": {"html": rich_html}}))
        try:
            for method, payload in attempts:
                url = f"https://api.telegram.org/bot{self.telegram_token}/{method}"
                async with self._session.post(
                    url,
                    json={"chat_id": self.telegram_chat_id, **payload},
                    timeout=self._SEND_TIMEOUT,
                    allow_redirects=False,
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get("ok") is True:
                            message_id = (result.get("result") or {}).get("message_id")
                            if return_message and (type(message_id) is not int or message_id <= 0):
                                self.last_telegram_result = {
                                    "state": "outcome_unknown",
                                    "http_status": 200,
                                    "reason": "missing_message_id",
                                }
                                return False
                            self.last_telegram_result = {"state": "sent", "http_status": 200}
                            logger.info("Telegram alert sent")
                            if return_message:
                                return ((result.get("result") or {}).get("message_id"), method == "sendRichMessage")
                            return True
                    if (
                        method == "sendRichMessage"
                        and response.status in (400, 404)
                        and (await response.json()).get("ok") is False
                    ):
                        logger.info("Telegram rejected rich alert (%d), using HTML", response.status)
                        continue
                    logger.warning("Telegram send returned %d", response.status)
                    body = await response.json()
                    self.last_telegram_result = self._telegram_failure(response.status, body)
                    return False
        except Exception as exc:
            self.last_telegram_result = {"state": "outcome_unknown", "reason": type(exc).__name__}
            logger.warning("Telegram send failed: %s", type(exc).__name__)
        return False

    async def _edit_telegram_unlocked(self, message_id, message, *, rich_html=None, copy_text=None):
        if not self.telegram_token or not self.telegram_chat_id:
            self.last_telegram_result = {"state": "unconfigured"}
            return False
        if not self._session:
            self._session = aiohttp.ClientSession()
        self.last_telegram_result = {"state": "edit_pending"}
        payload = {"chat_id": self.telegram_chat_id, "message_id": message_id}
        if rich_html is not None:
            payload["rich_message"] = {"html": rich_html}
        else:
            payload.update(text=message, parse_mode="HTML", link_preview_options={"is_disabled": True})
            if copy_text:
                payload["reply_markup"] = {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Копировать фьючерс",
                                "copy_text": {"text": copy_text},
                            }
                        ]
                    ]
                }
        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/editMessageText"
            async with self._session.post(
                url, json=payload, timeout=self._SEND_TIMEOUT, allow_redirects=False
            ) as response:
                body = await response.json()
                if response.status == 200 and body.get("ok") is True:
                    self.last_telegram_result = {"state": "edited", "http_status": 200}
                    return True
                if response.status == 400 and "message is not modified" in str(body.get("description", "")).lower():
                    self.last_telegram_result = {"state": "unchanged", "http_status": 400}
                    return True
                self.last_telegram_result = self._telegram_failure(response.status, body)
                return False
        except Exception as exc:
            self.last_telegram_result = {"state": "outcome_unknown", "reason": type(exc).__name__}
            logger.warning("Telegram edit failed: %s", type(exc).__name__)
            return False

    def _telegram_failure(self, status, body):
        description = str(body.get("description", ""))[:500].replace(self.telegram_token, "[redacted]")
        return {
            "state": "failed" if status < 500 else "outcome_unknown",
            "http_status": status,
            "error_code": body.get("error_code"),
            "description": description,
            "retry_after": (body.get("parameters") or {}).get("retry_after"),
        }

    async def send_test(self) -> bool:
        """Send a test alert to verify configuration."""
        if not self.enabled:
            return False
        await self._send("🧪 HyperData Alert Test — Connection verified!")
        return True
