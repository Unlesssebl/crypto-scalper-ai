"""
Notification transport — delivers messages to Telegram and Discord webhooks.

Automatic event alerts (liquidations, smart money, HLP Z-score) are not sent:
the screener owns those decisions and posts through the same transport.

Usage:
    alerts = AlertManager()
    await alerts.start()
    await alerts.send_telegram("...")

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

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class AlertManager:
    def __init__(self):
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.discord_webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
        self._session: aiohttp.ClientSession | None = None
        self.enabled = bool(self.telegram_token and self.telegram_chat_id) or bool(self.discord_webhook)
        self.alerts_sent = 0

        # Hub reference for on-demand market intelligence reports.
        self._hub = None

        if not self.enabled:
            logger.info("AlertManager: No TELEGRAM_BOT_TOKEN/CHAT_ID or DISCORD_WEBHOOK_URL set. Alerts disabled.")
            logger.info("AlertManager: Set env vars to enable. See .env.example")

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()

    async def stop(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def attach(self, hub) -> None:
        """Attach the hub used by on-demand market intelligence reports."""
        self._hub = hub

    # ── Market Intelligence ─────────────────────────────────────────

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

    async def send_telegram(
        self,
        message,
        *,
        rich_html=None,
        copy_text=None,
        return_message=False,
        chat_id=None,
        reply_markup=None,
        silent=False,
    ):
        return await self._telegram_serialized(
            self._send_telegram_unlocked,
            message,
            rich_html=rich_html,
            copy_text=copy_text,
            return_message=return_message,
            chat_id=chat_id,
            reply_markup=reply_markup,
            silent=silent,
        )

    async def edit_telegram(
        self, message_id, message, *, rich_html=None, copy_text=None, chat_id=None, reply_markup=None
    ):
        return await self._telegram_serialized(
            self._edit_telegram_unlocked,
            message_id,
            message,
            rich_html=rich_html,
            copy_text=copy_text,
            chat_id=chat_id,
            reply_markup=reply_markup,
        )

    async def _send_telegram_unlocked(
        self,
        message: str,
        *,
        rich_html: str | None = None,
        copy_text: str | None = None,
        return_message: bool = False,
        chat_id=None,
        reply_markup=None,
        silent=False,
    ):
        """Send a Telegram alert, with HTML fallback on explicit Rich Message rejection."""
        destination = self.telegram_chat_id if chat_id is None else chat_id
        if not self.telegram_token or not destination:
            self.last_telegram_result = {"state": "unconfigured"}
            return False
        self.last_telegram_result = {"state": "send_pending"}
        if not self._session:
            self._session = aiohttp.ClientSession()
        plain = {"text": message, "parse_mode": "HTML", "link_preview_options": {"is_disabled": True}}
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
                    json={
                        "chat_id": destination,
                        **({"disable_notification": True} if silent else {}),
                        **payload,
                        **({"reply_markup": reply_markup} if reply_markup is not None else {}),
                    },
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

    async def _edit_telegram_unlocked(
        self, message_id, message, *, rich_html=None, copy_text=None, chat_id=None, reply_markup=None
    ):
        destination = self.telegram_chat_id if chat_id is None else chat_id
        if not self.telegram_token or not destination:
            self.last_telegram_result = {"state": "unconfigured"}
            return False
        if not self._session:
            self._session = aiohttp.ClientSession()
        self.last_telegram_result = {"state": "edit_pending"}
        payload = {"chat_id": destination, "message_id": message_id}
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
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
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
