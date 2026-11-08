"""Windows desktop notifications; a click opens the instrument in TigerTrade.

Tiger's local API has no notification command (only setLinkSymbol and ping), so the
notification is a native Windows toast shown by this process. Clicking it sends one
setLinkSymbol to the configured link group, the same loopback-only command as the
Telegram button. Nothing switches without a click; an expired toast does nothing.

Enable with SCALPER_DESKTOP_NOTIFY=1 (Windows, `pip install -e ".[desktop]"`, the winrt-* packages).
SCALPER_TIGER_NOTIFY_GROUP picks the group bound to the trader's main chart/DOM;
it defaults to SCALPER_TIGER_LINK_GROUP.
"""

import asyncio
import html
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from xml.sax.saxutils import escape

import aiohttp

from .tiger import TigerBridge

LOG = logging.getLogger(__name__)
TAG = re.compile(r"<[^>]+>")


def plain(text):
    return html.unescape(TAG.sub("", text)).strip()


def toast_text(card_text):
    """Header as the toast title; the reason and the level as its two body lines."""
    lines = [plain(line) for line in card_text.split("\n") if plain(line)]
    title = lines[0] if lines else ""
    # Skip the muted source footer and the price/cost line: the toast is a prompt to look, not the full card.
    body = [line for line in lines[1:-1] if not line.startswith(("💲", "Цена "))][:2]
    return title, body


# Command Prompt's AUMID is registered on every Windows install, so a click on the toast
# is delivered to the activated handler without registering our own app identity.
AUMID = r"{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\cmd.exe"


class WinRTToaster:
    """Minimal native toast: title + body lines, replace-by-tag, expiry, click callback."""

    def __init__(self):
        from winrt.windows.data.xml.dom import XmlDocument
        from winrt.windows.ui.notifications import ToastNotification, ToastNotificationManager

        self.XmlDocument, self.ToastNotification = XmlDocument, ToastNotification
        self.notifier = ToastNotificationManager.create_toast_notifier_with_id(AUMID)
        self.live = {}  # tag -> toast: keeps the toast and its handler alive until replaced

    def show(self, title, body, tag, expires_at, on_click):
        texts = "".join(f"<text>{escape(t)}</text>" for t in [title, *body])
        document = self.XmlDocument()
        document.load_xml(
            '<toast duration="short"><visual><binding template="ToastGeneric">'
            f'{texts}<text placement="attribution">HyperData screener</text>'
            "</binding></visual></toast>"
        )
        toast = self.ToastNotification(document)
        toast.tag, toast.group = tag, "hyperdata"
        toast.expiration_time = datetime.fromtimestamp(expires_at, timezone.utc)
        toast.add_activated(lambda _sender, _args: on_click())
        self.live[tag] = toast
        self.notifier.show(toast)


class DesktopNotifier:
    def __init__(self, toaster=None):
        self.enabled = os.getenv("SCALPER_DESKTOP_NOTIFY", "0") == "1"
        self.error = None
        self.clicks = 0
        self.shown = 0
        self.last_result = None
        self.tiger = TigerBridge()
        group = os.getenv("SCALPER_TIGER_NOTIFY_GROUP", "").strip().upper()
        if group:
            self.tiger.group = group
        self.loop = None
        self.session = None
        self.toaster = toaster
        if not self.enabled or toaster is not None:
            return
        if sys.platform != "win32":
            self.error = "Desktop notifications need Windows"
            return
        try:
            self.toaster = WinRTToaster()
        except Exception as exc:  # ImportError or WinRT failure
            self.error = f"WinRT notifications unavailable: {type(exc).__name__}"

    @property
    def active(self):
        return self.enabled and self.toaster is not None and self.error is None

    def notify(self, symbol, card_text, expires_at):
        """Shows one toast per instrument (a newer one replaces the older). Never raises."""
        if not self.active:
            return False
        try:
            self.loop = self.loop or asyncio.get_running_loop()
            title, body = toast_text(card_text)
            self.toaster.show(title, body, symbol[-64:], expires_at, lambda: self.clicked(symbol, expires_at))
            self.shown += 1
            return True
        except Exception as exc:
            self.error = f"toast failed: {type(exc).__name__}"
            LOG.warning("Desktop notification failed", exc_info=True)
            return False

    def clicked(self, symbol, expires_at):
        """Called on a WinRT thread: hand the open over to the event loop."""
        if time.time() > expires_at or self.loop is None or self.loop.is_closed():
            self.last_result = "Уведомление устарело"
            return
        self.clicks += 1
        asyncio.run_coroutine_threadsafe(self.open(symbol), self.loop)

    async def open(self, symbol):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        self.last_result = await self.tiger.open(self.session, symbol)
        return self.last_result

    async def close(self):
        if self.session is not None and not self.session.closed:
            await self.session.close()

    def status(self):
        return dict(
            enabled=self.enabled,
            active=self.active,
            error=self.error,
            tiger_group=self.tiger.group or None,
            shown=self.shown,
            clicks=self.clicks,
            last_result=self.last_result,
        )
