"""Desktop toasts: shown for admitted cards, a click opens the instrument in Tiger."""

import asyncio
import time
from types import SimpleNamespace

import pytest

from extensions.scalper.desktop import DesktopNotifier, toast_text

CARD = (
    "<b>▲ AIOT · ранний импульс · +1.08%/1м</b>\n"
    "Покупки 63% · 951 сделок за 1м\n"
    "🧱 Спрос 0.1345 · -0.12% · $45.3k · ≈19с оборота\n"
    "Цена 0.134665 · 🟢 спред 3.7 б.п. = 4% NATR\n"
    "<i>Binance · активный поток сделок · 12:33:03</i>"
)


class FakeToaster:
    def __init__(self):
        self.toasts = []

    def show(self, title, body, tag, expires_at, on_click):
        self.toasts.append(SimpleNamespace(title=title, body=body, tag=tag, click=on_click))


@pytest.fixture
def notifier(monkeypatch):
    monkeypatch.setenv("SCALPER_DESKTOP_NOTIFY", "1")
    monkeypatch.setenv("SCALPER_TIGER_LINK_GROUP", "S")
    monkeypatch.setenv("SCALPER_TIGER_NOTIFY_GROUP", "a")
    return DesktopNotifier(toaster=FakeToaster())


def test_toast_keeps_verdict_reason_and_level_only():
    title, body = toast_text(CARD)
    assert title == "▲ AIOT · ранний импульс · +1.08%/1м"
    assert body == ["Покупки 63% · 951 сделок за 1м", "🧱 Спрос 0.1345 · -0.12% · $45.3k · ≈19с оборота"]


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("SCALPER_DESKTOP_NOTIFY", raising=False)
    off = DesktopNotifier(toaster=FakeToaster())
    assert not off.active and off.notify("BINANCE:AIOT", CARD, time.time() + 60) is False


@pytest.mark.asyncio
async def test_click_opens_symbol_in_the_notify_group(notifier, monkeypatch):
    opened = []

    async def fake_open(session, symbol):
        opened.append((symbol, notifier.tiger.command(symbol)["linkGroup"]))
        return "ok"

    monkeypatch.setattr(notifier.tiger, "open", fake_open)
    assert notifier.notify("BINANCE:AIOT", CARD, time.time() + 60)
    toast = notifier.toaster.toasts[0]
    assert toast.tag == "BINANCE:AIOT" and toast.title.startswith("▲ AIOT") and len(toast.body) == 2
    # WinRT calls the activated handler from its own thread.
    await asyncio.to_thread(toast.click)
    for _ in range(50):
        if opened:
            break
        await asyncio.sleep(0.01)
    assert opened == [("BINANCE:AIOT", "A")]
    assert notifier.status()["clicks"] == 1 and notifier.last_result == "ok"
    await notifier.close()


@pytest.mark.asyncio
async def test_expired_toast_click_does_nothing(notifier, monkeypatch):
    async def fail(*_):
        raise AssertionError("must not open")

    monkeypatch.setattr(notifier.tiger, "open", fail)
    notifier.notify("BINANCE:AIOT", CARD, time.time() - 1)
    notifier.toaster.toasts[0].click()
    await asyncio.sleep(0.05)
    assert notifier.clicks == 0 and notifier.last_result == "Уведомление устарело"


@pytest.mark.asyncio
async def test_toast_failure_is_reported_not_raised(notifier):
    class Broken:
        def show(self, *args):
            raise OSError("no notification platform")

    notifier.toaster = Broken()
    assert notifier.notify("BINANCE:AIOT", CARD, time.time() + 60) is False
    assert notifier.status()["error"] == "toast failed: OSError"
