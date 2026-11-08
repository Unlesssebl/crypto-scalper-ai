"""Liquidity tier × signal strength decides who gets interrupted, and how."""

import time
from types import SimpleNamespace

import pytest

from extensions.scalper.telegram_ux import liquidity_tier, route, signal_strength
from tests.test_telegram_product import observe, operations
from tests.test_telegram_product import service as service

ABSORPTION = dict(kind="absorption", side=1)


@pytest.mark.parametrize(
    "volume, turnover, expected",
    [
        (500e6, None, (1, "24h")),
        (62e6, 1_000, (2, "24h")),  # 24h wins over a quiet minute
        (8e6, None, (3, "24h")),
        (541e3, 1_537, (4, "24h")),  # FLUX: $541k/24h is not tradable for a scalper
        (None, 50e3, (2, "60s")),  # fallback when 24h is unknown
        (None, None, (4, "unknown")),  # unknown liquidity never interrupts
    ],
)
def test_liquidity_tier(volume, turnover, expected):
    assert liquidity_tier(volume, turnover) == expected


def test_strength_scale():
    big = dict(ABSORPTION, prior=dict(move_bps=30))
    assert signal_strength(dict(tape=big, natr_1m=0.3), discovery=True) == "strong"  # 0.30% ≥ half of 0.3%
    # EVAA 08.11 15:44: +0.11% impulse in a 0.3% minute is not worth a sound, however clean the reading.
    assert signal_strength(dict(tape=dict(ABSORPTION, prior=dict(move_bps=11.4)), natr_1m=0.3), True) == "notable"
    # Volatility unknown: a 0.4% impulse is the floor (ERA +0.71% stays strong, EVAA +0.11% does not).
    assert signal_strength(dict(tape=big), discovery=True) == "notable"
    assert signal_strength(dict(tape=dict(ABSORPTION, prior=dict(move_bps=71.4))), discovery=True) == "strong"
    assert signal_strength(dict(phase="confirmed", formation_strength=3), discovery=False) == "strong"
    assert signal_strength(dict(phase="confirmed", formation_strength=2), discovery=False) == "notable"
    assert signal_strength(dict(flow=0.8, count=40, ratio=4), discovery=True) == "notable"
    assert signal_strength(dict(flow=0.8, count=40, ratio=1.7), discovery=True) == "background"


@pytest.mark.parametrize(
    "tier, strength, expected",
    [
        (1, "strong", "alert"), (2, "strong", "alert"), (3, "strong", "silent"), (4, "strong", "watch"),
        (1, "notable", "silent"), (2, "notable", "silent"), (3, "notable", "watch"),
        (1, "background", "watch"),
    ],
)
def test_route_matrix(tier, strength, expected):
    assert route(tier, strength) == expected


class Toasts:
    def __init__(self):
        self.shown = []

    def notify(self, symbol, text, expires_at):
        self.shown.append(symbol)

    def status(self):
        return {}


def test_illiquid_instrument_stays_in_the_watch_list(service):
    service.hub.market.assets["BTC"] = SimpleNamespace(volume_24h=541e3)
    observe(service, time.time())
    assert not operations(service)


def test_notable_liquid_card_is_silent_and_has_no_toast(service):
    service.ux.desktop = Toasts()
    observe(service, time.time())
    row = operations(service)[0]
    assert row["silent"] == 1 and row["text"].startswith("<b>🔸 T1 ")
    assert service.ux.desktop.shown == []


def test_strong_liquid_card_alerts_with_sound_and_toast(service, monkeypatch):
    import extensions.scalper.telegram_ux as ux

    monkeypatch.setattr(ux, "signal_strength", lambda state, discovery: "strong")
    service.ux.desktop = Toasts()
    observe(service, time.time())
    row = operations(service)[0]
    assert row["silent"] == 0 and row["text"].startswith("<b>🔥 T1 ")
    assert service.ux.desktop.shown == ["BTC"]
