"""Declared runtime settings must affect admission and attention."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from apps.screener_web.__main__ import update_policy
from extensions.scalper.ai_routes import AIRoutes
from extensions.scalper.journal import Journal
from extensions.scalper.service import Screener
from extensions.scalper.unified_attention import UnifiedAttention


@pytest.mark.parametrize("requested,effective", [(300, 330), (450, 450), (600, 600), (900, 900)])
def test_context_hold_uses_configured_duration_and_discloses_minimum(requested, effective):
    selector = UnifiedAttention(None, hold_seconds=requested)
    now = 1_000_000
    target = int((now * 1000 - 5000) // 60000 * 60000 - 60000)
    selector.activity = {"NEW": dict(status="ready", start_ms=target, ratios=dict(trade_count=8))}
    selector.choose([dict(symbol="NEW", mark_price=1)], now)
    assert selector.engine.active["NEW"]["state"] == "probe"
    assert "context_until" not in selector.engine.active["NEW"]
    for elapsed in (5, 15, 25):
        selector.feedback = {"NEW": dict(healthy=True, assigned_at=now, received_at=now + elapsed,
                                        last_trade_received_at=now + elapsed, midpoint=1 + elapsed / 10000,
                                        spread_bps=1, trade_window_truncated=False, epoch=1)}
        selector.choose([dict(symbol="NEW", mark_price=1)], now + elapsed)
    assert selector.engine.active["NEW"]["context_until"] == now + 25 + effective
    assert selector.plan["hold_seconds"] == effective
    assert selector.plan["requested_hold_seconds"] == requested


@pytest.mark.parametrize("config", [dict(slots=2, detail_slots=3), dict(slots=21),
                                  dict(detail_slots=9), dict(hold_seconds=59), dict(hold_seconds=3601)])
def test_invalid_resource_settings_rejected(config):
    with pytest.raises(ValueError, match="attention requires"):
        UnifiedAttention(None, **config)


def test_paid_and_free_routes_respect_episode_cooldown_and_lower_budget(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    try:
        routes = AIRoutes(journal, SimpleNamespace(session=AsyncMock(), key="", model="test"))
        state = dict(flow=.8, relation="aligned", cross="aligned", zone=None)
        changed = {**state, "flow": -.8}
        for route in ("free", "strong"):
            packet = dict(episode_id=route, symbol="NEW")
            assert routes.reserve(packet, route, state, 1000, 1, cooldown_seconds=600)[0]
            assert routes.reserve(packet, route, changed, 1400, 1, cooldown_seconds=600)[1] == "cooldown"
        assert routes.reserve(dict(episode_id="other", symbol="OTHER"), "meta", state, 1400, 1)[1] == "hourly_limit"
        assert routes.summary(1400, limit=1, cooldown_seconds=600)["paid_hour_limit"] == 1
        assert routes.reserve(dict(episode_id="free", symbol="NEW"), "free", changed,
                              1601, 1, cooldown_seconds=600)[0]
    finally:
        journal.close()


def test_restart_keeps_saved_lower_ai_budget(tmp_path, monkeypatch):
    monkeypatch.setenv("SCALPER_AI_HOURLY_LIMIT", "40")
    monkeypatch.setenv("PULSE_ATTENTION_HOLD_SECONDS", "600")
    journal = Journal(tmp_path / "journal.sqlite")
    try:
        journal.save_policy(dict(mode="balanced", hourly_limit=2, cooldown_seconds=600, early=True, experiments=True))
        service = Screener(SimpleNamespace(demo=False), journal,
                           SimpleNamespace(session=AsyncMock(), key="", model="test"), False)
        assert service.policy.hourly_limit == 2
        assert service.policy.cooldown_seconds == 600
        assert service.policy.experiments is False
    finally:
        journal.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("requested,effective", [(2, 2), (100, 40)])
async def test_panel_saves_and_returns_effective_paid_limit(tmp_path, requested, effective):
    journal = Journal(tmp_path / "journal.sqlite")
    try:
        policy = dict(mode="balanced", hourly_limit=requested, cooldown_seconds=600, early=True, experiments=True)
        service = SimpleNamespace(journal=journal, ai_routes=object())
        request = SimpleNamespace(content_type="application/json", headers={}, json=AsyncMock(return_value=policy),
                                  app={"service": service})
        response = await update_policy(request)
        assert response.status == 200
        assert journal.load_policy()["hourly_limit"] == service.policy.hourly_limit == effective
        assert journal.load_policy()["experiments"] is False
    finally:
        journal.close()
