"""Live evidence, fair observation leases and reproducible policy migration."""

from dataclasses import asdict

import pytest

from extensions.scalper.context_auto import decide
from extensions.scalper.history.archive import Archive
from extensions.scalper.history.replay import replay
from extensions.scalper.relative_attention import Attention, AttentionPolicy
from extensions.scalper.unified_attention import UnifiedAttention


def engine(**kwargs):
    return Attention(AttentionPolicy(version=5, probe_seconds=90, **kwargs))


def observation(now, *, assigned=100, price=1, spread=1, healthy=True, trade=None, epoch=1):
    return dict(price=price, volume_24h=0, candle_ratio=20, detail=dict(
        assigned_at=assigned, received_at=now, last_trade_received_at=now if trade is None else trade,
        midpoint=price, spread_bps=spread, healthy=healthy, epoch=epoch, trade_window_truncated=False))


def confirmed(attention):
    attention.update({"NEW": observation(100)}, 100)
    for now in (105, 115, 125):
        attention.update({"NEW": observation(now, price=1 + (now - 100) / 10000)}, now)


def test_extreme_candle_ratio_only_probes_and_scores_share_bounded_scale():
    attention = engine()
    attention.update({"NEW": dict(price=1, volume_24h=0, candle_ratio=1e200)}, 100)
    assert attention.active["NEW"]["state"] == "probe"
    assert "context_until" not in attention.active["NEW"]
    assert 0 <= attention.scouts["NEW"]["score"] <= 12


def test_real_continuous_flow_and_price_response_reserve_full_context():
    attention = engine()
    confirmed(attention)
    row = attention.active["NEW"]
    assert row["state"] == "focus"
    assert row["quality"]["confirmed"]
    assert row["context_until"] == 725


@pytest.mark.parametrize("problem", ["wide_spread", "same_trade", "wrong_episode", "unhealthy", "no_reaction"])
def test_broad_anomaly_cannot_replace_live_confirmation(problem):
    attention = engine()
    attention.update({"NEW": observation(100)}, 100)
    for now in (105, 115, 125):
        row = observation(now, price=1 + (now - 100) / 10000)
        if problem == "wide_spread":
            row["detail"]["spread_bps"] = 100
        elif problem == "same_trade":
            row["detail"]["last_trade_received_at"] = 105
        elif problem == "wrong_episode":
            row["detail"]["assigned_at"] = 99
        elif problem == "unhealthy":
            row["detail"]["healthy"] = False
        else:
            row["detail"]["midpoint"] = 1
        attention.update({"NEW": row}, now)
    assert attention.active["NEW"]["state"] == "probe"


def test_quiet_protected_slot_is_released_and_not_immediately_reacquired():
    attention = engine()
    confirmed(attention)
    for now in (140, 156):
        attention.update({"NEW": observation(now, healthy=False)}, now)
    assert "NEW" not in attention.active
    assert attention.transitions[-1]["reason"] == "live_quality_lost"
    attention.update({"NEW": observation(160)}, 160)
    assert "NEW" not in attention.active


def test_new_candidates_cannot_preempt_probe_before_usable_observation():
    attention = engine(slots=1)
    attention.update({"FIRST": observation(100)}, 100)
    for now in (120, 145, 170):
        attention.update({"FIRST": dict(price=1, volume_24h=0),
                          "OTHER": dict(price=1, volume_24h=0, candle_ratio=1000)}, now)
        assert list(attention.active) == ["FIRST"]
    attention.update({"FIRST": dict(price=1, volume_24h=0),
                      "OTHER": dict(price=1, volume_24h=0, candle_ratio=1000)}, 190)
    assert list(attention.active) == ["OTHER"]


def test_transport_epoch_change_cannot_join_price_and_flow_history():
    attention = engine()
    attention.update({"NEW": observation(100)}, 100)
    for now in (105, 115, 125):
        attention.update({"NEW": observation(now, price=1 + (now - 100) / 10000,
                                             epoch=2 if now == 125 else 1)}, now)
    assert attention.active["NEW"]["state"] == "probe"


def test_rms_baseline_handles_mostly_zero_movement_without_infinite_priority():
    attention = engine(baseline_samples=3)
    for now, price in ((100, 1), (105, 1), (110, 1.0001), (115, 1.0001), (120, 1.0001)):
        attention.update({"NEW": dict(price=price, volume_24h=0)}, now)
    attention.update({"NEW": dict(price=1.0002, volume_24h=0)}, 125)
    assert attention.scouts["NEW"]["movement_ratio"] < 3
    assert not attention.active


def test_last_trade_discovery_price_is_used_instead_of_mark_drift():
    selector = UnifiedAttention(None)
    selector.choose([dict(symbol="NEW", price=1, mark_price=10)], 100)
    assert selector.engine.previous["NEW"][1] == 1


def test_new_policy_replays_evidence_and_transitions_exactly(tmp_path):
    attention = engine()
    archive = Archive(tmp_path)
    archive.record("policy", asdict(attention.policy), 99)
    expected = []
    for now in (100, 105, 115, 125, 140, 156):
        rows = {"NEW": observation(now, price=1 + (now - 100) / 10000, healthy=now < 140)}
        changes = attention.update(rows, now)
        archive.record("scout", rows, now)
        for change in changes:
            archive.record("assignment", change, now)
        expected.extend(changes)
    archive.close()
    assert replay(archive.path)["assignments"] == expected


def test_archive_freezes_mutable_attention_state_before_background_indexing(tmp_path):
    archive = Archive(tmp_path)
    state = {"active": {"NEW": {"detail_samples": [[100, 1, 100, 1]]}}}
    frozen = archive.record("attention_state", state, 100)
    state["active"]["NEW"]["detail_samples"].clear()
    archive.close()
    assert frozen["payload"]["active"]["NEW"]["detail_samples"] == [[100, 1, 100, 1]]


def test_unconfirmed_probe_cannot_trigger_automatic_interpretation():
    from extensions.scalper.fact_packet import build_packet
    from tests.test_context_auto import attention as context_fixture

    source = context_fixture()
    source["policy_version"] = 5
    assignment = source["active"][0]
    assignment["state"] = "probe"
    packet = build_packet(source, assignment, 100)
    priority, reason, _ = decide(packet, [])
    assert priority is None
    assert reason == "live_quality_unconfirmed"


@pytest.mark.asyncio
async def test_native_feedback_newer_than_market_snapshot_can_confirm(monkeypatch):
    from unittest.mock import AsyncMock, Mock

    from extensions.scalper.remote import RemoteHub

    selector = UnifiedAttention(None)
    selector.engine.update({"NEW": observation(100)}, 100)
    for now in (105, 115):
        selector.engine.update({"NEW": observation(now, price=1 + (now - 100) / 10000)}, now)
    detail = observation(125, price=1.0025)["detail"]
    detail["received_at"] = 124.5  # Newer than market as_of=124, older than decision=125.
    source = dict(enabled=True, degraded=False, active=[dict(
        symbol="NEW", state="probe", assigned_at=100, ready=True, facts=detail)])
    market = dict(as_of=124, assets=[dict(symbol="NEW", price=1.0025, mark_price=1,
                                        observed_at=124, observed_fields=["price"])])
    hub = RemoteHub("http://unused", demo=True)
    hub.attention_selector = selector
    hub._get = AsyncMock(side_effect=[source, market, {}])
    hub._apply = Mock()
    hub._refresh_health = AsyncMock()
    monkeypatch.setattr("extensions.scalper.remote.time.time", lambda: 125)
    await hub.refresh_snapshot()
    assert selector.engine.active["NEW"]["state"] == "focus"
    assert selector.engine.active["NEW"]["quality"]["fresh"]
