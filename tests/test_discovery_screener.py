"""The screener finds instruments to study; price follow-through is optional."""

from dataclasses import asdict

from extensions.scalper.context_auto import decide, fingerprint
from extensions.scalper.fact_packet import build_packet
from extensions.scalper.history.archive import Archive
from extensions.scalper.history.replay import replay
from extensions.scalper.relative_attention import Attention, AttentionPolicy
from tests.test_attention_quality import observation
from tests.test_telegram_product import operations
from tests.test_telegram_product import service as service


def discovery_packet(now=100, *, live=False, potential=True, healthy=True, spread=20):
    card = dict(status="observing", received_at=now, midpoint=1, spread_bps=spread,
                observed_levels=[20, 20], evidence="run:book", densities=[],
                observed_trade_count_60s=3 if live else 0, observed_trade_imbalance=0,
                observed_turnover_60s_usd=10 if live else 0,
                last_trade_received_at=now if live else None, trade_window_truncated=False)
    assignment = dict(symbol="NEW", assigned_at=90, state="focus" if live else "probe", score=7,
                      facts=card, quality=dict(confirmed=live, price_reaction_confirmed=False),
                      interest=dict(at=now, potential=potential, reason="closed_candle_activity", score=7,
                                    candle_ratio=8 if potential else 1, movement_ratio=0))
    return build_packet(dict(policy_version=6, last_scout_at=now,
                             plan_error=None if healthy else "source_unavailable"), assignment, now)


def test_busy_flat_market_becomes_focus_without_price_reaction():
    attention = Attention(AttentionPolicy(version=6, probe_seconds=90))
    for now in (100, 105, 115, 125):
        attention.update({"NEW": observation(now, price=1, spread=100)}, now)
    row = attention.active["NEW"]
    assert row["state"] == "focus"
    assert row["quality"]["confirmed"]
    assert not row["quality"]["price_reaction_confirmed"]
    assert attention.transitions[-1]["reason"] == "live_flow_confirmed"


def test_unusual_candle_is_a_candidate_before_any_detailed_trade():
    p = discovery_packet()
    priority, reason, state = decide(p, [])
    assert priority is not None and reason == "interesting_instrument"
    assert state["count"] is None and state["discovery_kind"] == "potential"
    assert not p["coverage"]["price_reaction_confirmed"]


def test_balanced_live_flow_is_interesting_without_direction_or_twenty_trades():
    priority, reason, state = decide(discovery_packet(live=True, potential=False), [])
    assert priority is not None and reason == "active_instrument"
    assert state["flow"] == 0 and state["count"] == 3


def test_fresh_ordinary_instrument_is_quiet():
    priority, reason, _ = decide(discovery_packet(potential=False), [])
    assert priority is None and reason == "discovery_unconfirmed"


def test_stable_unusual_zone_can_be_interesting_before_flow_starts():
    from extensions.scalper.fact_packet import checksum
    from tests.test_telegram_product import packet

    history = []
    for now in (100, 110):
        p = packet(now, flow=0, count=0)
        p["coverage"].update(attention_policy_version=6, live_quality_confirmed=False,
                             discovery=dict(potential=False, score=0))
        p.pop("packet_id")
        p["packet_id"] = checksum(p)
        priority, reason, state = decide(p, history)
        history.append(state)
    assert priority is not None and reason == "interesting_level"


def test_discovery_still_requires_healthy_sources():
    priority, reason, _ = decide(discovery_packet(healthy=False), [])
    assert priority is None and reason == "insufficient_context"


def test_only_material_change_repeats_and_active_is_an_upgrade():
    p = discovery_packet()
    _, _, baseline = decide(p, [])
    assert decide(p, [], baseline)[0] is None
    priority, reason, active = decide(discovery_packet(live=True), [], baseline)
    assert priority is not None and reason == "active_instrument"
    assert fingerprint(active) != fingerprint(baseline)


def test_one_price_burst_does_not_immediately_announce_a_candidate():
    attention = Attention(AttentionPolicy(version=6, baseline_samples=3, probe_seconds=90))
    for now, price in ((100, 1), (105, 1.0001), (110, 1.0002), (115, 1.0003), (120, 1.1)):
        attention.update({"NEW": dict(price=price, volume_24h=0)}, now)
    assert attention.active["NEW"]["interest"]["potential"] is False
    attention.update({"NEW": dict(price=1.5, volume_24h=0)}, 130)
    assert attention.active["NEW"]["interest"]["potential"] is True


def test_candidate_notification_does_not_need_ai_or_a_tight_spread(service):
    p = discovery_packet(spread=100)
    service.context_memory.observe(p)
    service.context_packets = {"NEW": p}
    service.ux.observe(100)
    rows = operations(service)
    assert len(rows) == 1
    assert "Кандидат для наблюдения" in rows[0]["text"]
    assert "8×" in rows[0]["text"]
    assert service.journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == 0
    service.ux.observe(101)
    assert len(operations(service)) == 1


def test_activation_can_update_candidate_before_two_minute_cooldown(service):
    p = discovery_packet()
    service.context_memory.observe(p)
    service.context_packets = {"NEW": p}
    service.ux.observe(100)
    active = discovery_packet(125, live=True)
    service.context_memory.observe(active)
    service.context_packets = {"NEW": active}
    service.ux.observe(125)
    rows = operations(service)
    assert len(rows) == 2
    assert any("Инструмент активен" in row["text"] for row in rows)


def test_discovery_policy_and_interest_replay_exactly(tmp_path):
    attention = Attention(AttentionPolicy(version=6, probe_seconds=90))
    archive = Archive(tmp_path)
    archive.record("policy", asdict(attention.policy), 99)
    expected = []
    for now in (100, 105, 115, 125):
        rows = {"NEW": observation(now, price=1)}
        changes = attention.update(rows, now)
        archive.record("scout", rows, now)
        for change in changes:
            archive.record("assignment", change, now)
        expected.extend(changes)
    archive.close()
    assert replay(archive.path)["assignments"] == expected
