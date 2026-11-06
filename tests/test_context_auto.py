"""Automatic interpretation admission uses episode changes, not polling or direction votes."""
import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from extensions.scalper.context_auto import ContextMemory, decide, enrich, relation, summarize
from extensions.scalper.fact_packet import build_packet, verify_packet
from extensions.scalper.fact_review import FactReviewer, schema, validate_review
from extensions.scalper.history.facts import Facts
from extensions.scalper.journal import Journal
from extensions.scalper.service import Screener


def attention(now=100, size=1000, zone=True, relative=8):
    densities = [dict(side="bid", price=9.99, size=size, notional_usd=size * 9.99,
                      relative_size=relative, observed_age_seconds=now - 60, size_change=0,
                      distance_bps=10, size_to_observed_turnover=1, evidence="run:book")] if zone else []
    card = dict(status="observing", received_at=now, midpoint=10, spread_bps=2, observed_levels=[20, 20],
                densities=densities, evidence="run:book", observed_turnover_60s_usd=100,
                trade_evidence=["run:trade"])
    return dict(context_only=True, enabled=True, archive=dict(run="run", error=None), last_scout_at=now,
                active=[dict(symbol="SMALL", state="focus", assigned_at=0, score=6, facts=card)])


def packet(now=100, **kwargs):
    a = attention(now, **kwargs)
    return build_packet(a, a["active"][0], now)


@pytest.fixture
def service(tmp_path, monkeypatch):
    clock = SimpleNamespace(now=100.)
    journal = Journal(tmp_path / "journal.sqlite")
    hub = SimpleNamespace(attention=attention(), demo=False)
    analyst = SimpleNamespace(key="configured", provider="openrouter", model="test/model")
    service = Screener(hub, journal, analyst, False)
    monkeypatch.setattr("extensions.scalper.service.time", SimpleNamespace(time=lambda: clock.now))
    service.context_memory.observe(packet(90))
    async def answer(p):
        return dict(status="ok", model="test/model", raw=json.dumps(dict(packet_id=p["packet_id"],
            observations=[dict(id="book", fact_id="book.midpoint", value=10)], interpretations=[],
            contradictions=[], missing_data=[], abstention=False)))
    call = AsyncMock(side_effect=answer)
    monkeypatch.setattr(FactReviewer, "review", call)
    yield service, clock, call
    journal.close()


@pytest.mark.asyncio
async def test_real_worker_dispatches_once_and_restart_preserves_semantic_dedup(service):
    s, clock, call = service
    result = await s.auto_context_once()
    assert result[0]["status"] == "ok" and call.await_count == 1
    row = s.journal.db.execute("SELECT origin,trigger_reason FROM fact_reviews").fetchone()
    assert tuple(row) == ("automatic", "density_context")
    assert s.journal.db.execute("SELECT purpose FROM ai_requests").fetchone()[0] == "context_auto"
    assert result[0]["packet"]["selection"]["reason"] == "density_context"
    assert any(f["role"] == "history" for f in result[0]["packet"]["facts"])
    clock.now = 500
    s.hub.attention = attention(500)
    restarted = Screener(s.hub, s.journal, s.gemini, False)
    assert not await restarted.auto_context_once()
    assert call.await_count == 1


@pytest.mark.asyncio
async def test_material_zone_change_can_trigger_after_cooldown_but_not_time_or_price(service):
    s, clock, call = service
    await s.auto_context_once()
    clock.now = 500
    s.hub.attention = attention(500)
    s.hub.attention["active"][0]["facts"]["midpoint"] = 10.1
    assert not await s.auto_context_once()
    clock.now = 510
    s.hub.attention = attention(510, size=2000)
    result = await s.auto_context_once()
    assert result[0]["review"]["trigger_reason"] == "density_changed"
    assert call.await_count == 2


@pytest.mark.asyncio
async def test_automatic_telegram_uses_compact_rich_and_html_fallback(service):
    s, clock, call = service
    s.notify = True
    s.hub.alerts = SimpleNamespace(send_telegram=AsyncMock())
    await s.auto_context_once()
    s.hub.alerts.send_telegram.assert_not_awaited()
    clock.now = 500
    s.hub.attention = attention(500)
    assert not await s.auto_context_once()

    async def answer(p):
        return dict(status="ok", raw=json.dumps(dict(packet_id=p["packet_id"],
            observations=[dict(id="book", fact_id="book.midpoint", value=10)],
            interpretations=[dict(id="idea", kind="other", evidence=["book.midpoint"],
                                  text="Возможна связь с видимой структурой книги.")],
            contradictions=[], missing_data=[], abstention=False)))
    call.side_effect = answer
    clock.now = 510
    s.hub.attention = attention(510, size=2000)
    outcomes = await s.auto_context_once()
    assert outcomes and outcomes[0]["status"] == "ok", outcomes
    assert any(c["status"] == "hypothesis" for c in outcomes[0]["review"]["result"]["claims"]), outcomes
    s.hub.alerts.send_telegram.assert_awaited_once()
    args, kwargs = s.hub.alerts.send_telegram.await_args
    assert "Середина спреда: 10" in args[0]
    assert "Гипотезы AI · не доказаны" in args[0]
    assert kwargs["rich_html"] == "<p>" + args[0].replace("\n", "<br>") + "</p>"


@pytest.mark.asyncio
@pytest.mark.parametrize("block", ["no_api_key", "hourly_limit", "disabled"])
async def test_preflight_blocks_without_spending_or_creating_provider_attempts(service, block):
    s, clock, call = service
    if block == "no_api_key":
        s.gemini.key = ""
    elif block == "hourly_limit":
        s.policy = replace(s.policy, hourly_limit=0)
    else:
        s.context_auto_enabled = False
    await s.auto_context_once()
    await s.auto_context_once()
    call.assert_not_awaited()
    assert s.journal.db.execute("SELECT COUNT(*) FROM fact_reviews").fetchone()[0] == 0
    decision = s.context_memory.latest(packet()["episode_id"])
    assert decision["reason"] == block and decision["status"] == "skipped"
    assert s.journal.db.execute("SELECT checks FROM context_decisions").fetchone()[0] == 2


@pytest.mark.asyncio
async def test_network_error_is_recorded_and_identical_context_is_not_blindly_retried(service):
    s, clock, call = service
    call.side_effect = None
    call.return_value = dict(status="network_error")
    result = await s.auto_context_once()
    assert result[0]["status"] == "network_error"
    clock.now = 500
    s.hub.attention = attention(500)
    assert not await s.auto_context_once()
    assert call.await_count == 1
    assert s.consistency.report(501)["api_attempts"] == 1


@pytest.mark.asyncio
async def test_manual_and_auto_share_semantic_state_and_symbol_cooldown(service):
    s, clock, call = service
    s.refresh_context_packets(100)
    await s.review_context("SMALL")
    assert not await s.auto_context_once()
    clock.now = 110
    s.hub.attention = attention(110, size=2000)
    assert not await s.auto_context_once()
    assert call.await_count == 1
    assert s.context_memory.latest(packet()["episode_id"])["reason"] == "cooldown"


@pytest.mark.asyncio
async def test_no_call_for_neutral_or_unstable_context_and_book_gap_does_not_crash(service):
    s, clock, call = service
    s.hub.attention = attention(100, zone=False)
    assert not await s.auto_context_once()
    s.hub.attention["active"][0]["facts"] = dict(status="waiting_for_fresh_book", densities=[])
    assert not await s.auto_context_once()
    call.assert_not_awaited()


def test_single_snapshot_is_not_sufficient_and_future_history_cannot_confirm():
    p = packet()
    assert decide(p, [])[0] is None
    future = summarize(packet(110))
    assert decide(p, [future])[0] is None
    assert decide(p, [summarize(packet(90))])[1] == "density_context"


def test_early_flow_needs_independent_book_support_before_full_one_minute():
    a = attention(100, relative=4)
    a["active"][0]["facts"].update(observed_trade_imbalance=.9, observed_trade_count_60s=40,
                                    last_trade_received_at=100)
    p = build_packet(a, a["active"][0], 100)
    past = summarize(p)
    past.update(at=80, midpoint=9.9, relation="aligned")
    assert decide(p, [past])[1] == "early_context"
    a["active"][0]["facts"]["densities"] = []
    p = build_packet(a, a["active"][0], 100)
    assert decide(p, [past])[0] is None
    assert decide(p, [past], economical=True)[0] is None


def test_early_flow_is_not_compared_to_price_from_ten_minutes_ago():
    state = dict(at=600, midpoint=10, change=None, flow=.9, spread=2)
    history = [dict(at=0, midpoint=100), dict(at=580, midpoint=10)]
    assert relation(state, history) == "stalled"
    assert relation(state, history[:1]) == "unknown"


def test_historical_evidence_is_frozen_and_uses_original_measurement_references():
    p = packet(100)
    old = summarize(packet(70))
    frozen = deepcopy(p)
    enriched = enrich(p, [old], "density_context")
    assert p == frozen and enriched["packet_id"] != p["packet_id"]
    verify_packet(enriched)
    fact = next(f for f in enriched["facts"] if f["id"].startswith("history.") and f["id"].endswith("midpoint"))
    assert fact["observed_at"] == 70 and fact["evidence"] == ["run:book"]
    reply = dict(packet_id=enriched["packet_id"], observations=[dict(id="old", fact_id=fact["id"], value=10)],
                 contradictions=[], interpretations=[], missing_data=[], abstention=False)
    assert validate_review(reply, enriched, 120)["claims"][0]["status"] == "supported"


def test_episode_memory_is_bounded_and_survives_new_store(service):
    s, clock, call = service
    for now in range(100, 1100, 10):
        s.context_memory.observe(packet(now))
    memory = ContextMemory(s.journal.db).get(packet()["episode_id"])
    assert len(memory["samples"]) == 64
    assert memory["samples"][0]["at"] == 460


def test_early_trade_facts_do_not_invent_side_or_coverage():
    facts = Facts(version=2)
    facts._trade(dict(coin="SMALL", px="10", sz="1", time=100000, tid=1), 100, "trade", "run:trade")
    facts.books["SMALL"] = dict(received_at=100, midpoint=10, evidence="run:book")
    facts.zones["SMALL"] = {("bid", "10"): dict(notional_usd=10, price=10, first_seen=100)}
    card = facts.card("SMALL", 100)
    assert card["observed_trade_count_60s"] == 1
    assert card["observed_trade_imbalance"] is None


def test_provider_schema_restricts_claim_ids_to_validator_format():
    import re
    contract = schema(packet())
    for section in ("observations", "interpretations", "contradictions"):
        pattern = contract["properties"][section]["items"]["properties"]["id"]["pattern"]
        assert re.fullmatch(pattern, "obs_1")
        assert not re.fullmatch(pattern, "obs.midpoint")


@pytest.mark.asyncio
async def test_provider_timeout_is_explicit_and_has_no_hidden_retry():
    from extensions.scalper.analysis import OpenRouter
    session = SimpleNamespace(post=Mock(side_effect=TimeoutError()))
    reviewer = FactReviewer(OpenRouter(session, "test/model", "test-key"))
    result = await reviewer.review(packet())
    assert result == dict(status="network_error", reason="timeout")
    assert session.post.call_count == 1
    assert session.post.call_args.kwargs["timeout"].total == 60
