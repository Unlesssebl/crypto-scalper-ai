"""Content identity, factual contradiction, unknowns, hypotheses and real request wiring."""
import json
import sqlite3
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from apps.screener_web.__main__ import build_app
from extensions.scalper.consistency_store import ConsistencyStore
from extensions.scalper.fact_packet import build_packet, packet_ready, verify_packet
from extensions.scalper.fact_review import FactReviewer, validate_review
from extensions.scalper.journal import Journal
from extensions.scalper.service import Screener
from extensions.scalper.telegram import consistency_text


@pytest.fixture
def attention():
    card = dict(symbol="SMALL", status="observing", received_at=100., evidence="run:1",
                midpoint=10., spread_bps=2., observed_levels=[20, 20], observed_turnover_60s_usd=1000.,
                trade_evidence=["run:2"], densities=[dict(side="bid", price=9.99, size=100.,
                notional_usd=999., observed_age_seconds=8., distance_bps=10., relative_size=4.,
                size_change=2., size_to_observed_turnover=.999, evidence="run:1")])
    return dict(enabled=True, context_only=True, archive=dict(run="run", error=None),
                last_scout_at=100., active=[dict(symbol="SMALL", state="focus", assigned_at=90., score=6., facts=card)])


def packet(attention):
    return build_packet(attention, attention["active"][0], 100.)


def reply(p):
    return dict(packet_id=p["packet_id"], observations=[dict(id="price", fact_id="book.midpoint", value=10.)],
                interpretations=[], contradictions=[], missing_data=[], abstention=False)


def test_interpretation_schema_excludes_numeric_prose_and_requires_evidence(attention):
    import re

    from extensions.scalper.fact_review import schema

    p = packet(attention)
    fields = schema(p)["properties"]["interpretations"]["items"]["properties"]
    assert re.fullmatch(fields["text"]["pattern"], "Наблюдаемая зона стакана")
    assert not re.fullmatch(fields["text"]["pattern"], "Наблюдаемый L2 стакан")
    assert fields["evidence"]["minItems"] == 1
    assert "context_link" not in fields["kind"]["enum"]
    p["scope"] = "market_meta"
    assert "context_link" in schema(p)["properties"]["interpretations"]["items"]["properties"]["kind"]["enum"]


def test_packet_content_hash_detects_metric_change_even_with_identical_time_and_symbol(attention):
    p = packet(attention)
    changed = deepcopy(p)
    changed["facts"][0]["value"] = 999
    with pytest.raises(ValueError, match="checksum"):
        verify_packet(changed)
    assert verify_packet(p) == p
    attention["active"][0]["facts"]["midpoint"] = 99
    assert next(f["value"] for f in p["facts"] if f["id"] == "book.midpoint") == 10
    assert packet(attention)["packet_id"] != p["packet_id"]


def test_hash_deterministic_and_source_coverage_explicit(attention):
    p = packet(attention)
    assert p == packet(attention)
    assert any(f["id"].endswith("actual_order_age") and f["quality"] == "unknown" for f in p["facts"])
    assert packet_ready(p, 110)
    assert not packet_ready(p, 116)


def test_previous_episode_book_and_impossible_density_age_not_accepted(attention):
    attention["active"][0]["assigned_at"] = 101
    assert not packet_ready(packet(attention), 101)
    attention["active"][0]["assigned_at"] = 99
    p = packet(attention)
    age = next(f for f in p["facts"] if f["id"].endswith("observed_age_seconds"))
    assert age["quality"] == "unknown"


def test_same_value_supported_but_wrong_number_is_contradicted(attention):
    p = packet(attention)
    r = reply(p)
    r["observations"][0]["value"] = 10  # JSON integer and floating number have the same measured value.
    assert validate_review(r, p, 101)["claims"][0]["status"] == "supported"
    r["observations"][0]["value"] = 11
    checked = validate_review(r, p, 101)
    assert checked["claims"][0]["status"] == "contradicted"
    r["observations"][0]["value"] = True
    assert validate_review(r, p, 101)["claims"][0]["status"] == "contradicted"


def test_unavailable_order_age_not_converted_into_confirmed_fact(attention):
    p = packet(attention)
    ref = next(f["id"] for f in p["facts"] if f["id"].endswith("actual_order_age"))
    r = reply(p)
    r["observations"] = [dict(id="age", fact_id=ref, value=100)]
    r["missing_data"] = [ref]
    result = validate_review(r, p, 101)
    assert [c["status"] for c in result["claims"]] == ["unverifiable", "supported"]


def test_foreign_references_unverifiable_and_wrong_packet_rejected(attention):
    p = packet(attention)
    r = reply(p)
    r["observations"][0]["fact_id"] = "OTHER_SYMBOL.price"
    assert validate_review(r, p, 101)["claims"][0]["status"] == "unverifiable"
    r["packet_id"] = "foreign"
    with pytest.raises(ValueError, match="another fact packet"):
        validate_review(r, p, 101)


def test_expired_response_checks_frozen_facts_separately_from_current_freshness(attention):
    p = packet(attention)
    result = validate_review(reply(p), p, 116)
    assert result["expired_at_completion"]
    assert result["assessment_basis"] == "frozen_packet"
    assert result["claims"][0]["status"] == "supported"
    r = reply(p)
    r["observations"][0]["value"] = 20
    r["missing_data"] = ["book.midpoint"]
    for delay in (0, 16, 1000):
        checked = validate_review(r, p, 100 + delay)
        assert [c["status"] for c in checked["claims"]] == ["contradicted", "contradicted"]


def test_archive_gap_marks_packet_unavailable(attention):
    attention["archive"]["error"] = "writer_failed"
    p = packet(attention)
    assert not packet_ready(p, 101)
    assert validate_review(reply(p), p, 101)["claims"][0]["status"] == "unverifiable"


def test_attention_outage_cannot_certify_fresh_book_or_zero_tape(attention):
    # /attention is unavailable while the valid lease is kept: the execution
    # cursor stops advancing, but L2 may still look fresh. Neither the packet
    # nor a zero tape turnover may be presented as observed facts.
    attention["plan_error"] = "RuntimeError: pulse unavailable"
    p = packet(attention)
    by_id = {fact["id"]: fact for fact in p["facts"]}
    assert p["coverage"]["observation_healthy"] is False
    assert not packet_ready(p, 101)
    assert by_id["book.midpoint"]["quality"] == "unknown"
    assert by_id["tape.observed_turnover_60s"]["value"] is None


def test_compatible_comparison_checked_but_units_and_roles_cannot_be_conflated(attention):
    p = packet(attention)
    r = reply(p)
    r["contradictions"] = [dict(id="depth", left="book.visible_bid_levels", right="book.visible_ask_levels",
                                relation="greater")]
    result = validate_review(r, p, 101)
    assert result["claims"][1]["status"] == "contradicted"
    r["contradictions"][0]["right"] = "book.spread_bps"
    assert validate_review(r, p, 101)["claims"][1]["status"] == "unverifiable"


def test_interpretation_is_always_hypothesis_not_confirmed_by_matching_evidence(attention):
    p = packet(attention)
    r = reply(p)
    r["interpretations"] = [dict(id="hyp", kind="other", evidence=["book.midpoint"],
                                  text="Нужны дополнительные наблюдения за ценой.")]
    result = validate_review(r, p, 101)
    assert result["claims"][-1]["status"] == "hypothesis"
    assert result["independence"] == "shared_input_facts"


def test_absorption_without_flow_and_price_evidence_is_unverifiable(attention):
    p = packet(attention)
    r = reply(p)
    r["interpretations"] = [dict(id="hyp", kind="possible_absorption", evidence=["book.midpoint"],
                                  text="Возможно поглощение.")]
    assert validate_review(r, p, 101)["claims"][-1]["status"] == "unverifiable"


def test_flow_price_hypothesis_has_same_window_native_measurements_without_direction_vote(attention):
    def metric(value, at=99):
        return dict(quality="ok", value=value, updated_at=at)
    frame = dict(symbol="SMALL", window="1m", captured_at=100, metrics=dict(
        first_trade_price=metric(10, 45), last_trade_price=metric(9), ofi=metric(.8), cvd=metric(1000)))
    p = build_packet(attention, attention["active"][0], 100, frames=[frame])
    r = reply(p)
    r["interpretations"] = [dict(id="h", kind="flow_price_mismatch", text="Поток расходится с реакцией цены.",
                                  evidence=["frame.1m.ofi", "frame.1m.flow_price_conflict"])]
    assert validate_review(r, p, 101)["claims"][-1]["status"] == "hypothesis"
    assert not any("direction" in f["id"] for f in p["facts"])
    frame["metrics"]["ofi"]["updated_at"] = 105
    future = build_packet(attention, attention["active"][0], 100, frames=[frame])
    assert next(f for f in future["facts"] if f["id"] == "frame.1m.ofi")["quality"] == "unknown"


def test_cross_window_comparison_cannot_be_counted_as_same_window_agreement(attention):
    def frame(window):
        m = dict(quality="ok", value=.8, updated_at=99)
        return dict(symbol="SMALL", window=window, captured_at=100, metrics={"ofi": m})
    p = build_packet(attention, attention["active"][0], 100, frames=[frame("1m"), frame("5m")])
    r = reply(p)
    r["contradictions"] = [dict(id="different_windows", left="frame.1m.ofi", right="frame.5m.ofi", relation="equal")]
    assert validate_review(r, p, 101)["claims"][-1]["status"] == "unverifiable"


@pytest.mark.parametrize("text", ["Новый уровень 100", "Открыть позицию", "BUY", "Покупай сейчас"])
def test_invented_levels_and_trade_commands_rejected(attention, text):
    p = packet(attention)
    r = reply(p)
    r["interpretations"] = [dict(id="h", kind="other", evidence=["book.midpoint"], text=text)]
    with pytest.raises(ValueError):
        validate_review(r, p, 101)


def test_abstention_and_empty_response_not_scored_as_success(attention):
    p = packet(attention)
    r = reply(p)
    r["observations"] = []
    with pytest.raises(ValueError, match="explicitly abstain"):
        validate_review(r, p, 101)
    r["abstention"] = True
    result = validate_review(r, p, 101)
    assert result["abstention"] and not result["claims"]


def test_model_cannot_self_assign_supported_status_or_duplicate_claim_identity(attention):
    p = packet(attention)
    r = reply(p)
    r["observations"][0]["status"] = "supported"
    with pytest.raises(ValueError, match="Invalid claim fields"):
        validate_review(r, p, 101)
    r = reply(p)
    r["observations"].append(dict(r["observations"][0]))
    with pytest.raises(ValueError, match="duplicate claim"):
        validate_review(r, p, 101)


def test_false_missing_data_claim_is_contradicted(attention):
    p = packet(attention)
    r = reply(p)
    r["missing_data"] = ["book.midpoint"]
    assert validate_review(r, p, 101)["claims"][-1]["status"] == "contradicted"


def test_repetition_and_report_keep_hypotheses_out_of_supported_denominator(attention):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    store = ConsistencyStore(db)
    p = packet(attention)
    r = reply(p)
    r["interpretations"] = [dict(id="h", kind="other", evidence=["book.midpoint"], text="Нужен дальнейший контекст.")]
    for at in (101, 102):
        key = store.begin(p, at, "test", "test")
        store.finish(key, "ok", at, result=validate_review(r, p, at))
    report = store.report(103)
    assert report["claims"]["observation"] == {"supported": 2}
    assert report["claims"]["interpretation"] == {"hypothesis": 2}
    assert report["novelty"] == {"first_seen": 2, "repeated": 2}
    assert report["observed_episodes"] == report["reviewed_episodes"] == 1
    assert "accuracy" not in report
    assert "Точность" not in consistency_text(report)
    db.close()


@pytest.fixture
def service(tmp_path, attention, monkeypatch):
    journal = Journal(tmp_path / "journal.sqlite")
    hub = SimpleNamespace(attention=attention, demo=False)
    analyst = SimpleNamespace(key="", provider="openrouter", model="test/model")
    s = Screener(hub, journal, analyst, False)
    monkeypatch.setattr("extensions.scalper.service.time", SimpleNamespace(time=lambda: 101.))
    monkeypatch.setattr("apps.screener_web.__main__.time", SimpleNamespace(time=lambda: 101.))
    s.refresh_context_packets(100.)
    yield s
    journal.close()


@pytest.mark.asyncio
async def test_no_key_returns_packet_without_spending_budget(service):
    p = service.context_packets["SMALL"]
    result = await service.review_context("SMALL", p["packet_id"])
    assert result["status"] == "unconfigured" and result["packet"] == p
    assert service.journal.db.execute("SELECT count(*) FROM ai_requests").fetchone()[0] == 0
    assert service.consistency.report(102)["statuses"] == {"unconfigured": 1}


@pytest.mark.asyncio
async def test_manual_review_shared_budget_dedup_and_new_snapshot_cooldown(service, monkeypatch):
    service.gemini.key = "configured"
    async def answer(p):
        return dict(status="ok", raw=json.dumps(reply(p)), model="test/model")
    call = AsyncMock(side_effect=answer)
    monkeypatch.setattr(FactReviewer, "review", call)
    p = service.context_packets["SMALL"]
    a = await service.review_context("SMALL", p["packet_id"])
    assert a["status"] == "ok"
    b = await service.review_context("SMALL", p["packet_id"])
    assert b["cached"] and call.await_count == 1
    service.refresh_context_packets(100.5)
    c = await service.review_context("SMALL", service.context_packets["SMALL"]["packet_id"])
    assert c["review"]["reason"] == "cooldown"
    assert service.journal.db.execute("SELECT purpose FROM ai_requests").fetchone()[0] == "context"


@pytest.mark.asyncio
async def test_native_api_failure_blocks_new_reviews(service, monkeypatch):
    service.gemini.key = "configured"
    call = AsyncMock()
    monkeypatch.setattr(FactReviewer, "review", call)
    service.hub.last_error = "ClientConnectorError: native API refused"

    packet = service.context_packets["SMALL"]
    result = await service.review_context("SMALL", packet["packet_id"])

    assert result == {"status": "skipped", "reason": "native_unavailable"}
    call.assert_not_awaited()
    assert service.journal.db.execute("SELECT count(*) FROM ai_requests").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_auto_context_never_spends_budget_while_native_is_down(service):
    service.hub.last_error = "ClientConnectorError: native API refused"
    assert await service.auto_context_once() == []


@pytest.mark.asyncio
async def test_native_api_failure_keeps_previous_results_as_history(service, monkeypatch):
    service.gemini.key = "configured"

    async def answer(packet):
        return dict(status="ok", raw=json.dumps(reply(packet)), model="test/model")

    monkeypatch.setattr(FactReviewer, "review", AsyncMock(side_effect=answer))
    packet = service.context_packets["SMALL"]
    first = await service.review_context("SMALL", packet["packet_id"])
    assert first["status"] == "ok"

    service.hub.last_error = "ServerDisconnectedError: native API dropped"
    second = await service.review_context("SMALL", packet["packet_id"])
    assert second == {"status": "skipped", "reason": "native_unavailable"}
    # The completed review remains readable in the journal as history.
    assert service.consistency.latest(packet["packet_id"])["status"] == "ok"


@pytest.mark.asyncio
async def test_invalid_answer_logged_and_charged_once(service, monkeypatch):
    service.gemini.key = "configured"
    call = AsyncMock(return_value=dict(status="ok", raw='{"direction":"BULLISH"}', model="test/model"))
    monkeypatch.setattr(FactReviewer, "review", call)
    result = await service.review_context("SMALL", service.context_packets["SMALL"]["packet_id"])
    assert result["status"] == "invalid_response"
    assert service.journal.db.execute("SELECT count(*) FROM ai_requests").fetchone()[0] == 1
    assert service.consistency.report(102)["statuses"] == {"invalid_response": 1}


@pytest.mark.asyncio
async def test_budget_exhaustion_and_changed_episode_never_call_model(service, monkeypatch):
    service.gemini.key = "configured"
    service.policy = replace(service.policy, hourly_limit=0)
    call = AsyncMock()
    monkeypatch.setattr(FactReviewer, "review", call)
    p = service.context_packets["SMALL"]
    result = await service.review_context("SMALL", p["packet_id"])
    assert result["review"]["reason"] == "hourly_limit"
    service.hub.attention["active"][0]["assigned_at"] = 99
    result = await service.review_context("SMALL", p["packet_id"])
    assert result["reason"] == "episode_changed_or_unavailable"
    call.assert_not_awaited()


def test_restart_marks_pending_attempt_interrupted(service):
    p = service.context_packets["SMALL"]
    key = service.consistency.begin(p, 100, "test", "test")
    service.journal.recover()
    assert service.consistency.review(key)["status"] == "interrupted"


def test_blind_export_is_read_only_and_keeps_failed_attempts(service, tmp_path):
    from scripts.export_context_evaluation import export
    p = service.context_packets["SMALL"]
    key = service.consistency.begin(p, 100, "test", "test")
    service.consistency.finish(key, "invalid_response", 101, reason="invalid")
    source = service.journal.db.execute("PRAGMA database_list").fetchone()[2]
    result = export(source, tmp_path / "blind")
    cases = json.loads((tmp_path / "blind" / "blind-cases.json").read_text(encoding="utf-8"))
    assert result["cases"] == 1 and result["model_calls"] == 0
    assert any(v.get("status") == "invalid_response" for v in cases[0]["variants"].values())
    assert "direction" not in json.dumps(cases)
    assert service.consistency.review(key)["status"] == "invalid_response"


def test_offline_recheck_uses_saved_completion_time_and_verifies_packet_content(service):
    from scripts.replay_fact_reviews import replay
    p = service.context_packets["SMALL"]
    key = service.consistency.begin(p, 100, "test", "test")
    raw = json.dumps(reply(p))
    service.consistency.finish(key, "ok", 101.125, raw=raw, result=validate_review(raw, p, 101))
    source = service.journal.db.execute("PRAGMA database_list").fetchone()[2]
    assert replay(source)["reviews"] == 1
    bad = deepcopy(p)
    bad["symbol"] = "FOREIGN"
    with service.journal.db:
        service.journal.db.execute("UPDATE fact_packets SET payload=?", (json.dumps(bad),))
    with pytest.raises(ValueError, match="checksum"):
        replay(source)


@pytest.mark.asyncio
async def test_api_freezes_packet_requires_hash_and_checks_origin(service):
    app = build_app(service.hub, service)
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        r = await client.get("/v1/scalper/context/SMALL")
        p = await r.json()
        assert verify_packet(p)
        result = await client.post("/v1/scalper/context/SMALL", json={"packet_id": p["packet_id"]})
        assert (await result.json())["status"] == "unconfigured"
        assert (await client.post("/v1/scalper/context/SMALL", json={})).status == 400
        assert (await client.post("/v1/scalper/context/SMALL", json={"packet_id": p["packet_id"]},
                                  headers={"Origin": "https://foreign.example"})).status == 403
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["gemini", "openrouter"])
async def test_provider_receives_facts_prompt_and_no_native_direction(attention, provider):
    p = packet(attention)
    captured = []
    async def handler(request):
        body = await request.json()
        captured.append(body)
        if provider == "openrouter":
            response = {"choices": [{"finish_reason": "stop", "message": {
                "content": json.dumps(reply(p))}}], "model": "test/model"}
        else:
            response = {"candidates": [{"finishReason": "STOP", "content": {"parts": [
                {"text": json.dumps(reply(p))}]}}]}
        return web.json_response(response)
    app = web.Application()
    app.router.add_post("/chat", handler)
    server = TestServer(app)
    await server.start_server()
    from extensions.scalper.analysis import Gemini, OpenRouter
    try:
        async with aiohttp.ClientSession() as session:
            analyst = (OpenRouter(session, "test/model", "test") if provider == "openrouter"
                       else Gemini(session, "gemini-test", "test"))
            original = analyst.request
            def local_request(market, response_schema):
                _, body, headers = original(market, response_schema)
                return str(server.make_url("/chat")), body, headers
            analyst.request = local_request
            result = await FactReviewer(analyst).review(p)
            assert result["status"] == "ok"
            body = captured[0]
            prompt = (body["messages"][0]["content"] if provider == "openrouter" else
                      body["systemInstruction"]["parts"][0]["text"])
            assert "Не прогнозируй направление" in prompt
            market = (body["messages"][1]["content"] if provider == "openrouter" else
                      body["contents"][0]["parts"][0]["text"])
            sent = json.loads(market)
            assert sent == p and "direction" not in sent and "hyperdata" not in sent
            if provider == "openrouter":
                assert body["response_format"]["json_schema"]["name"] == "fact_review"
    finally:
        await server.close()
