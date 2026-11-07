"""Cross-component acceptance for the merged system, without paid/network calls."""

import asyncio
import json
import time
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.ai_routes import AIRoutes
from extensions.scalper.delivery import CardOutbox
from extensions.scalper.fact_packet import build_packet
from extensions.scalper.fact_review import FactReviewer
from extensions.scalper.history.archive import Archive
from extensions.scalper.history.replay import replay
from extensions.scalper.journal import Journal
from extensions.scalper.service import Screener
from extensions.scalper.unified_attention import UnifiedAttention
from hyperdata_terminal.data_layer.orderbook import OrderBookEngine
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine


@pytest.fixture
def journal(tmp_path, monkeypatch):
    monkeypatch.setenv("SCALPER_CONTEXT_AUTO", "true")
    j = Journal(tmp_path / "journal.sqlite")
    yield j
    j.close()


def attention(now=100, size=1000, zone=True, relative=8):
    densities = (
        [
            dict(
                side="bid",
                price=9.99,
                size=size,
                notional_usd=size * 9.99,
                relative_size=relative,
                observed_age_seconds=now - 60,
                size_change=0,
                distance_bps=10,
                size_to_observed_turnover=1,
                evidence="run:book",
            )
        ]
        if zone
        else []
    )
    card = dict(
        status="observing",
        received_at=now,
        midpoint=10,
        spread_bps=2,
        observed_levels=[20, 20],
        densities=densities,
        evidence="run:book",
        observed_turnover_60s_usd=100,
        trade_evidence=["run:trade"],
    )
    return dict(
        context_only=True,
        enabled=True,
        archive=dict(run="run", error=None),
        last_scout_at=now,
        active=[dict(symbol="SMALL", state="focus", assigned_at=0, score=6, facts=card)],
    )


def paid():
    return SimpleNamespace(session=AsyncMock(), key="test", model="test/paid", provider="openrouter", proxy=None)


def state(flow=0.8):
    return dict(flow=flow, relation="aligned", cross="aligned", zone=None)


def test_free_and_paid_separate_budgets_and_durable_dedup(journal):
    routes = AIRoutes(journal, paid())
    packet = dict(episode_id="episode", symbol="SMALL")
    now = time.time()
    assert routes.reserve(packet, "free", state(), now)[0]
    assert journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == 0
    assert routes.reserve(packet, "strong", state(), now)[0]  # Independent strong promotion.
    assert routes.reserve(packet, "free", state(), now + 400)[1] == "unchanged_context"
    restarted = AIRoutes(journal, paid())
    assert restarted.reserve(packet, "strong", state(), now + 400)[1] == "unchanged_context"
    assert journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == 1


def test_shared_paid_limit_includes_meta_and_manual(journal):
    routes = AIRoutes(journal, paid())
    now = time.time()
    for i in range(40):
        assert routes.reserve(dict(episode_id=str(i), symbol=str(i)), "strong", state(), now)[0]
    assert routes.reserve(dict(episode_id="meta", symbol="MARKET"), "meta", state(), now)[1] == "hourly_limit"
    assert routes.reserve(dict(episode_id="weak", symbol="WEAK"), "free", state(), now)[0]


def test_free_hour_and_day_limits_and_429(journal):
    routes = AIRoutes(journal, paid())
    now = time.time()
    for i in range(12):
        assert routes.reserve(dict(episode_id=str(i), symbol=str(i)), "free", state(), now)[0]
    assert routes.reserve(dict(episode_id="extra", symbol="X"), "free", state(), now)[1] == "free_quota"
    job = journal.db.execute("SELECT job_id FROM ai_route_requests LIMIT 1").fetchone()[0]
    routes.finish(job, "http_429", "review", 3600)
    assert routes.reserve(dict(episode_id="next", symbol="Y"), "free", state(), now + 10)[1] == "provider_cooldown"
    assert routes.reserve(dict(episode_id="strong", symbol="Y"), "strong", state(), now + 10)[0]


def test_relative_attention_zero_reference_and_no_liquidity_ranking(journal):
    selector = UnifiedAttention(journal, slots=2, detail_slots=2)
    assets = [dict(symbol="SMALL", mark_price=1, volume_24h=1), dict(symbol="BIG", mark_price=100, volume_24h=10**12)]
    now = 1_000_000
    target = int((now * 1000 - 5000) // 60000 * 60000 - 60000)
    selector.activity = {"SMALL": dict(status="ready", start_ms=target, ratios=dict(trade_count=8), trade_count=100)}
    selector.choose(assets, now)
    assert selector.plan["detail"] == ["SMALL"]
    assert selector.plan["selected"][0] == "SMALL"
    restored = UnifiedAttention(journal, slots=2, detail_slots=2)
    assert restored.engine.active == selector.engine.active


def test_relative_decisions_archive_replays_and_restored_state_replays(tmp_path, journal):
    selector = UnifiedAttention(journal, slots=2, detail_slots=2)
    selector.archive = Archive(tmp_path / "decisions")
    selector.archive.record("policy", asdict(selector.engine.policy), 100)
    selector.archive.record("attention_state", selector.state(), 100)
    for now in range(100, 160):
        selector.choose([dict(symbol="SMALL", mark_price=1 if now < 130 else 2, volume_24h=0)], now)
    selector.close()
    result = replay(selector.archive.path)
    assert result["verified"] and result["assignments"]


@pytest.mark.asyncio
async def test_external_flow_has_no_trade_socket_and_ingests_one_tape():
    engine = OrderFlowEngine([], include_binance=False)
    engine.selective, engine.pulse_external = True, True
    await engine.start()
    await engine.set_attention_symbols(["SMALL"])
    assert not engine._hl_tasks and not engine._attention_tasks
    row = dict(
        venue="hyperliquid",
        native_market_id="SMALL",
        delivery_kind="live",
        side="buy",
        price="2",
        base_amount="3",
        quote_amount="6",
        event_time_ms=int(time.time() * 1000),
        source_epoch="one",
    )
    assert engine.accept_execution(row)
    assert len(engine.recent_trades["SMALL"]) == 1
    assert not engine.accept_execution({**row, "delivery_kind": "recovery"})
    assert not engine.accept_execution({**row, "side": "unknown"})
    assert engine.accept_execution({**row, "source_epoch": "two"})
    assert len(engine.recent_trades["SMALL"]) == 1  # Epoch reset, not a mixed window.
    await engine.stop()


def test_book_age_is_instrument_specific():
    engine = OrderBookEngine(["BTC", "ETH"])
    frame = dict(levels=[[dict(px="100", sz="1")], [dict(px="101", sz="1")]])
    engine._update_book("BTC", frame)
    engine.books["BTC"]["updated_at"] = time.time() - 60
    engine._build_snapshot("BTC")
    engine._update_book("ETH", frame)
    assert engine.get_snapshot("BTC").stale
    assert not engine.get_snapshot("ETH").stale


@pytest.mark.asyncio
async def test_outbox_429_survives_restart_and_edits_one_card(journal):
    alerts = SimpleNamespace(
        send_telegram=AsyncMock(return_value=None),
        edit_telegram=AsyncMock(return_value=True),
        last_telegram_result=dict(state="failed", http_status=429, retry_after=30),
    )
    box = CardOutbox(journal, alerts)
    now = time.time()
    box.enqueue("episode", "facts", "<p>facts</p>", now + 120)
    await box.once(now)
    restarted = CardOutbox(journal, alerts)
    await restarted.once(now + 10)
    assert alerts.send_telegram.await_count == 1
    alerts.send_telegram.return_value = (123, True)
    alerts.last_telegram_result = dict(state="sent", http_status=200)
    await restarted.once(now + 31)
    restarted.enqueue("episode", "analysis", "<p>analysis</p>", now + 120)
    await restarted.once(now + 32)
    assert alerts.send_telegram.await_count == 2 and alerts.edit_telegram.await_count == 1
    assert alerts.edit_telegram.await_args.args[0] == 123


@pytest.mark.asyncio
async def test_ambiguous_initial_card_is_never_resent(journal):
    alerts = SimpleNamespace(
        send_telegram=AsyncMock(return_value=None), last_telegram_result=dict(state="outcome_unknown")
    )
    box = CardOutbox(journal, alerts)
    box.enqueue("episode", "facts", "facts", time.time() + 120)
    await box.once()
    CardOutbox(journal, alerts).enqueue("episode", "new", "new", time.time() + 120)
    await box.once()
    assert alerts.send_telegram.await_count == 1


@pytest.mark.asyncio
async def test_actual_auto_worker_free_and_strong_route(monkeypatch, journal):
    monkeypatch.setenv("PULSE_UNIFIED", "1")
    monkeypatch.setenv("OPENROUTER_API_KEY", "test")
    now = time.time()
    card = dict(
        status="observing",
        received_at=now,
        midpoint=10,
        spread_bps=2,
        observed_levels=[20, 20],
        densities=[
            dict(
                side="bid",
                price=9.99,
                size=1000,
                notional_usd=9990,
                relative_size=8,
                observed_age_seconds=50,
                size_change=0,
                distance_bps=10,
                evidence="book",
            )
        ],
        evidence="book",
        trade_evidence=[],
    )
    attention = dict(
        enabled=True,
        context_only=True,
        policy_version=4,
        last_scout_at=now,
        archive=dict(run="one", error=None),
        active=[dict(symbol="SMALL", state="focus", assigned_at=now - 60, score=8, facts=card)],
    )
    hub = SimpleNamespace(attention=attention, alerts=SimpleNamespace(), demo=False)
    service = Screener(hub, journal, paid(), False)
    service.ai_routes.check_free = AsyncMock(return_value=True)
    previous = json.loads(json.dumps(attention))
    previous["active"][0]["facts"]["received_at"] = now - 10
    p = build_packet(previous, previous["active"][0], now - 10)
    service.context_memory.observe(p)
    calls = []

    async def answer(self, packet):
        calls.append(self.analyst.model)
        return dict(
            status="ok",
            model=self.analyst.model,
            raw=json.dumps(
                dict(
                    packet_id=packet["packet_id"],
                    observations=[],
                    interpretations=[],
                    contradictions=[],
                    missing_data=[],
                    abstention=True,
                )
            ),
        )

    monkeypatch.setattr(FactReviewer, "review", answer)
    # Empty frames are excluded; this fixture tests a valid native density packet.
    service.rows = {}
    assert (await service.auto_context_once())[0]["status"] == "ok"
    assert calls == ["nvidia/nemotron-3.5-super-vl:free"]
    assert journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == 0

    service.rows = {
        ("SMALL", "1m"): dict(
            snapshot=dict(symbol="SMALL", window="1m", captured_at=now, metrics={}), strength=dict(rank=3)
        )
    }
    assert (await service.auto_context_once())[0]["status"] == "ok"
    assert calls[-1] == "test/paid"
    assert journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == 1
    assert await service.auto_context_once() == []


@pytest.mark.asyncio
async def test_meta_requires_two_material_episodes_and_is_not_repeated(monkeypatch, journal):
    from copy import deepcopy

    monkeypatch.setenv("PULSE_UNIFIED", "1")
    now = time.time()
    a = attention(now)
    a["policy_version"] = 4
    second = deepcopy(a["active"][0])
    second["symbol"] = "OTHER"
    hub = SimpleNamespace(attention=a, alerts=SimpleNamespace(), demo=False)
    service = Screener(hub, journal, paid(), False)
    for assignment in [a["active"][0], second]:
        old = deepcopy(assignment)
        old["facts"]["received_at"] = now - 10
        service.context_memory.observe(build_packet(a, old, now - 10))
    calls = []

    async def answer(self, packet):
        calls.append(packet)
        return dict(
            status="ok",
            model=self.analyst.model,
            raw=json.dumps(
                dict(
                    packet_id=packet["packet_id"],
                    observations=[],
                    interpretations=[],
                    contradictions=[],
                    missing_data=[],
                    abstention=True,
                )
            ),
        )

    monkeypatch.setattr(FactReviewer, "review", answer)
    service.refresh_context_packets(now)
    assert await service.meta_context_once() is None
    a["active"].append(second)
    service.refresh_context_packets(now)
    result = await service.meta_context_once()
    assert result["status"] == "ok" and result["review"]["route"] == "meta"
    assert result["packet"]["symbol"] == "MARKET"
    assert any(f["role"] == "history" for f in result["packet"]["facts"])
    assert all("review" not in f["source"] for f in result["packet"]["facts"])
    assert await service.meta_context_once() is None and len(calls) == 1
    assert journal.db.execute("SELECT COUNT(*) FROM ai_requests WHERE purpose='context_meta'").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_free_catalog_failure_never_calls_paid(monkeypatch, journal):

    monkeypatch.setenv("PULSE_UNIFIED", "1")
    a = attention(time.time())
    hub = SimpleNamespace(attention=a, alerts=SimpleNamespace(), demo=False)
    service = Screener(hub, journal, paid(), False)
    service.ai_routes.check_free = AsyncMock(return_value=False)
    call = AsyncMock()
    monkeypatch.setattr(FactReviewer, "review", call)
    result = await service.review_context("SMALL", route="free")
    assert result["status"] == "skipped"
    call.assert_not_awaited()
    assert journal.db.execute("SELECT COUNT(*) FROM ai_requests").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_one_plan_updates_native_l2_and_flow_cursor_skips_duplicates(tmp_path, monkeypatch):
    from extensions.scalper.history.pulse_runtime import PulseObservation

    async def waiting(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(PulseObservation, "run", waiting)
    engine = OrderFlowEngine([], include_binance=False)
    engine.selective, engine.pulse_external = True, True
    await engine.start()
    hub = SimpleNamespace(orderflow=engine, orderbook=SimpleNamespace(set_symbols=AsyncMock()))
    observation = PulseObservation(hub, tmp_path)
    now = time.time()
    observation.plan = dict(
        schema_version="attention.v1",
        generated_at_ms=int(now * 1000),
        expires_at_ms=int((now + 60) * 1000),
        selected=["SMALL"],
        detail=["SMALL"],
        controls=[],
        policy_version=4,
        active=[dict(symbol="SMALL", state="focus", reason="relative", assigned_at=now - 10, since=now - 10, score=8)],
    )
    row = dict(
        event_seq=11,
        venue="hyperliquid",
        native_market_id="SMALL",
        delivery_kind="live",
        side="buy",
        price="2",
        base_amount="3",
        quote_amount="6",
        event_time_ms=int(now * 1000),
        source_epoch="one",
        trade_id="one",
        raw_ref="raw:one",
    )
    replies = [
        dict(head_cursor=10, next_cursor=None, events=[]),
        dict(head_cursor=11, next_cursor=11, events=[row]),
        dict(head_cursor=11, next_cursor=11, events=[row]),
    ]

    async def get(path, **params):
        if path == "/attention":
            return dict(plan=observation.plan)
        return replies.pop(0)

    observation.get = get
    await observation.cycle()
    assert set(engine.symbols) == {"SMALL"}
    assert set(hub.orderbook.set_symbols.await_args.args[0]) == set(engine.symbols)
    await observation.cycle()
    await observation.cycle()
    assert len(engine.recent_trades["SMALL"]) == 1 and observation.cursor == 11
    await observation.close()
    await engine.stop()
    assert replay(observation.archive.path)["verified"]


@pytest.mark.asyncio
async def test_free_transport_disables_reasoning_to_preserve_json_budget(journal):
    from unittest.mock import MagicMock

    from extensions.scalper.fact_review import FactReviewer

    response = SimpleNamespace(
        status=200,
        json=AsyncMock(
            return_value=dict(
                choices=[dict(finish_reason="stop", message=dict(content="{}"))], model="free", usage=dict(cost=0)
            )
        ),
    )
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=None)
    client = paid()
    client.session = SimpleNamespace(post=MagicMock(return_value=context))
    routes = AIRoutes(journal, client)
    routes.free.key = "test"
    a = attention(time.time())
    packet = build_packet(a, a["active"][0], time.time())
    assert (await FactReviewer(routes.free).review(packet))["status"] == "ok"
    body = client.session.post.call_args.kwargs["json"]
    assert body["reasoning"] == {"enabled": False}
    assert body["model"] == "nvidia/nemotron-3.5-super-vl:free"


@pytest.mark.asyncio
async def test_delisted_markets_are_excluded_without_a_volume_floor(monkeypatch):
    from hyperdata_terminal.data_layer.market_data import MarketData

    engine = MarketData()
    response = [
        dict(universe=[dict(name="OLD", isDelisted=True), dict(name="SMALL")]),
        [dict(markPx="1", dayNtlVlm="0"), dict(markPx="1", dayNtlVlm="0")],
    ]
    monkeypatch.setattr(engine, "_post", AsyncMock(return_value=response))
    await engine.refresh()
    assert set(engine.assets) == {"SMALL"}


def test_no_assignments_without_a_publisher():
    from extensions.scalper.history.pulse_runtime import PulseObservation

    observer = PulseObservation.__new__(PulseObservation)
    observer.plan = None
    assert observer.assignments(100) == observer.assignments(200) == {}


@pytest.mark.asyncio
async def test_failed_history_pauses_native_collection():
    from extensions.scalper.history.pulse_runtime import PulseObservation

    observer = PulseObservation.__new__(PulseObservation)
    observer.archive = SimpleNamespace(error="storage_budget_exhausted")
    observer.plan = None
    observer.hub = SimpleNamespace(
        orderbook=SimpleNamespace(set_symbols=AsyncMock()),
        orderflow=SimpleNamespace(set_attention_symbols=AsyncMock()),
    )
    with pytest.raises(RuntimeError, match="history_unavailable_collection_paused"):
        await observer.cycle()
    observer.hub.orderbook.set_symbols.assert_awaited_once_with([])
    observer.hub.orderflow.set_attention_symbols.assert_awaited_once_with([])


def test_archive_uses_assigned_budget_and_preserves_active_run(tmp_path):
    from extensions.scalper.history.archive import Archive

    archive = Archive(tmp_path, quota_bytes=1)
    try:
        (archive.path / "protected-evidence").write_bytes(b"over budget")
        archive.check_space(time.time())
        assert archive.error == "storage_budget_exhausted"
        assert archive.path.exists()
    finally:
        archive.close()


@pytest.mark.asyncio
async def test_restart_releases_forgotten_channels_without_repairing_old_gap():
    from hyperdata_terminal.market_pulse.gaps import GapTracker
    from hyperdata_terminal.market_pulse.ingest import GapControlService
    from hyperdata_terminal.market_pulse.storage import InMemoryMarketPulseStore

    store = InMemoryMarketPulseStore()
    gaps = GapTracker(store)
    for symbol in ("OLD-USDT-SWAP", "BTC-USDT-SWAP"):
        await gaps.on_connected("okx", symbol, "trades-all", "before-restart", 1000)
        await gaps.on_disconnected("okx", symbol, "trades-all", "before-restart", 2000, reason="connection_lost")
    original = [g.gap_id for g in await store.list_gaps() if g.state == "gap_detected"]
    control = GapControlService(store, gaps)
    await control.release_outside_attention("okx", ["BTC"], 3000)
    await control.release_outside_attention("okx", ["BTC"], 4000)
    coverage = {c.native_market_id: c for c in await store.list_coverage()}
    assert coverage["OLD-USDT-SWAP"].state == "outside_attention"
    assert coverage["BTC-USDT-SWAP"].state == "gap_detected"
    history = await store.list_gaps()
    assert {g.gap_id for g in history if g.state == "gap_detected"} == set(original)
    assert len([g for g in history if g.state == "outside_attention"]) == 1


def test_live_sqlite_backup_restores_durable_episode_state(tmp_path):
    import sqlite3

    from scripts.pulse_ops import sqlite_snapshot, verify_sqlite

    source = tmp_path / 'live.sqlite'
    target = tmp_path / 'market-pulse-test.dump'
    with sqlite3.connect(source) as live:
        live.execute('PRAGMA journal_mode=WAL')
        live.execute('CREATE TABLE episode_cards(message_id INTEGER)')
        live.execute('INSERT INTO episode_cards VALUES(1232)')
        live.commit()
        metadata = sqlite_snapshot(source, target.with_suffix('.sqlite'))
        assert verify_sqlite(target, metadata) == {'integrity': 'ok', 'tables': 1}
        with sqlite3.connect(target.with_suffix('.sqlite')) as restored:
            assert restored.execute('SELECT message_id FROM episode_cards').fetchone()[0] == 1232
    target.with_suffix('.sqlite').write_bytes(b'changed')
    with pytest.raises(ValueError, match='checksum'):
        verify_sqlite(target, metadata)


def test_meta_cross_instrument_interpretation_is_only_a_hypothesis():
    from extensions.scalper.fact_packet import checksum
    from extensions.scalper.fact_review import META_VALIDATOR_VERSION, validate_review

    now = time.time()
    a = attention(now)
    packet = build_packet(a, a["active"][0], now)
    base = next(f for f in packet["facts"] if f["id"] == "book.midpoint")
    packet.update(
        symbol="MARKET",
        scope="market_meta",
        input_packets=["one", "two"],
        facts=[dict(base, id="ONE:book.midpoint"), dict(base, id="TWO:book.midpoint")],
    )
    packet.pop("packet_id")
    packet["packet_id"] = checksum(packet)
    raw = json.dumps(
        dict(
            packet_id=packet["packet_id"],
            observations=[],
            contradictions=[],
            missing_data=[],
            abstention=False,
            interpretations=[
                dict(
                    id="link",
                    kind="context_link",
                    evidence=["ONE:book.midpoint", "TWO:book.midpoint"],
                    text="Возможна связь наблюдаемых изменений.",
                )
            ],
        )
    )
    result = validate_review(raw, packet, now)
    assert result["validator_version"] == META_VALIDATOR_VERSION
    assert result["claims"][0]["status"] == "hypothesis"
