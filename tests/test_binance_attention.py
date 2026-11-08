"""Binance instruments compete adaptively, with durable and isolated evidence."""

import json
import time
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.fact_packet import build_packet
from extensions.scalper.history.archive import Archive
from extensions.scalper.history.facts import Facts
from extensions.scalper.history.replay import replay
from extensions.scalper.pulse import PulseContext
from extensions.scalper.tiger import TigerBridge
from extensions.scalper.unified_attention import UnifiedAttention
from hyperdata_terminal.data_layer.binance_market import refresh as refresh_market
from hyperdata_terminal.data_layer.market_data import MarketData
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine
from hyperdata_terminal.instruments import binance_key, identity
from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
from hyperdata_terminal.market_pulse.collectors.binance import (
    BinanceCollector,
    binance_spec,
    normalize_binance_trades,
)
from hyperdata_terminal.market_pulse.gaps import GapTracker
from hyperdata_terminal.market_pulse.ingest import GapControlService, IngestionService
from hyperdata_terminal.market_pulse.ingest_http import create_ingest_app
from hyperdata_terminal.market_pulse.raw_archive import RawArchive
from hyperdata_terminal.market_pulse.storage import InMemoryMarketPulseStore
from hyperdata_terminal.market_pulse.transport import HttpIngestClient
from hyperdata_terminal.market_pulse.windows import WindowService


def catalog(native="NEWUSDT", **changes):
    return dict(symbol=native, baseAsset=native[:-4], quoteAsset="USDT", marginAsset="USDT",
                status="TRADING", contractType="PERPETUAL", filters=[dict(filterType="PRICE_FILTER", tickSize=".01")],
                **changes)


def trade(now, native="NEWUSDT", **changes):
    return {"e": "aggTrade", "s": native, "T": int(now * 1000), "a": 1, "m": False,
            "p": "10", "q": "2", "f": 10, "l": 100, **changes}


def normalize(payload, now):
    return normalize_binance_trades(payload, binance_spec(catalog()), receive_time_ms=int(now * 1000),
                                    source_epoch="epoch", raw_ref="raw:1")[0]


@pytest.mark.parametrize("base", ["NEW", "IDOL", "1000PEPE", "UNSEEN987", "币安人生"])
def test_catalog_accepts_new_exact_instruments_without_symbol_lists(base):
    spec = binance_spec(catalog(base + "USDT"))
    assert identity(binance_key(spec.native_market_id)) == ("binance", base + "USDT")
    assert spec.base_quantity("2") == 2
    assert spec.to_payload()["tick_size"] == "0.01"


@pytest.mark.parametrize("field,value", [("status", "SETTLING"), ("contractType", "CURRENT_QUARTER"),
                                        ("marginAsset", "USDC"), ("quoteAsset", "BTC")])
def test_unverified_contracts_do_not_enter_usdtm_universe(field, value):
    with pytest.raises(ValueError):
        binance_spec({**catalog(), field: value})


def test_aggregate_semantics_and_aggressor_are_not_individual_fills():
    row = normalize(trade(100), 100)
    assert row.aggregation_kind == "aggregated" and row.execution_count == 1
    assert row.base_amount == 2 and row.quote_amount == 20 and row.side == "buy"
    assert normalize(trade(100, m=True), 100).side == "sell"
    with pytest.raises(ValueError):
        normalize(trade(100, m="false"), 100)


def test_relative_allocation_is_venue_neutral_and_scale_invariant(tmp_path):
    now = 1_000_000
    selector = UnifiedAttention(None, slots=3, detail_slots=1)
    target = int((now * 1000 - 5000) // 60000 * 60000 - 60000)
    # A previously unknown Binance instrument wins by its own relative activity,
    # even with a tiny dollar volume against a large quiet Hyperliquid market.
    selector.activity = {"BINANCE:UNSEEN987": dict(status="ready", start_ms=target, ratios=dict(trade_count=20))}
    selector.archive = Archive(tmp_path / "decisions")
    selector.archive.record("policy", asdict(selector.engine.policy), now - 1)
    selector.choose([dict(symbol="BTC", mark_price=100000, volume_24h=10**12),
                     dict(symbol="BINANCE:UNSEEN987", mark_price=.00001, volume_24h=.001)], now)
    assert selector.plan["detail"] == ["BINANCE:UNSEEN987"]
    selector.close()
    assert replay(selector.archive.path)["verified"]
    # The same policy also selects HL when only its relative score is higher.
    other = UnifiedAttention(None, slots=3, detail_slots=1)
    other.activity = {"BTC": dict(status="ready", start_ms=target, ratios=dict(trade_count=30))}
    other.choose([dict(symbol="BTC", mark_price=1), dict(symbol="BINANCE:UNSEEN987", mark_price=100000)], now)
    assert other.plan["detail"] == ["BTC"]


@pytest.mark.asyncio
async def test_only_attention_plan_creates_detail_and_release_unsubscribes(tmp_path):
    archive, sink, gaps = RawArchive(tmp_path, "binance"), AsyncMock(), AsyncMock()
    collector = BinanceCollector(CollectorConfig("binance", (), str(tmp_path), ""), archive, sink, gaps)
    collector.dynamic = True
    collector._ws = SimpleNamespace(closed=False, send_json=AsyncMock())
    collector._epoch = "epoch"
    try:
        collector.specs = {s: binance_spec(catalog(s)) for s in ("NEWUSDT", "OTHERUSDT")}
        await collector.sync_overview()
        sent = collector._ws.send_json.call_args.args[0]
        assert set(sent["params"]) == {"newusdt@kline_1m", "otherusdt@kline_1m"}
        assert not collector.config.markets and not collector.confirmed
        await collector.change_markets(["BINANCE:OTHER"])
        sub = collector._ws.send_json.call_args.args[0]
        assert sub["params"] == ["otherusdt@aggTrade"]
        await collector._handle_payload(json.dumps(dict(result=None, id=sub["id"])), "epoch")
        assert collector.confirmed == {"OTHERUSDT"}
        await collector.change_markets(())
        assert collector._ws.send_json.call_args.args[0]["method"] == "UNSUBSCRIBE"
        assert not collector.confirmed and not collector.pending_ack
        gaps.released.assert_awaited_once()
    finally:
        archive.close()


@pytest.mark.asyncio
async def test_ingest_dedup_aggregate_window_and_no_phantom_execution_window():
    store = InMemoryMarketPulseStore()
    tracker = GapTracker(store)
    windows = WindowService(store)
    ingest = IngestionService(store, windows, tracker)
    await store.register_instruments([binance_spec(catalog())])
    await tracker.on_connected("binance", "NEWUSDT", "aggTrade", "epoch", 0)
    event = normalize(trade(30), 30)
    first = await ingest.append_batch([event], now_ms=30_000)
    again = await ingest.append_batch([event], now_ms=30_000)
    assert first["accepted"] == again["duplicates"] == 1
    await tracker.on_heartbeat("binance", "NEWUSDT", "aggTrade", 66_000)
    await windows.close_due(now_ms=66_000)
    row = await store.get_window("binance", "NEWUSDT", "aggTrade", "aggregated", 0)
    assert row and row.event_count == 1 and row.buy_quote == 20
    assert await store.get_window("binance", "NEWUSDT", "aggTrade", "execution", 0) is None
    # HL labels in the same shared plan cannot release a Binance assignment.
    control = GapControlService(store, tracker)
    await control.release_outside_attention("binance", ["BTC", "BINANCE:NEW"], 70_000)
    assert (await store.get_coverage("binance", "NEWUSDT", "aggTrade")).state != "outside_attention"
    await control.release_outside_attention("binance", ["BTC"], 80_000)
    assert (await store.get_coverage("binance", "NEWUSDT", "aggTrade")).state == "outside_attention"


@pytest.mark.asyncio
async def test_flow_isolated_from_hyperliquid_dedup_and_release():
    now = time.time()
    engine = OrderFlowEngine([], include_binance=False)
    engine.selective = engine.pulse_external = True
    await engine.set_attention_symbols(["NEW", "BINANCE:NEW"])
    row = normalize(trade(now), now).to_payload()
    assert engine.accept_execution(row)
    assert not engine.accept_execution(row)
    assert len(engine.recent_trades["BINANCE:NEW"]) == 1
    assert len(engine.recent_trades["NEW"]) == 0
    assert engine.get_cumulative_cvd("BINANCE:NEW")["hyperliquid"] == 0
    assert engine.get_cumulative_cvd("BINANCE:NEW")["binance"] == 20
    await engine.set_attention_symbols(["NEW"])
    assert not engine.accept_execution({**row, "trade_id": "2"})
    assert not engine.accept_execution({**row, "delivery_kind": "recovery"})


def test_native_binance_facts_keep_venue_and_aggregate_units(tmp_path):
    now = 100
    facts = Facts(version=2)
    archive = Archive(tmp_path / "history")
    archive.record("source", dict(fact_version=2, assignment_mode="external_plan.v1"), now)
    assignment = dict(symbol="BINANCE:NEW", assigned_at=now, state="focus", previous="scout", at=now, score=10)
    facts.apply(archive.record("assignment", assignment, now))
    depth = dict(e="depthUpdate", s="NEWUSDT", T=100_000, u=1,
                 b=[["9.99", "2"], ["9.98", "1"]], a=[["10.01", "1"], ["10.02", "1"]])
    facts.apply(archive.record("raw", dict(source="binance_book", text=json.dumps(depth)), now, epoch="book"))
    row = normalize(trade(now), now).to_payload()
    row["event_seq"] = 1
    facts.apply(archive.record("execution", row, now, epoch="epoch"))
    card = facts.card("BINANCE:NEW", now)
    archive.record("facts", card, now)
    archive.close()
    assert replay(archive.path)["verified"]
    packet = build_packet(dict(policy_version=4, last_scout_at=now), {**assignment, "facts": card}, now)
    sources = {f["source"] for f in packet["facts"]}
    assert "binance_depth20_snapshot" in sources and "hyperliquid_l2_snapshot" not in sources
    count = next(f for f in packet["facts"] if f["id"] == "tape.observed_trade_count_60s")
    assert count["unit"] == "aggregates" and count["value"] == 1
    assert not facts.books.get("NEW")


@pytest.mark.asyncio
async def test_market_catalogue_is_dynamic_and_unknown_funding_is_not_zero(monkeypatch):
    now = time.time()
    owner = MarketData()
    responses = {"/fapi/v1/exchangeInfo": dict(symbols=[catalog("SURPRISEUSDT")]),
                 "/fapi/v1/ticker/24hr": [dict(symbol="SURPRISEUSDT", lastPrice="10", quoteVolume="2",
                                              priceChangePercent="3")],
                 "/fapi/v1/premiumIndex": [dict(symbol="SURPRISEUSDT", markPrice="10", indexPrice="10",
                                                time=int(now * 1000))]}

    async def get(session, path, **kwargs):
        return responses[path]

    monkeypatch.setattr("hyperdata_terminal.data_layer.binance_market.get_json", get)
    await refresh_market(owner)
    assert set(owner.assets) == {"BINANCE:SURPRISE"}
    assert owner.observed_fields["BINANCE:SURPRISE"] == {"price", "volume_24h"}


def test_tiger_uses_exact_binance_contract_without_cross_venue_alias(monkeypatch):
    monkeypatch.setenv("SCALPER_TIGER_LINK_GROUP", "S")
    tiger = TigerBridge()
    assert tiger.command("BINANCE:1000PEPE")["symbol"] == "1000PEPEUSDT"
    assert tiger.command("BINANCE:NEW")["symbol"] == "NEWUSDT"


def test_binance_pulse_has_one_venue_and_does_not_claim_cross_confirmation():
    pulse = PulseContext("http://unused", ["BINANCE:NEW"])
    pulse.status, pulse.updated_at = "connected", 125
    pulse.channels = [dict(venue="binance", native_market_id="NEWUSDT", channel="aggTrade", stale=False)]
    pulse.windows = [dict(venue="binance", native_market_id="NEWUSDT", channel="aggTrade",
                          aggregation_kind="aggregated", window_start_ms=60_000, quality="observed_no_detected_gap",
                          provisional=False, version=1, execution_count=3, event_count=3,
                          buy_quote="30", sell_quote="10", coverage={})]
    result = pulse.facts("BINANCE:NEW", 125)
    assert result["status"] == "ready" and result["flow"] == "single_venue_buy"
    assert len(result["venues"]) == 1
    assert result["venues"][0]["execution_count"] is None
    assert result["venues"][0]["aggregate_count"] == 3


@pytest.mark.asyncio
async def test_binance_catalogue_tape_and_collector_status_cross_real_http_contract():
    from aiohttp.test_utils import TestServer

    store = InMemoryMarketPulseStore()
    tracker = GapTracker(store)
    app = create_ingest_app(store, IngestionService(store, WindowService(store), tracker),
                            GapControlService(store, tracker), token="test")
    async with TestServer(app) as server:
        client = HttpIngestClient(str(server.make_url("/")), token="test")
        try:
            await client.register_instruments([binance_spec(catalog())])
            now = int(time.time() * 1000)
            plan = UnifiedAttention(None, slots=2, detail_slots=1)
            target = int((now - 5000) // 60000 * 60000 - 60000)
            plan.activity = {"BINANCE:NEW": dict(status="ready", start_ms=target, ratios=dict(trade_count=8))}
            plan.choose([dict(symbol="BINANCE:NEW", mark_price=10)], now / 1000)
            await store.set_control("attention", plan.plan, now)
            await client.collector_status("binance", {"BINANCE:NEW": dict(status="observing")},
                                          desired=["BINANCE:NEW"], confirmed=["NEWUSDT"], socket_connected=True)
            saved = await store.get_control("collector:binance")
            assert saved["confirmed"] == ["NEWUSDT"]
            result = await client.append_batch([normalize(trade(now / 1000), now / 1000)])
            assert result["accepted"] == 1
        finally:
            await client.close()
