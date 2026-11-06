"""Attention fairness, lifecycle, raw integrity and deterministic research replay."""
import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from extensions.scalper.attention import Attention, AttentionPolicy
from extensions.scalper.history.archive import Archive, read_archive
from extensions.scalper.history.facts import Facts
from extensions.scalper.history.replay import replay
from extensions.scalper.history.runtime import Observation, configure
from hyperdata_terminal.data_layer.orderbook import OrderBookEngine
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine, Trade


def warm(attention, symbols=None):
    scales = symbols or {"SMALL": 1, "BTC": 1000000}
    for at in range(14):
        attention.update({s: dict(price=scale, volume_24h=scale * (1000 + at))
                          for s, scale in scales.items()}, at * 5)
    return scales


def burst(attention, at=70, multiplier=20, scales=None):
    scales = scales or {"SMALL": 1, "BTC": 1000000}
    return attention.update({s: dict(price=scale, volume_24h=scale * (1013 + multiplier))
                             for s, scale in scales.items()}, at)


def test_relative_attention_is_invariant_to_dollar_scale_and_not_top20():
    attention = Attention()
    warm(attention)
    burst(attention)
    assert set(attention.active) == {"SMALL", "BTC"}
    assert attention.active["SMALL"]["score"] == attention.active["BTC"]["score"] == 20
    assert attention.policy.min_activity_usd == 0


def test_small_instrument_beats_high_volume_but_quiet_coin_at_capacity():
    attention = Attention(AttentionPolicy(slots=1))
    warm(attention)
    attention.update({"SMALL": dict(price=1, volume_24h=1033),
                      "BTC": dict(price=1000000, volume_24h=1014e6)}, 70)
    assert list(attention.active) == ["SMALL"]


def test_burst_compares_with_past_baseline_and_has_no_price_move_requirement():
    attention = Attention()
    warm(attention)
    burst(attention)
    assert attention.scouts["SMALL"]["activity_ratio"] == 20
    assert attention.scouts["SMALL"]["move_bps"] == 0


def test_no_dollar_floor_hides_sub_dollar_activity():
    attention = Attention()
    scales = {"TINY": 0.00001}
    warm(attention, scales)
    burst(attention, scales=scales)
    assert "TINY" in attention.active
    assert attention.scouts["TINY"]["activity_usd_lower_bound"] < 1


def test_zero_baseline_gets_probe_not_infinite_priority():
    attention = Attention()
    for at in range(14):
        attention.update({"QUIET": dict(price=1, volume_24h=1)}, at * 5)
    attention.update({"QUIET": dict(price=1, volume_24h=1.000001)}, 70)
    assert attention.active["QUIET"]["state"] == "probe"
    assert attention.active["QUIET"]["score"] == 3


def test_negative_rolling_volume_is_unknown_not_negative_trades():
    attention = Attention()
    warm(attention)
    attention.update({"SMALL": dict(price=1, volume_24h=1000)}, 70)
    assert attention.scouts["SMALL"]["activity_ratio"] is None
    assert attention.scouts["SMALL"]["activity_usd_lower_bound"] is None
    assert not attention.active


def test_focus_hysteresis_and_cooling_keep_one_episode():
    attention = Attention(AttentionPolicy(version=2))
    warm(attention)
    burst(attention)
    for at in range(75, 135, 5):
        attention.update({"SMALL": dict(price=1, volume_24h=1033),
                          "BTC": dict(price=1e6, volume_24h=1033e6)}, at)
    assert attention.active["SMALL"]["state"] == "cooling"
    assert attention.active["SMALL"]["assigned_at"] == 70
    attention.update({}, 200)
    assert not attention.active


def test_unobserved_symbol_cannot_stay_in_focus_forever():
    attention = Attention()
    warm(attention)
    burst(attention)
    changes = attention.update({}, 101)
    assert not attention.active
    assert {c["reason"] for c in changes} == {"scout_stale"}


@pytest.mark.parametrize("fields", [dict(slots=9), dict(slots=1.5), dict(probe_ratio=float("nan")),
                                  dict(min_activity_usd=-1), dict(release_ratio=4)])
def test_invalid_policy_rejected(fields):
    with pytest.raises(ValueError):
        AttentionPolicy(**fields)


def raw(book, at=100, sequence=1, epoch="book1"):
    return dict(kind="raw", received_at=at, sequence=sequence, run="test", epoch=epoch,
                payload={"text": json.dumps(book)})


def book(at=100, scale=1):
    return {"channel": "l2Book", "data": {"coin": "SMALL", "time": at * 1000, "levels": [
        [{"px": str(.99 * scale), "sz": "100"}, {"px": str(.98 * scale), "sz": "1"}],
        [{"px": str(1.01 * scale), "sz": "1"}, {"px": str(1.02 * scale), "sz": "1"}]]}}


def test_density_relative_size_is_invariant_and_no_minimum_dollars():
    for scale in (1, 0.00001, 1e6):
        facts = Facts()
        facts.apply(raw(book(scale=scale)))
        densities = facts.card("SMALL", 100)["densities"]
        assert len(densities) == 1
        assert densities[0]["relative_size"] == pytest.approx(99 / 1.015)


def test_density_age_is_observation_only_and_resets_on_gap_or_epoch():
    facts = Facts()
    facts.apply(raw(book(), 100))
    facts.apply(raw(book(105), 105, 2))
    assert facts.card("SMALL", 105)["densities"][0]["observed_age_seconds"] == 5
    facts.apply(raw(book(110), 110, 3, "new_epoch"))
    assert facts.card("SMALL", 110)["densities"][0]["observed_age_seconds"] == 0
    facts.apply(raw(book(140), 140, 4, "new_epoch"))
    assert facts.card("SMALL", 140)["densities"][0]["observed_age_seconds"] == 0
    assert facts.card("SMALL", 156)["status"] == "waiting_for_fresh_book"


def test_out_of_view_density_does_not_inherit_unobserved_age():
    facts = Facts()
    facts.apply(raw(book()))
    missing = book(105)
    missing["data"]["levels"][0] = missing["data"]["levels"][0][1:]
    facts.apply(raw(missing, 105, 2))
    facts.apply(raw(book(110), 110, 3))
    assert facts.card("SMALL", 110)["densities"][0]["first_seen"] == 110


def test_raw_trade_dedup_uses_time_coin_and_tid():
    facts = Facts()
    facts.apply(raw(book()))
    trade = {"coin": "SMALL", "time": 100000, "px": "1", "sz": "2", "tid": 1, "side": "B"}
    message = {"channel": "trades", "data": [trade, trade]}
    facts.apply(raw(message, 100, 2, "trades1"))
    assert facts.card("SMALL", 100)["observed_turnover_60s_usd"] == 2
    trade["time"] = 101000
    facts.apply(raw(message, 101, 3, "trades1"))
    assert facts.card("SMALL", 101)["observed_turnover_60s_usd"] == 4


def test_archive_checksum_drain_rotation_and_replay(tmp_path):
    archive = Archive(tmp_path, segment_records=2)
    facts = Facts()
    archive.record("policy", asdict(AttentionPolicy()), 99)
    event = archive.record("raw", {"text": json.dumps(book())}, 100, epoch="book1")
    facts.apply(event)
    archive.record("facts", facts.card("SMALL", 100), 100)
    archive.close()
    assert archive.report()["complete"]
    assert len(list(read_archive(archive.path))) == 3
    assert replay(archive.path)["checksum"] == replay(archive.path)["checksum"]
    segment = archive.path / archive.segments[0]["file"]
    segment.write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        list(read_archive(archive.path))


def test_archive_invalid_payload_and_unfinished_run_cannot_claim_complete(tmp_path):
    archive = Archive(tmp_path)
    assert archive.record("raw", {"price": float("nan")}, 0) is None
    archive.close()
    assert not archive.report()["complete"]
    with pytest.raises(ValueError, match="incomplete"):
        list(read_archive(archive.path))


@pytest.mark.asyncio
async def test_observation_uses_all_assets_and_replays_assignments_without_llm(tmp_path):
    asset = SimpleNamespace(price=1., volume_24h=1000.)
    hub = SimpleNamespace(market=SimpleNamespace(assets={"NOT_TOP20": asset},
                          observed_fields={"NOT_TOP20": {"price", "volume_24h"}}, observed_at=0),
                          orderbook=SimpleNamespace(set_symbols=AsyncMock()),
                          orderflow=SimpleNamespace(set_attention_symbols=AsyncMock()))
    observation = Observation(hub, tmp_path)
    for i in range(14):
        asset.volume_24h = 1000 + i
        hub.market.observed_at = i * 5
        await observation.tick(i * 5)
    asset.volume_24h = 1033
    hub.market.observed_at = 70
    await observation.tick(70)
    assert list(observation.attention.active) == ["NOT_TOP20"]
    hub.orderbook.set_symbols.assert_awaited_with(["NOT_TOP20"])
    frame = book(70)
    frame["data"]["coin"] = "NOT_TOP20"
    observation.raw("book", json.dumps(frame), "book1", 70, 1)
    observation.raw("trades:NOT_TOP20", json.dumps(dict(channel="trades", data=[dict(
        coin="NOT_TOP20", px="1", sz="2", time=70000, tid=1, side="B")])), "trade1", 70, 2)
    asset.volume_24h = 1034
    hub.market.observed_at = 75
    await observation.tick(75)
    card = observation.report(75)["active"][0]["facts"]
    assert card["observed_trade_count_60s"] == 1 and card["observed_trade_imbalance"] == 1
    await observation.close()
    assert replay(observation.archive.path)["verified"]


@pytest.mark.asyncio
async def test_selective_trade_subscription_preserves_survivor_and_resets_new_windows():
    engine = OrderFlowEngine([], include_binance=False)
    engine.selective = True
    engine._running = True
    engine._hl_shard_plan = [[] for _ in range(8)]
    import asyncio
    async def idle(index):
        await asyncio.Future()
    engine._run_forever = idle
    await engine.set_attention_symbols(["SMALL", "BTC"])
    survivor = next(engine._attention_tasks[i] for i, names in enumerate(engine._hl_shard_plan)
                    if names == ["SMALL"])
    engine.buckets["BTC"]["1m"].add_trade(Trade(1, "BTC", "buy", 1, 10, 10))
    await engine.set_attention_symbols(["SMALL"])
    await engine.set_attention_symbols(["SMALL", "BTC"])
    assert survivor in engine._attention_tasks.values() and not survivor.cancelled()
    bucket = engine.buckets["BTC"]["1m"]
    assert not bucket.trades and bucket.buy_volume == 0 and bucket.trade_count == 0
    await engine.stop()


@pytest.mark.asyncio
async def test_book_subscription_removed_state_not_reused():
    engine = OrderBookEngine([])
    await engine.set_symbols(["SMALL"])
    engine._update_book("SMALL", book()["data"])
    assert engine.get_snapshot("SMALL") is not None
    await engine.set_symbols([])
    assert engine.get_snapshot("SMALL") is None


def test_demo_never_records_live_history(tmp_path, monkeypatch):
    monkeypatch.setenv("HYPERDATA_HISTORY_DIR", str(tmp_path / "not-created"))
    assert configure(SimpleNamespace(demo=True)) is None
    assert not (tmp_path / "not-created").exists()


def test_archive_overflow_explicitly_invalidates_run(tmp_path, monkeypatch):
    import queue
    archive = Archive(tmp_path)
    def fail(_line):
        raise queue.Full()
    monkeypatch.setattr(archive.queue, "put_nowait", fail)
    assert archive.record("raw", {}, 1) is None
    archive.close()
    assert archive.dropped == 1 and not archive.report()["complete"]
    with pytest.raises(ValueError, match="incomplete"):
        list(read_archive(archive.path))


def test_malformed_frame_kept_in_archive_and_never_creates_facts(tmp_path):
    archive = Archive(tmp_path)
    event = archive.record("raw", {"text": "not json"}, 1)
    facts = Facts()
    facts.apply(event)
    archive.close()
    assert list(read_archive(archive.path))[0]["payload"]["text"] == "not json"
    assert facts.errors == 1 and not facts.books


def test_late_frame_after_release_cannot_resurrect_density():
    facts = Facts()
    facts.apply(dict(kind="assignment", payload={"symbol": "SMALL", "previous": "scout", "state": "focus"}))
    facts.apply(raw(book()))
    facts.apply(dict(kind="assignment", payload={"symbol": "SMALL", "previous": "focus", "state": "scout"}))
    facts.apply(raw(book(101), 101, 2))
    assert not facts.books


@pytest.mark.asyncio
async def test_remote_idle_attention_does_not_fall_back_to_top20(monkeypatch):
    from extensions.scalper.remote import RemoteHub
    hub = RemoteHub("http://unused")
    import time
    now = time.time()
    async def get(path, **params):
        if path == "/v1/attention":
            return dict(enabled=True, context_only=True, active=[])
        assert path == "/v1/market" and params == {"limit": 1000}
        return dict(as_of=now, assets=[dict(symbol="BTC", price=1, mark_price=1)])
    monkeypatch.setattr(hub, "_get", get)
    monkeypatch.setattr(hub, "_refresh_health", AsyncMock())
    await hub.refresh_snapshot()
    assert hub.ready and not hub.get_all_assets() and hub.status.market_data == "connected"


def test_manual_context_card_never_requires_or_calls_model():
    from extensions.scalper.service import Screener
    facts = Facts()
    facts.apply(raw(book()))
    service = Screener.__new__(Screener)
    service.hub = SimpleNamespace(attention=dict(context_only=True, active=[dict(
        symbol="SMALL", facts=facts.card("SMALL", 100))]))
    result = service.request_analysis("small", "1m", 101)
    assert "факты наблюдения" in result and "наблюдается 0 с" in result
    assert "Нет свежей" in service.request_analysis("SMALL", "1m", 116)


def test_forward_outcomes_keep_censored_and_observed_background_separate(tmp_path):
    from extensions.scalper.history.evaluate import evaluate
    archive = Archive(tmp_path / "capture")
    policy = AttentionPolicy()
    attention = Attention(policy)
    archive.record("policy", asdict(policy), 0)
    for at in range(0, 121, 5):
        observations = {"SMALL": dict(price=1 + at / 10000, volume_24h=1000 + at)}
        archive.record("scout", observations, at)
        for change in attention.update(observations, at):
            archive.record("assignment", change, at)
    archive.close()
    result = evaluate(archive.path, tmp_path / "evaluation")
    assert result["groups"]["background_unmatched/60s"]["observed"] == 1
    assert result["groups"]["background_unmatched/60s"]["mean_abs_end_bps"] == pytest.approx(60)
    assert result["groups"]["background_unmatched/300s"]["censored"] == 1
    assert result["groups"]["background_unmatched/900s"]["censored"] == 1
