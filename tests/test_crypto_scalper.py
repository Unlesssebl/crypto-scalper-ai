import time
from types import SimpleNamespace

import pytest

from extensions.scalper.activity import (
    active_anomalies,
    btc_context,
    candle_indicators,
    finding_strength,
    market_risk_context,
    tape_baseline,
)
from extensions.scalper.journal import Journal
from extensions.scalper.market import capture
from extensions.scalper.profile import calculate_profiles, profile_context, sample_window
from extensions.scalper.quality import calculate_quality
from extensions.scalper.service import minute_gate
from extensions.scalper.telegram import digest_text
from hyperdata_terminal.data_layer.market_data import AssetInfo, MarketData
from hyperdata_terminal.data_layer.orderbook import OrderBookEngine
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine, ShardState, Trade


@pytest.fixture
def market():
    now = time.time()
    symbol = "kPEPE"
    asset = AssetInfo(symbol, 12, 0, 300, 400, 0, 12, 12)
    engine = OrderFlowEngine([symbol], include_binance=False)
    engine._hl_shard_plan = [[symbol]]
    engine._hl_shard_state = {0: ShardState(connected_at=now - 301)}
    engine._process_trade(Trade(now - 299, symbol, "buy", 12, 10, 120), venue="hyperliquid")
    engine._process_trade(Trade(now - 1, symbol, "buy", 12, 10, 120), venue="hyperliquid")
    book = OrderBookEngine([symbol])
    book._update_book(symbol, {"levels": [[{"px": "11", "sz": "2"}], [{"px": "13", "sz": "1"}]]})
    hub = SimpleNamespace(
        demo=False, market_timestamp=lambda: now - 1, market_age=lambda: 1,
        market=SimpleNamespace(observed_fields={symbol: {"price", "volume_24h", "open_interest"}}),
        orderflow=engine, get_orderbook=book.get_snapshot, get_all_assets=lambda: [asset],
        status=SimpleNamespace(mode="live", market_data="connected"),
    )
    return now, hub, asset


def model_reply(direction):
    return {
        "direction": direction, "summary": "Инициатива меняется",
        "scenario": "Если спрос сохранится, покупатели могут удержать инициативу",
        "arguments": [{"metric": "cvd", "reason": "Односторонний поток"},
                      {"metric": "imbalance", "reason": "Стакан даёт дополнительный контекст"}],
        "contradictions": [],
    }


def test_capture_uses_native_flow_and_venue_quality(market):
    now, hub, asset = market
    snapshot, native = capture(hub, asset, "5m", now)
    assert native["native_signal"] == "STRONG_BULL"
    assert snapshot["metrics"]["cvd"]["value"] == 240
    assert snapshot["metrics"]["first_trade_price"]["updated_at"] == now - 299
    assert snapshot["metrics"]["last_trade_price"]["updated_at"] == now - 1
    assert snapshot["metrics"]["funding_rate"]["value"] is None
    assert snapshot["metrics"]["spread"]["value"] == 2
    assert snapshot["metrics"]["liquidations"]["quality"] == "unavailable"
    assert capture(hub, asset, "5m", now - 10)[1]["quality"] == "clock_skew"
    hub.orderflow._hl_shard_state[0].connected_at = now - 10
    assert capture(hub, asset, "5m", now)[1]["quality"] == "warming_up"
    hub.orderflow._hl_shard_state[0].connected_at = 0
    assert capture(hub, asset, "5m", now)[1]["direction"] == "NOT_AVAILABLE"











def test_journal_has_one_service_owner(tmp_path):
    path = tmp_path / "scalper.sqlite"
    journal = Journal(path)
    try:
        with pytest.raises(RuntimeError, match="journal lock"):
            Journal(path)
    finally:
        journal.close()
    Journal(path).close()






def test_quality_counts_each_attempt_once_and_checks_each_horizon(tmp_path):
    path = tmp_path / "scalper.sqlite"
    journal = Journal(path)
    now = 100_000

    def record(key, window, at, price, status, direction=None):
        snapshot = {"symbol": key, "window": window, "captured_at": at,
                    "metrics": {"price": {"value": price, "quality": "ok"}}}
        event = {"type": "orderflow_transition", "detected_at": at - 9}
        if key == "long":
            event["anomalies"] = ["tape_burst"]
        journal.record(key, event, snapshot, {})
        if status:
            journal.finish(key, {"status": status, "direction": direction}, {}, at + 1)

    try:
        record("long", "1m", now - 350, 100, "ok", "BULLISH")
        record("short", "1m", now - 350, 100, "ok", "BEARISH")
        record("neutral", "1m", now - 30, 100, "ok", "NEUTRAL")
        record("insufficient", "1m", now - 30, 100, "ok", "INSUFFICIENT_DATA")
        record("failed", "1m", now - 30, 100, "network_error")
        record("skipped", "1m", now - 30, 100, "expired")
        record("pending", "1m", now - 10, 100, None)
        record("other", "5m", now - 350, 100, "ok", "BEARISH")
        record("old", "1m", now - 90_000, 100, "ok", "BULLISH")
        journal.observe(now - 285, {"long": (101, now - 286), "short": (100, now - 286)})
        journal.observe(now - 45, {"long": (98, now - 46), "short": (99, now - 46)})
        journal.observe(now, {})

        report = calculate_quality(path, now, ("1m", "5m"))["windows"]
        assert report["1m"]["attempts"] == {
            "directional": 2, "neutral": 1, "insufficient": 1, "error": 1, "skipped": 1, "pending": 1,
        }
        assert report["1m"]["horizons"][1] == {
            "correct": 1, "incorrect": 0, "flat": 1, "pending": 0, "missing": 0,
        }
        assert report["1m"]["horizons"][5] == {
            "correct": 1, "incorrect": 1, "flat": 0, "pending": 0, "missing": 0,
        }
        assert report["1m"]["horizons"][15]["pending"] == 2
        assert report["5m"]["horizons"][1]["missing"] == 1
        tape = report["1m"]["triggers"]["tape_burst"]
        assert tape["horizons"][1]["correct"] == 1
        assert tape["horizons"][5]["incorrect"] == 1
        assert tape["median_delay_seconds"] == 10 and tape["delay_samples"] == 1
    finally:
        journal.close()


def test_activity_uses_closed_candles_and_nonoverlapping_trade_baseline():
    candles = [{"timestamp": i * 60_000, "high": 101, "low": 99, "close": 100, "trade_count": 10}
               for i in range(20)]
    candles[-2].update(high=104, low=96, trade_count=500)
    candles[-1].update(high=140, low=50, trade_count=900)
    now = 19 * 60 + 10
    indicators = candle_indicators(candles, 60, now)
    assert indicators["closed_at"] == 19 * 60
    assert indicators["range_ratio"] == pytest.approx(4)
    assert indicators["natr"] == pytest.approx((13 * 2 + 8) / 14)
    assert tape_baseline(indicators, now, 60) == 10
    snapshot = {"metrics": {"activity_ratio": {"quality": "ok", "value": 2},
                            "trade_count": {"value": 20},
                            "range_ratio": {"quality": "ok", "value": 4}}}
    assert active_anomalies(snapshot) == {"tape_burst", "range_burst"}


async def test_native_candle_request_preserves_market_symbol_and_trade_count(monkeypatch):
    market = MarketData()
    market.assets["kPEPE"] = None

    async def post(session, request):
        assert request["type"] == "candleSnapshot"
        assert request["req"]["coin"] == "kPEPE"
        assert request["req"]["interval"] == "1m"
        return [{"t": 0, "o": "1", "h": "2", "l": "1", "c": "2", "v": "3", "n": 7}]

    monkeypatch.setattr(market, "_post", post)
    assert (await market.get_candles("kPEPE", "1m"))[0]["trade_count"] == 7






def test_minute_gate_flags_wide_spread_and_missing_book(market):
    now, hub, asset = market
    snapshot, _native = capture(hub, asset, "1m", now)
    assert minute_gate(snapshot) == "ready"
    snapshot["metrics"]["natr"].update(value=0.5, quality="ok")
    assert minute_gate(snapshot) == "wide_spread"
    snapshot["metrics"]["spread"]["quality"] = "stale"
    assert minute_gate(snapshot) == "book_unavailable"






def test_context_trial_scores_three_arms_from_a_common_post_reply_price(tmp_path):
    path = tmp_path / "scalper.sqlite"
    journal = Journal(path)
    try:
        snapshot = {"symbol": "BTC", "window": "1m", "captured_at": 100,
                    "metrics": {"price": {"quality": "ok", "value": 90}}}
        journal.record("pair", {}, snapshot, {"direction": "BEARISH"})
        journal.finish("pair", {"status": "ok", "direction": "BULLISH"}, {}, 103)
        journal.record_context_trial("pair", {"status": "ok", "direction": "NEUTRAL"}, 110)
        journal.observe(110, {"BTC": (95, 109)})
        journal.observe(115, {"BTC": (100, 114)})
        journal.observe(172, {"BTC": (99, 171)})
        report = calculate_quality(path, 172, ("1m", "5m"))["context_experiment"]["1m"]
        assert (report["attempted"], report["matched"], report["changed"]) == (1, 1, 1)
        assert report["horizons"][1]["hyperdata"]["correct"] == 1
        assert report["horizons"][1]["baseline"]["no_direction"] == 1
        assert report["horizons"][1]["senior"]["incorrect"] == 1
        assert report["horizons"][5]["senior"]["pending"] == 1
    finally:
        journal.close()


def test_instrument_profiles_count_regular_windows_and_filter_missing_metrics(market, tmp_path):
    now, hub, asset = market
    slot = int(now // 60) * 60
    snapshot, _ = capture(hub, asset, "1m", now)
    snapshot["captured_at"] = slot + 4
    sample = sample_window(snapshot, 60)
    assert sample[:3] == (asset.symbol, "1m", slot)
    assert sample[3] is not None
    path = tmp_path / "profiles.sqlite"
    journal = Journal(path)
    try:
        # Reference intervals precede the current rolling window.
        samples = [(asset.symbol, "1m", slot - (i + 1) * 60, i + 1, None, 2.0, 0.0, None) for i in range(20)]
        journal.record_instrument_samples(samples + samples)
        profile = calculate_profiles(path, now, [asset.symbol])[(asset.symbol, "1m")]
        period = profile["periods"][7]
        assert period["windows"] == 20
        assert period["metrics"]["trades"] == {"samples": 20, "median": 10.5, "p90": 18.0}
        assert "natr" not in period["metrics"]
        assert "trades" in profile_context(profile)["periods"][7]
    finally:
        journal.close()


def test_finding_strength_requires_price_confirmation_for_critical_rank(market):
    now, hub, asset = market
    snapshot, native = capture(hub, asset, "1m", now)
    metrics = snapshot["metrics"]
    for name, value in {
        "spread": 0.001, "natr": 1.0, "first_trade_price": 12, "last_trade_price": 12,
        "activity_ratio": 1, "trade_count": 20, "range_ratio": 1,
    }.items():
        metrics[name].update(value=value, quality="ok")

    assert finding_strength(snapshot, native)["rank"] == 1
    metrics["activity_ratio"]["value"] = 2.5
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["range_ratio"]["value"] = 3
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["last_trade_price"]["value"] = 12.05
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["last_trade_price"]["value"] = 12.14
    assert finding_strength(snapshot, native)["rank"] == 3
    metrics["last_trade_price"]["value"] = 12.20
    assert finding_strength(snapshot, native)["rank"] == 4
    metrics["spread"]["value"] = 0.04
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["spread"]["value"] = 0.001
    metrics["spread"]["quality"] = "stale"
    assert finding_strength(snapshot, native)["rank"] == 2
    metrics["ofi"]["quality"] = "stale"
    assert finding_strength(snapshot, native)["rank"] == 0


def test_btc_context_requires_aligned_closed_returns_and_ignores_flat_series():
    returns = {300 * i: (i % 7 - 3) / 10 for i in range(1, 37)}
    btc = {"returns": returns}
    asset = {"returns": {stamp: -change for stamp, change in returns.items()}}
    context = btc_context(asset, btc, 36 * 300 + 10)
    assert context["correlation"] == pytest.approx(-1)
    assert context["samples"] == 36
    assert context["relative_last_return_pp"] == pytest.approx(-2 * returns[36 * 300])
    assert btc_context(asset, btc, 36 * 300 + 500) is None
    assert btc_context({"returns": {stamp: 0 for stamp in returns}}, btc, 36 * 300) is None
    assert btc_context({"returns": dict(list(returns.items())[1:])}, btc, 36 * 300) is None
    assert btc_context(btc, btc, 36 * 300) is None




def test_market_risk_context_flags_skew_and_iv():
    context = {
        "lsr": {"BTC": {"long_short_ratio": 1.8, "source": "okx"}},
        "iv": {"BTC": {"mark_iv": 65.0}},
        "basis": {"BTC": {"basis_pct": -0.6, "source": "coinbase"}},
        "liquidations": {"confirmed_count": 7, "confirmed_volume_usd": 500_000},
    }
    notes = market_risk_context(context)
    assert any("L/S BTC" in note for note in notes)
    assert any("IV BTC" in note for note in notes)
    assert any("Базис BTC" in note for note in notes)
    assert any("Ликвидации" in note for note in notes)


def test_market_risk_context_is_empty_without_context():
    assert market_risk_context(None) == []
    assert market_risk_context({}) == []




def test_digest_text_renders_context_and_directional_rows():
    context = {
        "lsr": {"BTC": {"long_short_ratio": 1.4, "source": "okx"}},
        "iv": {"BTC": {"mark_iv": 45.0}},
        "basis": {"BTC": {"basis_pct": 0.12}},
        "liquidations": {"confirmed_count": 3, "confirmed_volume_usd": 250_000},
    }
    rows = {
        ("BTC", "1m"): {"hyperdata": {"direction": "BULLISH"}},
        ("ETH", "1m"): {"hyperdata": {"direction": "BEARISH"}},
        ("ETH", "5m"): {"hyperdata": {"direction": "NEUTRAL"}},
    }
    text = digest_text(rows, context)
    assert text is not None
    assert "L/S BTC 1.40" in text
    assert "IV BTC 45%" in text
    assert "Базис BTC +0.12%" in text
    assert "Ликвидации (1 ч): 3" in text
    assert "BTC 🟢" in text
    assert "ETH 🔴" in text
    assert "5m" not in text


def test_digest_text_requires_context():
    assert digest_text({}, None) is None
    assert digest_text({}, {}) is None
