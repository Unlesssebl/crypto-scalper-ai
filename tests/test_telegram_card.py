"""Notification wording keeps pace, rarity and flow direction distinct."""
from __future__ import annotations

from extensions.scalper.market import metric
from extensions.scalper.service import alert_market_facts, alert_text


def snapshot():
    values = {"first_trade_price": 100, "last_trade_price": 99.97, "activity_ratio": 6.11,
              "trade_count": 611, "trade_rate": 10.18, "volume": 173_530, "cvd": -139_600,
              "ofi": -0.8, "price": 100, "spread": 0.008, "imbalance": 0.21}
    return {"symbol": "XRP", "window": "1m", "captured_at": 1_700_000_000,
            "metrics": {key: metric(value, "test", 1_700_000_000, "test") for key, value in values.items()},
            "activity_baseline": {"kind": "median_trade_count", "samples": 14, "window_seconds": 60}}


def test_card_distinguishes_price_pace_and_volume_share():
    snap = snapshot()
    text = alert_text(snap, {"direction": "BEARISH"}, {"status": "queued"},
                      {"anomalies": ["tape_burst"], "opportunity_kind": "early"})
    assert "-0.03%" in text and "6.1×" in text
    assert "медиане 14 закрытых свечей по 1 мин" in text
    assert "покупки <b>10%</b> / продажи <b>90%</b> объёма" in text
    assert "к обычному темпу" not in text and "Ранний импульс" not in text
    assert "перевес сделок" not in text
    assert "<b>Поток HL</b> 🔴 продажи" in text
    assert "редкость не оценена" in text
    assert text.index("<b>Цена</b>") < text.index("<b>Активность</b>") < text.index("<b>Лента HL</b>")
    assert "гипотеза входа не сформирована" in text


def test_old_snapshot_does_not_invent_baseline_sample_count():
    snap = snapshot()
    snap.pop("activity_baseline")
    text = str(alert_market_facts(snap))
    assert "состав не сохранён" in text
    assert "14 закрытых" not in text


def test_warmup_and_stale_are_visible_and_not_rendered_as_values():
    snap = snapshot()
    for key in ("ofi", "cvd", "volume", "first_trade_price", "last_trade_price", "activity_ratio"):
        snap["metrics"][key]["quality"] = "warming_up"
    snap["metrics"]["spread"]["quality"] = "stale"
    text = str(alert_market_facts(snap))
    assert "Лента: прогрев" in text and "Стакан: данные устарели" in text
    assert "90%" not in text and "6.1" not in text


def test_tiny_price_change_never_displays_negative_zero():
    snap = snapshot()
    snap["metrics"]["last_trade_price"]["value"] = 99.99999
    text = str(alert_market_facts(snap))
    assert "-0.00%" not in text


def test_markup_is_escaped_and_both_delivery_formats_keep_facts():
    snap = snapshot()
    event = {"strength": {"icon": "!", "label": "<unsafe>"}}
    ai = {"status": "ok", "direction": "NEUTRAL", "summary": "<script>", "scenario": "a & b"}
    for rich in (False, True):
        text = alert_text(snap, {"direction": "NEUTRAL"}, ai, event, rich=rich)
        assert "&lt;unsafe&gt;" in text and "&lt;script&gt;" in text and "a &amp; b" in text
        assert "гипотеза входа не сформирована" not in text
        assert "Активность" in text and "611 сделок" in text


def test_unknown_ai_error_cannot_break_html():
    text = alert_text(snapshot(), {"direction": "NEUTRAL"}, {"status": "error", "reason": "<bad&>"}, {})
    assert "&lt;bad&amp;&gt;" in text and "<bad&>" not in text
