"""Notifications are summaries, not the machine assertion audit."""
from copy import deepcopy

import pytest

from extensions.scalper.telegram import context_review_text


def response():
    def claim(section, status, ref, **assertion):
        return dict(section=section, status=status, references=[ref], assertion=assertion)
    return dict(status="ok", packet=dict(symbol="BNB", facts=[
        dict(id="book.midpoint", quality="ok", value=692.1, unit="price"),
        dict(id="density.bid.692.10000000000002.size", quality="ok", value=12, unit="base_units"),
        dict(id="history.123.midpoint", quality="ok", value=690, unit="price"),
    ]), review=dict(trigger_reason="flow_price_conflict", result=dict(
        abstention=False, expired_at_completion=False, claims=[
            claim("observation", "supported", "book.midpoint"),
            claim("observation", "supported", "density.bid.692.10000000000002.size"),
            claim("observation", "supported", "history.123.midpoint"),
            claim("missing_data", "supported", "density.bid.692.10000000000002.actual_order_age"),
            claim("missing_data", "supported", "frame.1m.cvd"),
            claim("comparison", "contradicted", "book.midpoint"),
            claim("interpretation", "unverifiable", "book.midpoint", text="Необоснованный вывод"),
            claim("interpretation", "hypothesis", "book.midpoint", text="Плотность <рядом> & видна."),
        ])))


def test_readable_values_missing_age_and_escaped_hypothesis():
    text = context_review_text(response())
    assert "Середина спреда: 692.1" in text
    assert "Спрос 692.1: размер: 12 ед." in text
    assert "возраст отдельных заявок неизвестен" in text
    assert "часть данных минутных окон недоступна" in text
    assert "противоречий данным — 1; непроверяемых утверждений — 1" in text
    assert "Гипотезы AI · не доказаны" in text
    assert "Плотность &lt;рядом&gt; &amp; видна." in text
    for raw in ("book.midpoint", "actual_order_age", "692.10000000000002", "history.",
                "Необоснованный вывод", "Подтверждено измерениями", "**", "\\"):
        assert raw not in text
    assert "690" not in text  # Historical facts cannot masquerade as current quotes.


def test_rich_and_fallback_have_same_content_and_bounded_size():
    data = response()
    data["review"]["result"]["claims"] *= 12
    for c in data["review"]["result"]["claims"]:
        if c["section"] == "interpretation" and c["status"] == "hypothesis":
            c["assertion"]["text"] = "<&>" * 134
    fallback = context_review_text(data)
    rich = context_review_text(data, rich=True)
    assert rich == "<p>" + fallback.replace("\n", "<br>") + "</p>"
    assert len(rich) < 4096
    assert fallback.count("Середина спреда") == 1


def test_expired_abstention_does_not_imply_current_insight():
    data = deepcopy(response())
    data["review"]["result"].update(abstention=True, expired_at_completion=True, claims=[])
    text = context_review_text(data)
    assert "пакет устарел" in text
    assert "Модель воздержалась" in text
    assert "Проверенные значения" not in text
    assert "Гипотезы AI" not in text


@pytest.mark.parametrize(("status", "reason", "expected"), [
    ("unconfigured", "no_api_key", "AI: не настроен — нет API-ключа"),
    ("skipped", "cooldown", "AI: пропущен — пауза между запросами"),
    ("skipped", "hourly_limit", "AI: пропущен — исчерпан бюджет"),
    ("network_error", None, "AI: ошибка соединения"),
    ("invalid_response", "private provider detail", "AI: ответ не прошёл проверку"),
])
def test_real_status_without_internal_errors_or_false_pending(status, reason, expected):
    text = context_review_text(dict(status=status, reason=reason))
    assert expected in text
    assert "анализ выполняется" not in text
    assert "private provider detail" not in text
