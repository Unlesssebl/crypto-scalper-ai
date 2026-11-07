"""Reply to the configured Telegram chat using the background quality report."""

import asyncio
import html
import logging
import re
import time

import aiohttp

from .fact_review import LABELS
from .quality import HORIZONS, TRIGGERS

LOG = logging.getLogger(__name__)


def consistency_text(report, *, rich=False):
    """Facts, hypotheses and abstentions; no direction accuracy denominator."""
    lines = [
        "<b>Согласованность фактов · 24 часа</b>",
        f"Эпизодов наблюдалось: {report['observed_episodes']}; разобрано: {report['reviewed_episodes']}.",
        f"Попыток: {report['attempts']}; допущено запросов: {report['api_attempts']}; "
        f"воздержаний: {report['abstentions']}.",
    ]
    for section, counts in report["claims"].items():
        label = {
            "observation": "Числа и факты",
            "comparison": "Сравнения",
            "interpretation": "Интерпретации",
            "missing_data": "Недостающие данные",
        }[section]
        lines.append(
            html.escape(label)
            + ": "
            + "; ".join(html.escape(LABELS[status]) + f" — {count}" for status, count in counts.items())
        )
    lines.append("Статусы попыток: " + html.escape(str(report["statuses"])))
    lines.append("Общие исходные факты не являются независимыми подтверждениями. Гипотезы не доказаны.")
    return "<p>" + "<br>".join(lines) + "</p>" if rich else "\n".join(lines)


def _context_fact(fact):
    """A current measurement for the notification, never a raw audit reference."""
    key, value = fact["id"], fact.get("value")
    labels = {
        "book.midpoint": "Середина спреда",
        "book.spread_bps": "Спред",
        "activity.relative_score": "Относительная активность",
        "tape.observed_trade_count_60s": "Наблюдалось сделок за 60 с",
        "tape.observed_turnover_60s": "Наблюдалось оборота за 60 с",
        "tape.observed_trade_imbalance": "Дисбаланс наблюдаемых сделок",
    }
    label = labels.get(key)
    if key.startswith("density."):
        _, side, rest = key.split(".", 2)
        price, field = rest.rsplit(".", 1)
        field_label = {
            "size": "размер",
            "size_change": "изменение размера",
            "relative_size": "размер к медиане уровней",
            "observed_age_seconds": "наблюдается",
        }.get(field)
        if side in {"bid", "ask"} and field_label:
            label = f"{'Спрос' if side == 'bid' else 'Предложение'} {float(price):.10g}: {field_label}"
    if not label or fact.get("quality") != "ok" or type(value) not in (float, int):
        return None
    unit_name = fact.get("unit")
    precision = ",.2f" if unit_name in {"bps", "ratio", "seconds", "usd"} else ",.10g"
    number = format(value, precision).replace(",", " ")
    unit = {"bps": " б.п.", "seconds": " с", "usd": " USD", "ratio": "", "base_units": " ед."}.get(fact.get("unit"), "")
    return html.escape(f"{label}: {number}{unit}")


def context_review_text(response, *, rich=False):
    """Compact human summary; the full assertion audit stays in SQLite/Dashboard."""
    review, packet = response.get("review"), response.get("packet")
    symbol = html.escape(str(packet["symbol"])[:40]) + " · " if packet else ""
    lines = [f"<b>{symbol}Проверка контекста</b>"]
    if review and review.get("route"):
        label = {"free": "Бесплатный разбор", "strong": "Разбор сильного эпизода", "meta": "Метаобзор"}
        lines.append(label.get(review["route"], "Контекстный разбор"))
    if not review and packet:
        facts = [
            f
            for f in packet.get("facts", [])
            if f.get("quality") == "ok"
            and f["id"]
            in {"book.midpoint", "book.spread_bps", "tape.observed_trade_count_60s", "activity.relative_score"}
        ]
        for fact in facts:
            text = _context_fact(fact)
            if text:
                lines.append(text)

    checked = review.get("result") if review else None
    if review and review.get("trigger_reason"):
        from .context_auto import REASONS

        lines.append("Повод: " + html.escape(REASONS.get(review["trigger_reason"], "изменение контекста")))
    if checked:
        lines.append("Разбор сохранённого снимка рынка.")
        if checked.get("expired_at_completion"):
            lines.append("К завершению ответа пакет устарел; текущая ситуация могла измениться.")
        if checked["abstention"]:
            lines.append("Модель воздержалась от заключения.")
        claims = checked.get("claims", [])
        supported = {
            ref
            for c in claims
            if c["section"] == "observation" and c["status"] == "supported"
            for ref in c["references"]
        }
        facts = sorted(
            (packet or {}).get("facts", []),
            key=lambda f: (not f["id"].startswith(("book.", "activity.", "tape.")), f["id"]),
        )
        measurements = [text for f in facts if f["id"] in supported if (text := _context_fact(f))]
        if measurements:
            lines.extend(["", "<b>Проверенные значения</b>", *measurements[:5]])
        hypotheses = [c for c in claims if c["section"] == "interpretation" and c["status"] == "hypothesis"]
        if hypotheses:
            lines.extend(["", "<b>Гипотезы AI · не доказаны</b>"])
            for claim in hypotheses[:2]:
                text = " ".join(claim["assertion"]["text"].split())
                lines.append(html.escape(text[:357] + "…" if len(text) > 360 else text))
        elif not checked["abstention"]:
            lines.append("AI не предложил гипотез с достаточными исходными данными.")
        rejected = sum(c["status"] == "contradicted" for c in claims)
        unverified = sum(c["status"] == "unverifiable" for c in claims)
        if rejected or unverified:
            lines.append(f"Проверка AI: противоречий данным — {rejected}; непроверяемых утверждений — {unverified}.")
        # 'Supported' in missing_data confirms an absence, never the missing value.
        missing = {
            ref
            for c in claims
            if c["section"] == "missing_data" and c["status"] == "supported"
            for ref in c["references"]
        }
        limitations = []
        if any(r.endswith(".actual_order_age") for r in missing):
            limitations.append("возраст отдельных заявок неизвестен")
        if any(r.startswith("frame.") for r in missing):
            limitations.append("часть данных минутных окон недоступна")
        if missing - {r for r in missing if r.endswith(".actual_order_age") or r.startswith("frame.")}:
            limitations.append("часть исходных данных отсутствует или устарела")
        if limitations:
            lines.extend(["", "Ограничения: " + "; ".join(limitations) + "."])
        lines.append("Полный протокол проверки — в Dashboard.")
    elif review:
        lines.append(_context_status(review.get("status", response["status"]), review.get("reason")))
    else:
        lines.append(_context_status(response["status"], response.get("reason")))
    return "<p>" + "<br>".join(lines) + "</p>" if rich else "\n".join(lines)


def _context_status(status, reason):
    statuses = {
        "skipped": "AI: пропущен",
        "unconfigured": "AI: не настроен",
        "unavailable": "AI: недоступен",
        "network_error": "AI: ошибка соединения",
        "invalid_response": "AI: ответ не прошёл проверку",
        "pending": "AI: анализ выполняется",
    }
    reasons = {
        "no_api_key": "нет API-ключа",
        "hourly_limit": "исчерпан бюджет запросов",
        "cooldown": "пауза между запросами по инструменту",
        "stale_or_unknown_packet": "нет свежего снимка рынка",
        "episode_changed_or_unavailable": "эпизод изменился или недоступен",
        "context_mode_required": "режим контекста недоступен",
        "demo": "демонстрационный режим",
    }
    text = statuses.get(status, "AI: анализ не завершён")
    return text + (" — " + reasons[reason] if reason in reasons else "")


REPORT = re.compile(r"/report(?:@([a-zA-Z0-9_]+))?(?:\s+(1m|5m))?", re.IGNORECASE)
ANALYZE = re.compile(r"/analyze(?:@([a-zA-Z0-9_]+))?\s+([a-zA-Z0-9_.:-]{1,40})\s+(1m|5m)", re.IGNORECASE)


def digest_text(rows, context, *, rich=False):
    """Short market snapshot: top rows by direction plus the market context.

    Used for a periodic unsolicited digest; `rows` is the screener's
    `service.rows` dict and `context` the hub's market-context block. Returns
    None when there is nothing worth sending (no rows or no context).
    """

    def esc(text):
        return html.escape(str(text))

    if not context:
        return None
    lines = ["<b>Обзор рынка</b>"]

    if lsr := context.get("lsr", {}).get("BTC"):
        ratio = lsr.get("long_short_ratio")
        if ratio is not None:
            lines.append(f"L/S BTC {ratio:.2f} · {esc(lsr.get('source', '?'))}")
    if iv := context.get("iv", {}).get("BTC"):
        mark = iv.get("mark_iv")
        if mark is not None:
            lines.append(f"IV BTC {mark:.0f}%")
    if basis := context.get("basis", {}).get("BTC"):
        bp = basis.get("basis_pct")
        if bp is not None:
            lines.append(f"Базис BTC {bp:+.2f}%")
    if liq := context.get("liquidations"):
        count = liq.get("confirmed_count")
        if count is not None:
            lines.append(f"Ликвидации (1 ч): {count} ({int(liq.get('confirmed_volume_usd', 0)):,} USD)")

    top = []
    for (symbol, window), row in rows.items():
        if window != "1m":
            continue
        direction = row.get("hyperdata", {}).get("direction", "NOT_AVAILABLE")
        if direction in ("BULLISH", "BEARISH"):
            top.append((symbol, direction))
    if top:
        ticks = " · ".join(f"{esc(sym)} {('🟢' if d == 'BULLISH' else '🔴')}" for sym, d in top)
        lines.append(f"Направленные 1m: {ticks}")

    text = "\n".join(["<b>" + lines[0] + "</b>", *lines[1:]]) if rich else "\n".join(lines)
    return text


async def announce_online(service):
    """Send one startup message once the first scan has rows.

    This is the quickest way for an operator to confirm Telegram delivery
    works end to end, instead of waiting for the first strong finding or the
    periodic digest.
    """
    alerts = service.hub.alerts
    if not (alerts.telegram_token and alerts.telegram_chat_id):
        return
    for _ in range(30):
        if service.rows:
            break
        await asyncio.sleep(1)
    context = getattr(service.hub, "context", None)
    body = digest_text(service.rows, context)
    head = "HyperData screener запущен."
    text = f"{head}\n{body}" if body else head
    try:
        await alerts.send_telegram(text, rich_html=digest_text(service.rows, context, rich=True) if body else None)
    except Exception:
        LOG.exception("Telegram startup message failed")


async def run_digest(session, service, interval_seconds):
    """Send the market-context digest every `interval_seconds` when enabled."""
    alerts = service.hub.alerts
    while True:
        await asyncio.sleep(interval_seconds)
        if not (alerts.telegram_token and alerts.telegram_chat_id):
            continue
        context = getattr(service.hub, "context", None)
        text = digest_text(service.rows, context)
        if text is None:
            continue
        try:
            await alerts.send_telegram(text, rich_html=digest_text(service.rows, context, rich=True))
        except Exception:
            LOG.exception("Market digest send failed")


def report_text(report, window=None, *, rich=False):
    if report is None:
        return "Отчёт ещё рассчитывается. Повторите /report через минуту."

    def paragraph(*items):
        return "<p>" + "<br>".join(items) + "</p>" if rich else "\n".join(items)

    lines = [
        paragraph(
            "<b>Архив: направление от снимка</b> · последние 24 часа",
            "Совпадение направления с последующей mark-ценой; не проверка согласованности фактов.",
        )
    ]
    for frame in (window,) if window else report["windows"]:
        data = report["windows"][frame]
        a = data["attempts"]
        lines.extend(("<hr/>" if rich else "", paragraph(f"<b>Окно {frame}</b>")))
        table = []
        notes = []
        for minutes in HORIZONS:
            h = data["horizons"][minutes]
            correct, incorrect = h["correct"], h["incorrect"]
            decided = correct + incorrect
            accuracy = f"{correct / decided:.0%}" if decided else "—"
            if rich:
                table.append(
                    f'<tr><td>{minutes} мин</td><td align="right">{correct}</td>'
                    f'<td align="right">{incorrect}</td><td align="right"><b>{accuracy}</b></td></tr>'
                )
            else:
                lines.append(f"<b>{minutes} мин</b> · 🟢 верно {correct} · 🔴 неверно {incorrect} · {accuracy}")
            remaining = [
                f"без движения {h['flat']}" if h["flat"] else None,
                f"ждём {h['pending']}" if h["pending"] else None,
                f"нет цены {h['missing']}" if h["missing"] else None,
            ]
            if other := " · ".join(item for item in remaining if item):
                notes.append(f"<i>{minutes} мин · {other}</i>")
        if rich:
            lines.append(
                "<table striped compact><tr><th>Горизонт</th><th>🟢 Верно</th>"
                "<th>🔴 Мимо</th><th>Точность</th></tr>" + "".join(table) + "</table>"
            )
        if notes:
            lines.append(paragraph(*notes))
        lines.append(
            paragraph(
                f"<b>Оценки</b> · направленных {a['directional']} · нейтральных {a['neutral']} "
                f"· без вывода {a['insufficient']}",
                f"Сбои {a['error']} · пропуски {a['skipped']} · в обработке {a['pending']}",
            )
        )
        if data.get("triggers"):
            group_lines = ["<b>По триггерам</b> · верно/неверно на 1, 5 и 15 мин"]
            for name, label in TRIGGERS.items():
                group = data["triggers"].get(name)
                if not group:
                    continue
                ratios = ", ".join(
                    f"{group['horizons'][m]['correct']}/{group['horizons'][m]['incorrect']}" for m in HORIZONS
                )
                delay = group["median_delay_seconds"]
                timing = f"{delay:g} с (n={group['delay_samples']})" if delay is not None else "—"
                group_lines.append(
                    f"<b>{label}</b> · {sum(group['attempts'].values())} событий · {ratios} · медиана {timing}"
                )
            if not rich:
                lines.append("")
            lines.append(paragraph(*group_lines))
        experiment = report.get("context_experiment", {}).get(frame)
        if experiment and experiment["attempted"]:
            comparison = [
                f"<b>Контекст 15m</b> · принятых пар {experiment['matched']}/{experiment['attempted']}"
                f" · вывод изменился {experiment['changed']}"
            ]
            for minutes in HORIZONS:
                arms = experiment["horizons"][minutes]
                scores = [
                    f"{arms[key]['correct']}/{arms[key]['incorrect']}" for key in ("hyperdata", "baseline", "senior")
                ]
                comparison.append(
                    f"<b>{minutes} мин</b> · HyperData {scores[0]} · AI {scores[1]} · AI с 15m {scores[2]}"
                )
            comparison.append("<i>Верно/неверно от цены после готовности обеих оценок.</i>")
            if not rich:
                lines.append("")
            lines.append(paragraph(*comparison))
        audit = report.get("finding_audit", {}).get(frame)
        if audit and audit["candidates"]:
            delivered = audit["decisions"].get("delivered", 0)
            merged = audit["decisions"].get("delivery_merged", 0)
            finding_lines = [
                f"<b>Находки</b> · кандидатов {audit['candidates']} · доставлено {delivered} · объединено {merged}"
            ]
            labels = ("Нет данных", "Фоновая", "Заметная", "Сильная", "Критичная")
            for version, ranks in audit["versions"].items():
                for rank, counts in sorted(ranks.items(), reverse=True):
                    hits, quiet = counts.get("hit", 0), counts.get("quiet", 0)
                    pending = counts.get("pending", 0)
                    missing = counts.get("missing", 0) + counts.get("no_natr", 0)
                    finding_lines.append(
                        f"{html.escape(version)} · {audit.get('rank_labels', {}).get(version, labels)[int(rank)]}"
                        f" · {hits}/{hits + quiet} "
                        f"движений ≥ NATR · ждём {pending} · без данных {missing}"
                    )
            if not rich:
                lines.append("")
            control = audit.get("control", {})
            if control.get("pairs"):
                finding_lines.append(
                    f"Пар {control['pairs']} · движение после сигнала {control['signal_hits']}"
                    f" · в обычных окнах {control['control_hits']}"
                )
            else:
                finding_lines.append("Сопоставимые обычные окна ещё накапливаются.")
            lines.append(paragraph(*finding_lines))
        comparison = report.get("minute_shadow") if frame == "1m" else None
        if comparison and comparison["baseline"]:
            lines.append(
                paragraph(
                    f"<b>Строгий порог 1m</b> · находок {comparison['strict']} против {comparison['baseline']}",
                    f"Без строгого сигнала в пределах ±60 с · {comparison['missed_moves']} из "
                    f"{comparison['baseline_moves']} базовых событий с движением ≥ 0,5% за 5 минут.",
                )
            )
    lines.append(
        paragraph("<i>Точность — верные среди верных и неверных. Проверка по mark без комиссий и исполнения.</i>")
    )
    return "\n".join(lines)


def report_command(update, chat_id, username, now):
    message = update.get("message", {})
    if str(message.get("chat", {}).get("id")) != str(chat_id):
        return None
    stamp = message.get("date")
    if not isinstance(stamp, int) or now - stamp > 120:
        return None
    matched = REPORT.fullmatch((message.get("text") or "").strip())
    if not matched:
        return None
    target, window = matched.groups()
    if target and target.casefold() != username.casefold():
        return None
    return (window.lower() if window else None,)


def analyze_command(update, chat_id, username, now):
    message = update.get("message", {})
    if str(message.get("chat", {}).get("id")) != str(chat_id):
        return None
    stamp = message.get("date")
    if not isinstance(stamp, int) or not 0 <= now - stamp <= 120:
        return None
    match = ANALYZE.fullmatch((message.get("text") or "").strip())
    if not match:
        return None
    target, symbol, window = match.groups()
    if target and target.casefold() != username.casefold():
        return None
    return symbol, window.lower()


async def telegram_call(session, token, method, body=None, timeout=10):
    url = f"https://api.telegram.org/bot{token}/{method}"
    try:
        async with session.post(
            url, json=body or {}, timeout=aiohttp.ClientTimeout(total=timeout), allow_redirects=False
        ) as response:
            if response.status != 200:
                LOG.warning("Telegram %s returned HTTP %d", method, response.status)
                return None
            payload = await response.json()
            return payload.get("result") if payload.get("ok") else None
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        LOG.warning("Telegram %s failed: %s", method, type(exc).__name__)
        return None


async def listen_reports(session, service):
    alerts = service.hub.alerts
    token = alerts.telegram_token
    while True:
        webhook = await telegram_call(session, token, "getWebhookInfo")
        if webhook is not None and webhook.get("url"):
            LOG.warning("Telegram report polling unavailable while a webhook is configured")
        elif webhook is not None:
            bot = await telegram_call(session, token, "getMe")
            if bot:
                break
        await asyncio.sleep(60)

    chat_id = alerts.telegram_chat_id
    scope = {"type": "chat", "chat_id": chat_id}
    commands = await telegram_call(session, token, "getMyCommands", {"scope": scope})
    if commands == []:
        commands = await telegram_call(session, token, "getMyCommands")
    additions = [
        item
        for item in (
            {"command": "report", "description": "Качество анализов AI за 24 часа"},
            {"command": "analyze", "description": "Анализ: /analyze BTC 1m"},
        )
        if commands is not None and not any(c["command"] == item["command"] for c in commands)
    ]
    if additions:
        await telegram_call(
            session,
            token,
            "setMyCommands",
            {
                "scope": scope,
                "commands": [*commands, *additions],
            },
        )

    offset = service.journal.telegram_offset(bot["id"])
    while True:
        updates = await telegram_call(
            session,
            token,
            "getUpdates",
            {
                "offset": offset,
                "timeout": 25,
                "allowed_updates": ["message", "callback_query"],
            },
            timeout=32,
        )
        if updates is None:
            await asyncio.sleep(10)
            continue
        for update in updates:
            offset = update["update_id"] + 1
            if update.get("callback_query") and getattr(service, "telegram_ux", None):
                query = update["callback_query"]
                reply = await service.telegram_ux.callback(query, session)
                await telegram_call(
                    session,
                    token,
                    "answerCallbackQuery",
                    {"callback_query_id": query["id"], "text": reply[:200], "show_alert": True},
                )
            command = report_command(update, chat_id, bot["username"], time.time())
            service.journal.save_telegram_offset(bot["id"], offset)
            manual = analyze_command(update, chat_id, bot["username"], time.time())
            if manual and getattr(service, "telegram_ux", None):
                if not service.telegram_ux.operator_allowed(update.get("message", {}).get("from", {}).get("id")):
                    manual = None
            if manual is not None:
                if not getattr(service, "legacy_ai_enabled", True) or getattr(
                    service.hub, "attention", {}
                ).get("context_only"):
                    response = await service.review_context(manual[0])
                    if getattr(service, "telegram_ux", None):
                        published = service.telegram_ux.publish_analysis(response) if response.get("packet") else False
                        await alerts.send_telegram(
                            "Разбор направлен в аналитический канал."
                            if published and service.telegram_ux.analytics_chat()
                            else "Разбор не опубликован: канал не настроен или свежий проверенный ответ недоступен."
                        )
                    else:
                        await alerts.send_telegram(
                            context_review_text(response), rich_html=context_review_text(response, rich=True)
                        )
                else:
                    await alerts.send_telegram(service.request_analysis(*manual, time.time()))
            if command is not None:
                if not getattr(service, "legacy_ai_enabled", True) or getattr(
                    service.hub, "attention", {}
                ).get("context_only"):
                    if getattr(service, "telegram_ux", None):
                        await alerts.send_telegram(service.telegram_ux.report(time.time()))
                        continue
                    report = service.consistency.report(time.time())
                    await alerts.send_telegram(consistency_text(report), rich_html=consistency_text(report, rich=True))
                    continue
                report = service.quality_report
                await alerts.send_telegram(
                    report_text(report, *command),
                    rich_html=report_text(report, *command, rich=True) if report is not None else None,
                )
