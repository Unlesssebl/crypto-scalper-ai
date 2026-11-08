"""Reply to the configured Telegram chat using the background quality report."""

import asyncio
import html
import logging
import re
import time

import aiohttp

from .fact_review import LABELS

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
BOTS = re.compile(r"/bots(?:@([a-zA-Z0-9_]+))?", re.IGNORECASE)
BOT = re.compile(r"/bot(?:@([a-zA-Z0-9_]+))?\s+([a-zA-Z0-9_.:-]{1,64})", re.IGNORECASE)


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


def bots_command(update, chat_id, username, now):
    """Parse /bots (summary) and /bot <id> (one observer)."""
    message = update.get("message", {})
    if str(message.get("chat", {}).get("id")) != str(chat_id):
        return None
    stamp = message.get("date")
    if not isinstance(stamp, int) or not 0 <= now - stamp <= 120:
        return None
    text = (message.get("text") or "").strip()
    summary = BOTS.fullmatch(text)
    if summary:
        target = summary.group(1)
        if target and target.casefold() != username.casefold():
            return None
        return "summary", None
    match = BOT.fullmatch(text)
    if not match:
        return None
    target, bot_id = match.groups()
    if target and target.casefold() != username.casefold():
        return None
    return "bot", bot_id.casefold()


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
            {"command": "bots", "description": "Статистика наблюдателей"},
            {"command": "bot", "description": "Детально: /bot flow.continuation"},
            {"command": "stats", "description": "Статистика сигналов этого бота"},
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
                # A brief toast, not a modal: the trader flips between instruments without dismissing dialogs.
                await telegram_call(
                    session,
                    token,
                    "answerCallbackQuery",
                    {"callback_query_id": query["id"], "text": reply[:200], "show_alert": False},
                )
            command = report_command(update, chat_id, bot["username"], time.time())
            service.journal.save_telegram_offset(bot["id"], offset)
            message = update.get("message") or {}
            if (
                getattr(service, "telegram_ux", None)
                and str(message.get("chat", {}).get("id", "")) == str(chat_id)
                and (message.get("text") or "").strip().split("@")[0] == "/stats"
            ):
                from .channels import stats_text

                anomalies = service.channels.get("anomalies")
                if anomalies:
                    await alerts.send_telegram(stats_text(service, anomalies), chat_id=chat_id)
                continue
            bot_query = bots_command(update, chat_id, bot["username"], time.time())
            if bot_query is not None:
                requested = None if bot_query[0] == "summary" else bot_query[1]
                payload = service.bot_report(requested)
                await alerts.send_telegram(
                    payload.get("text") or "Нет данных.",
                    rich_html=payload.get("html"),
                )
                continue
            manual = analyze_command(update, chat_id, bot["username"], time.time())
            if manual and getattr(service, "telegram_ux", None):
                if not service.telegram_ux.operator_allowed(update.get("message", {}).get("from", {}).get("id")):
                    manual = None
            if manual is not None:
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
            if command is not None:
                if getattr(service, "telegram_ux", None):
                    await alerts.send_telegram(service.telegram_ux.report(time.time()))
                    continue
                report = service.consistency.report(time.time())
                await alerts.send_telegram(consistency_text(report), rich_html=consistency_text(report, rich=True))
