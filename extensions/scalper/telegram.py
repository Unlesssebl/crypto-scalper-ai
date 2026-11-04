"""Reply to the configured Telegram chat using the background quality report."""
import asyncio
import logging
import re
import time

import aiohttp

from .quality import HORIZONS, TRIGGERS

LOG = logging.getLogger(__name__)
REPORT = re.compile(r"/report(?:@([a-zA-Z0-9_]+))?(?:\s+(1m|5m))?", re.IGNORECASE)


def report_text(report, window=None):
    if report is None:
        return "Отчёт ещё рассчитывается. Повторите /report через минуту."
    lines = ["📊 <b>Качество AI</b> · последние 24 часа"]
    for frame in (window,) if window else report["windows"]:
        data = report["windows"][frame]
        a = data["attempts"]
        lines.extend((
            "", f"⏱ <b>Окно {frame}</b>",
            f'Оценки: направленных {a["directional"]}, нейтральных {a["neutral"]}, '
            f'без вывода {a["insufficient"]}.',
            f'Сбои: {a["error"]} · пропуски: {a["skipped"]} · в обработке: {a["pending"]}.',
        ))
        for minutes in HORIZONS:
            h = data["horizons"][minutes]
            lines.append(
                f'<b>{minutes} мин</b> · 🟢 верно {h["correct"]} · 🔴 неверно {h["incorrect"]}'
            )
            remaining = [
                f'⚪ без движения {h["flat"]}' if h["flat"] else None,
                f'🟡 ждём {h["pending"]}' if h["pending"] else None,
                f'нет цены {h["missing"]}' if h["missing"] else None,
            ]
            if notes := " · ".join(item for item in remaining if item):
                lines.append(f"<i>{notes}</i>")
        if data.get("triggers"):
            lines.extend(("", "⚡ <b>По триггерам</b> · 🟢/🔴 на 1, 5 и 15 мин"))
            for name, label in TRIGGERS.items():
                group = data["triggers"].get(name)
                if not group:
                    continue
                ratios = ", ".join(f'{group["horizons"][m]["correct"]}/{group["horizons"][m]["incorrect"]}'
                                   for m in HORIZONS)
                delay = group["median_delay_seconds"]
                timing = f"{delay:g} с (n={group['delay_samples']})" if delay is not None else "—"
                lines.append(f"{label}: {sum(group['attempts'].values())} событий · {ratios} · медиана {timing}")
        experiment = report.get("context_experiment", {}).get(frame)
        if experiment and experiment["attempted"]:
            lines.extend(("", f'🔬 <b>Контекст 15m</b> · принятых пар {experiment["matched"]}/{experiment["attempted"]}'
                              f' · вывод изменился {experiment["changed"]}'))
            for minutes in HORIZONS:
                arms = experiment["horizons"][minutes]
                scores = [f'{arms[key]["correct"]}/{arms[key]["incorrect"]}'
                          for key in ("hyperdata", "baseline", "senior")]
                lines.append(f'{minutes} мин · HyperData {scores[0]} · AI {scores[1]} · AI с 15m {scores[2]}')
            lines.append("<i>Верно/неверно от цены после готовности обеих оценок.</i>")
    lines.extend(("", "<i>Проверка направления mark. Комиссии и исполнение в расчёт не входят.</i>"))
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


async def telegram_call(session, token, method, body=None, timeout=10):
    url = f"https://api.telegram.org/bot{token}/{method}"
    try:
        async with session.post(url, json=body or {}, timeout=aiohttp.ClientTimeout(total=timeout),
                                allow_redirects=False) as response:
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
    if commands is not None and not any(command["command"] == "report" for command in commands):
        await telegram_call(session, token, "setMyCommands", {
            "scope": scope,
            "commands": [*commands, {"command": "report", "description": "Качество анализов AI за 24 часа"}],
        })

    offset = service.journal.telegram_offset(bot["id"])
    while True:
        updates = await telegram_call(session, token, "getUpdates", {
            "offset": offset, "timeout": 25, "allowed_updates": ["message"],
        }, timeout=32)
        if updates is None:
            await asyncio.sleep(10)
            continue
        for update in updates:
            offset = update["update_id"] + 1
            command = report_command(update, chat_id, bot["username"], time.time())
            service.journal.save_telegram_offset(bot["id"], offset)
            if command is not None:
                await alerts.send_telegram(report_text(service.quality_report, *command))
