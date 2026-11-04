"""Reply to the configured Telegram chat using the background quality report."""
import asyncio
import html
import logging
import re
import time

import aiohttp

from .quality import HORIZONS, TRIGGERS

LOG = logging.getLogger(__name__)
REPORT = re.compile(r"/report(?:@([a-zA-Z0-9_]+))?(?:\s+(1m|5m))?", re.IGNORECASE)


def report_text(report, window=None, *, rich=False):
    if report is None:
        return "Отчёт ещё рассчитывается. Повторите /report через минуту."

    def paragraph(*items):
        return "<p>" + "<br>".join(items) + "</p>" if rich else "\n".join(items)

    lines = [paragraph("<b>Качество AI</b> · последние 24 часа")]
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
                table.append(f'<tr><td>{minutes} мин</td><td align="right">{correct}</td>'
                             f'<td align="right">{incorrect}</td><td align="right"><b>{accuracy}</b></td></tr>')
            else:
                lines.append(f"<b>{minutes} мин</b> · 🟢 верно {correct} · 🔴 неверно {incorrect} · {accuracy}")
            remaining = [
                f'без движения {h["flat"]}' if h["flat"] else None,
                f'ждём {h["pending"]}' if h["pending"] else None,
                f'нет цены {h["missing"]}' if h["missing"] else None,
            ]
            if other := " · ".join(item for item in remaining if item):
                notes.append(f"<i>{minutes} мин · {other}</i>")
        if rich:
            lines.append("<table striped compact><tr><th>Горизонт</th><th>🟢 Верно</th>"
                         "<th>🔴 Мимо</th><th>Точность</th></tr>" + "".join(table) + "</table>")
        if notes:
            lines.append(paragraph(*notes))
        lines.append(paragraph(
            f'<b>Оценки</b> · направленных {a["directional"]} · нейтральных {a["neutral"]} '
            f'· без вывода {a["insufficient"]}',
            f'Сбои {a["error"]} · пропуски {a["skipped"]} · в обработке {a["pending"]}',
        ))
        if data.get("triggers"):
            group_lines = ["<b>По триггерам</b> · верно/неверно на 1, 5 и 15 мин"]
            for name, label in TRIGGERS.items():
                group = data["triggers"].get(name)
                if not group:
                    continue
                ratios = ", ".join(f'{group["horizons"][m]["correct"]}/{group["horizons"][m]["incorrect"]}'
                                   for m in HORIZONS)
                delay = group["median_delay_seconds"]
                timing = f"{delay:g} с (n={group['delay_samples']})" if delay is not None else "—"
                group_lines.append(f"<b>{label}</b> · {sum(group['attempts'].values())} событий"
                                   f" · {ratios} · медиана {timing}")
            if not rich:
                lines.append("")
            lines.append(paragraph(*group_lines))
        experiment = report.get("context_experiment", {}).get(frame)
        if experiment and experiment["attempted"]:
            comparison = [f'<b>Контекст 15m</b> · принятых пар {experiment["matched"]}/{experiment["attempted"]}'
                          f' · вывод изменился {experiment["changed"]}']
            for minutes in HORIZONS:
                arms = experiment["horizons"][minutes]
                scores = [f'{arms[key]["correct"]}/{arms[key]["incorrect"]}'
                          for key in ("hyperdata", "baseline", "senior")]
                comparison.append(f'<b>{minutes} мин</b> · HyperData {scores[0]} · AI {scores[1]} · AI с 15m {scores[2]}')
            comparison.append("<i>Верно/неверно от цены после готовности обеих оценок.</i>")
            if not rich:
                lines.append("")
            lines.append(paragraph(*comparison))
        audit = report.get("finding_audit", {}).get(frame)
        if audit and audit["candidates"]:
            delivered = audit["decisions"].get("delivered", 0)
            finding_lines = [f'<b>Находки</b> · кандидатов {audit["candidates"]} · доставлено {delivered}']
            labels = ("Нет данных", "Фоновая", "Заметная", "Сильная", "Критичная")
            for version, ranks in audit["versions"].items():
                for rank, counts in sorted(ranks.items(), reverse=True):
                    hits, quiet = counts.get("hit", 0), counts.get("quiet", 0)
                    pending = counts.get("pending", 0)
                    missing = counts.get("missing", 0) + counts.get("no_natr", 0)
                    finding_lines.append(
                        f'{html.escape(version)} · {audit.get("rank_labels", {}).get(version, labels)[int(rank)]} · {hits}/{hits + quiet} '
                        f'движений ≥ NATR · ждём {pending} · без данных {missing}'
                    )
            if not rich:
                lines.append("")
            control = audit.get("control", {})
            if control.get("pairs"):
                finding_lines.append(f'Пар {control["pairs"]} · движение после сигнала {control["signal_hits"]}'
                                     f' · в обычных окнах {control["control_hits"]}')
            else:
                finding_lines.append("Сопоставимые обычные окна ещё накапливаются.")
            lines.append(paragraph(*finding_lines))
    lines.append(paragraph("<i>Точность — верные среди верных и неверных. Проверка по mark без комиссий и исполнения.</i>"))
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
                report = service.quality_report
                await alerts.send_telegram(
                    report_text(report, *command),
                    rich_html=report_text(report, *command, rich=True) if report is not None else None,
                )
