"""Telegram bots per signal type (docs/multi-bot-plan.md).

Each channel is its own Telegram bot: anomalies (the existing TELEGRAM_BOT_TOKEN bot), liquid movers and
levels. A channel without a token stays silent; its signals remain in the journal and the dashboard.
All channels share one card outbox; a card's `destination` picks the bot that sends it. Every bot is
polled for button presses (TigerTrade, «Разбор») and answers /stats about its own signals.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass

from hyperdata_terminal.data_layer.alerts import AlertManager

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChannelSpec:
    name: str
    title: str
    destination: str  # episode_cards.destination for this bot's operational cards
    token_env: str
    chat_env: str
    desktop: bool  # Windows toast + TigerTrade alert for strong cards (anomalies only, per the interview)


SPECS = (
    ChannelSpec("anomalies", "Аномалии", "operations", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", True),
    ChannelSpec("movers", "Ликвидные движения", "movers", "SCALPER_MOVERS_BOT_TOKEN", "SCALPER_MOVERS_CHAT_ID", False),
    ChannelSpec("levels", "Плотности и уровни", "levels", "SCALPER_LEVELS_BOT_TOKEN", "SCALPER_LEVELS_CHAT_ID", False),
)


class Channel:
    def __init__(self, spec, sender):
        self.spec = spec
        self.sender = sender

    @property
    def name(self):
        return self.spec.name

    @property
    def chat_id(self):
        return str(self.sender.telegram_chat_id or "")

    @property
    def configured(self):
        return bool(self.sender.telegram_token and self.sender.telegram_chat_id)

    def status(self):
        return dict(name=self.name, title=self.spec.title, configured=self.configured, desktop=self.spec.desktop)


def load_channels(main_alerts):
    """The anomalies channel reuses the hub's AlertManager; the others get their own token and chat."""
    channels = {}
    for spec in SPECS:
        if spec.name == "anomalies":
            sender = main_alerts
        else:
            sender = AlertManager()
            sender.telegram_token = os.getenv(spec.token_env, "").strip()
            sender.telegram_chat_id = os.getenv(spec.chat_env, "").strip()
            sender.discord_webhook = ""
        channels[spec.name] = Channel(spec, sender)
    return channels


def senders_by_destination(channels):
    return {c.spec.destination: c.sender for c in channels.values() if c.name != "anomalies"}


def stats_text(service, channel, now=None):
    """/stats: this bot's cards over 24h and 7 days, judged by the barrier rule against their background."""
    now = time.time() if now is None else now
    outcomes = service.telegram_ux.outcomes
    lines = [f"<b>{channel.spec.title} · статистика</b>"]

    def share(part, whole):
        return f"{part / whole * 100:.0f}%"

    def background(v):
        return f" · фон {v['base_rate'] * 100:.0f}%" if v["base_rate"] is not None else ""

    for hours, label in ((24, "24 часа"), (24 * 7, "7 дней")):
        s = outcomes.channel_summary(channel.name, now, hours)
        line = f"<b>{label}:</b> сигналов {s['sent']}"
        if s["pending"]:
            line += f" · ждут оценки {s['pending']}"
        sided, unsided = s["sided_verdict"], s["neutral_verdict"]
        if sided["judged"] and sided["judged"] < 30:
            line += " · мало данных"
        lines.append(line)
        if sided["judged"]:
            n = sided["judged"]
            lines.append(
                f"  Со стороной {n}: ✅ подтвердилось {sided['hit']} ({share(sided['hit'], n)}) · "
                f"❌ против {sided['failed']} · ➖ без движения {sided['flat']}{background(sided)}"
            )
            for row in s["by_strength"]:
                if row["judged"]:
                    lines.append(
                        f"    {'🔥 сильные' if row['strength'] == 'strong' else '🔸 заметные'}: "
                        f"подтвердилось {row['hit']} из {row['judged']}{background(row)}"
                    )
        if unsided["judged"]:
            n = unsided["judged"]
            lines.append(
                f"  Без стороны {n}: заметное движение {unsided['hit']} ({share(unsided['hit'], n)})"
                f"{background(unsided)}"
            )
        unjudged = sided["unjudged"] + unsided["unjudged"]
        if unjudged:
            lines.append(f"  Без оценки {unjudged}: не хватило свечей до или после карточки")
    lines.append(
        "<i>Подтвердилось — цена прошла обычный 5-минутный размах монеты в сторону сигнала раньше, чем столько "
        "же против, за 15 минут. Фон — как часто то же правило срабатывало у этой монеты в течение часа до "
        "карточки. Наблюдение после отбора, не торговый результат: без входа, комиссий и проскальзывания.</i>"
    )
    return "\n".join(lines)


async def listen_channel(session, service, channel):
    """Poll one extra bot: answer its buttons and /stats. The anomalies bot is polled by listen_reports."""
    from .telegram import telegram_call

    token = channel.sender.telegram_token
    while True:
        bot = await telegram_call(session, token, "getMe")
        if bot:
            break
        await asyncio.sleep(60)
    await telegram_call(
        session, token, "setMyCommands",
        {"commands": [{"command": "stats", "description": "Статистика сигналов этого бота"}]},
    )
    offset = service.journal.telegram_offset(bot["id"])
    while True:
        updates = await telegram_call(
            session, token, "getUpdates",
            {"offset": offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]},
            timeout=32,
        )
        if updates is None:
            await asyncio.sleep(10)
            continue
        for update in updates:
            offset = update["update_id"] + 1
            try:
                await handle_update(session, service, channel, token, update)
            except Exception:
                LOG.exception("Telegram update failed for %s", channel.name)
            service.journal.save_telegram_offset(bot["id"], offset)


async def handle_update(session, service, channel, token, update):
    from .telegram import telegram_call

    query = update.get("callback_query")
    if query and getattr(service, "telegram_ux", None):
        reply = await service.telegram_ux.callback(query, session)
        await telegram_call(session, token, "answerCallbackQuery",
                            {"callback_query_id": query["id"], "text": reply[:200], "show_alert": False})
        return
    message = update.get("message") or {}
    text = (message.get("text") or "").strip()
    if str(message.get("chat", {}).get("id", "")) != channel.chat_id:
        return  # only the configured chat may ask
    if text.split("@")[0] == "/stats":
        await channel.sender.send_telegram(stats_text(service, channel), chat_id=channel.chat_id)
