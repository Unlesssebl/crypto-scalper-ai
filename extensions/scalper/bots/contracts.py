"""Declared contracts for the measurement bots.

A bot here is an observer, not a trader: it has no balance, no position and no
leverage.  The contract is frozen before observations are collected so that a
threshold change cannot silently reinterpret old data.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

# Strength scales are deliberately local to a bot family.  Comparing "3" from
# the flow scale with "3" from the funding scale would be meaningless.
STRENGTH_SCALES = {
    "flow_rank_0_4": {
        0: "нет свежих данных",
        1: "фоновая",
        2: "заметная",
        3: "выраженное движение",
        4: "сильное движение",
    },
    "attention_rank_0_4": {
        0: "нет данных",
        1: "фон",
        2: "обзорный слот",
        3: "детальный слот",
        4: "детальный слот + подтверждение",
    },
    "ai_rank_0_4": {
        0: "нет данных",
        1: "слабый эпизод",
        2: "заметный эпизод",
        3: "сильный эпизод",
        4: "очень сильный эпизод",
    },
    "formation_rank_0_4": {
        0: "нет данных",
        1: "фоновая зона",
        2: "заметная зона",
        3: "подтверждённая зона",
        4: "сильная подтверждённая зона",
    },
    "liquidation_rank_0_4": {
        0: "нет данных",
        1: "мелкая серия",
        2: "заметная серия",
        3: "крупная серия",
        4: "каскад",
    },
    "tape_rank_0_4": {
        0: "нет данных",
        1: "импульс < 0.25 NATR",
        2: "импульс 0.25–0.5 NATR",
        3: "импульс 0.5–1 NATR",
        4: "импульс ≥ 1 NATR",
    },
    "whale_rank_0_4": {
        0: "нет данных",
        1: "малая позиция",
        2: "заметная позиция",
        3: "крупная позиция",
        4: "очень крупная позиция",
    },
    "funding_rank_0_4": {
        0: "нет данных",
        1: "слабый перекос",
        2: "заметный перекос",
        3: "сильный перекос",
        4: "экстремальный перекос",
    },
}


@dataclass(frozen=True)
class BotSpec:
    """One pre-registered observation contract."""

    id: str
    mode: str  # "directional" | "absolute"
    trigger: str
    strength: str
    hypothesis: str
    version: int = 1
    primary_horizon_s: int = 300
    cooldown_s: int = 300
    control: str = "random_natr_5"
    primary_test: str = ""
    window: str = "5m"
    enabled: bool = True
    note: str = ""

    def payload(self) -> dict:
        return asdict(self)

    @property
    def contract_hash(self) -> str:
        # Notes and the enabled flag are operational metadata: editing a note
        # must not force a version bump or invalidate old observations.
        payload = {key: value for key, value in self.payload().items() if key not in {"note", "enabled"}}
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]

    @property
    def scale_labels(self) -> dict[int, str]:
        return STRENGTH_SCALES.get(self.strength, {})


# Nine active observers.  funding.contrarian is declared but disabled: its
# natural horizon is 1-4 hours, so it cannot be compared on the 5-minute grid.
BOTS: tuple[BotSpec, ...] = (
    BotSpec(
        id="flow.continuation",
        mode="directional",
        trigger="flow_continuation",
        strength="flow_rank_0_4",
        hypothesis="Сильный поток (rank 3–4) продолжается лучше слабого (rank 1–2)",
        primary_test="mean signed bps, rank 3-4 > rank 1-2",
        window="5m",
        # v2: controls from the whole scouted universe matched on scout
        # volatility. v1 matched NATR of scan rows, which in the unified
        # runtime left every control set empty.
        version=2,
        control="random_scout_vol_5",
        note="Первая волна: журнал, исходы, Telegram-отчёт.",
    ),
    BotSpec(
        id="tape.reading",
        mode="directional",
        trigger="tape_reading",
        strength="tape_rank_0_4",
        hypothesis="Направление, которое подразумевает прочтение ленты (перехват, поглощение, истощение — "
                   "разворот; слабый объём, продолжение — дальше), сбывается лучше контролей",
        primary_test="mean signed bps > controls; rank 3-4 > rank 1-2; slices by reading kind",
        window="5m",
        control="random_scout_vol_5",
        note="Событие из карточек (microstructure.read_tape), не из строк скринера.",
    ),
    BotSpec(
        id="attention.relative_move",
        mode="absolute",
        trigger="attention_relative_move",
        strength="attention_rank_0_4",
        hypothesis="Инструмент, только что получивший детальный слот (аномалия или мувер), двигается "
                   "сильнее одновременных контролей вне отбора",
        primary_test="excess absolute move vs controls; slices by slot (detail/mover)",
        window="5m",
        # v3: event on getting a detail slot after 10 minutes without one (v2 used plan.selected, whose
        # scout tail rotates through hundreds of symbols a minute); controls drawn outside the plan.
        version=3,
        control="random_scout_vol_5",
        note="Отбор — относительный рейтинг, не рекомендация входа.",
    ),
    BotSpec(
        id="ai.direction",
        mode="directional",
        trigger="ai_direction",
        strength="ai_rank_0_4",
        hypothesis="Направление AI не хуже алгоритма на тех же эпизодах",
        primary_test="mean signed bps > 0 and non-inferior to algorithm",
        window="5m",
    ),
    BotSpec(
        id="ai.context",
        mode="absolute",
        trigger="ai_context",
        strength="ai_rank_0_4",
        hypothesis="После содержательного разбора движение выше фона",
        primary_test="excess absolute move vs controls",
        window="5m",
    ),
    BotSpec(
        id="formation.hold",
        mode="directional",
        trigger="level_hold",
        strength="formation_rank_0_4",
        hypothesis="Когда цена подходит к значимому уровню, она отходит от него: от спроса вверх, "
                   "от предложения вниз",
        primary_test="mean signed bps in the hold direction > controls",
        window="5m",
        # v2: fed by the levels bot's approach event; directional; scout-volatility controls.
        version=2,
        control="random_scout_vol_5",
    ),
    BotSpec(
        id="formation.break",
        mode="directional",
        trigger="level_break",
        strength="formation_rank_0_4",
        hypothesis="Когда цена подходит к значимому уровню, она проходит его и продолжает",
        primary_test="mean signed bps through the level > controls",
        window="5m",
        version=2,
        control="random_scout_vol_5",
    ),
    BotSpec(
        id="liquidation.continuation",
        mode="directional",
        trigger="liquidation_cascade",
        strength="liquidation_rank_0_4",
        hypothesis="После крупных подтверждённых ликвидаций цена продолжает в сторону принудительных закрытий",
        primary_test="mean signed continuation bps > controls",
        window="5m",
        # v2: confirmed liquidations only, sized against the instrument's average minute of volume.
        version=2,
        control="random_scout_vol_5",
    ),
    BotSpec(
        id="liquidation.exhaustion",
        mode="directional",
        trigger="liquidation_exhaustion",
        strength="liquidation_rank_0_4",
        hypothesis="После крупных подтверждённых ликвидаций цена разворачивается",
        primary_test="mean signed reversal bps > controls",
        window="5m",
        version=2,
        control="random_scout_vol_5",
    ),
    BotSpec(
        id="whale.follow",
        mode="directional",
        trigger="whale_follow",
        strength="whale_rank_0_4",
        hypothesis="После открытия или увеличения крупной позиции на Hyperliquid цена идёт в её сторону",
        primary_test="mean signed move in whale direction > controls",
        window="5m",
        # v3: new/increased positions from the scanner, sized against the coin's average minute of
        # Hyperliquid + Binance volume (v2 used whatever subset the screener had loaded). v4: "opened" only
        # on an address read before without the position (v3 counted the scanner refilling after a restart).
        version=4,
        control="random_scout_vol_5",
        note="Позиция видна с задержкой сканера (lag_s); исход меряется от момента обнаружения.",
    ),
    BotSpec(
        id="funding.contrarian",
        mode="directional",
        trigger="funding_contrarian",
        strength="funding_rank_0_4",
        hypothesis="Funding/basis перекос возвращается к норме",
        primary_test="mean signed reversal on 1-4h horizon",
        window="1h",
        primary_horizon_s=3600,
        enabled=False,
        note="Отложен: не влезает в 5-минутную сетку первой волны.",
    ),
)


def active_specs() -> tuple[BotSpec, ...]:
    return tuple(spec for spec in BOTS if spec.enabled)


def spec_by_id(bot_id: str) -> BotSpec | None:
    return next((spec for spec in BOTS if spec.id == bot_id), None)
