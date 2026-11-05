"""AI admission policy; no market prediction and no changes to the native vote."""
import os
from dataclasses import dataclass

from .activity import finding_strength, wide_spread
from .profile import fresh

MAX_AGE = {"1m": 45, "5m": 120}
REASON_LABELS = {
    "hourly_limit": "превышен часовой лимит", "cooldown": "пауза по инструменту",
    "unconfigured": "нет API-ключа", "queued": "в очереди", "analyzing": "анализ выполняется",
    "ordinary_flow": "обычный поток", "neutral": "нейтральный поток",
    "no_independent_confirmation": "нет независимого подтверждения", "early_disabled": "ранние импульсы отключены",
    "not_selected": "не выбран", "error": "ошибка анализа", "episode_pending": "эпизод уже в очереди",
    "queue_full": "очередь заполнена", "event_expired": "событие устарело",
    "stale_snapshot": "снимок устарел", "stale_data": "нет свежих данных",
    "stale_at_dispatch": "данные устарели перед отправкой", "wide_spread": "широкий спред",
    "left_universe": "инструмент вышел из Top-20", "anomaly_ended": "всплеск завершился",
    "direction_changed": "направление изменилось", "interrupted": "обработка прервана перезапуском",
    "ok": "анализ завершён", "network_error": "ошибка сети", "invalid_response": "некорректный ответ модели",
    "incomplete": "неполный ответ модели", "expired": "срок анализа истёк",
    "tape_unavailable": "лента недоступна", "book_unavailable": "стакан недоступен",
}


@dataclass(frozen=True)
class OpportunityPolicy:
    mode: str = "balanced"
    hourly_limit: int = 12
    cooldown_seconds: int = 300
    early: bool = True
    experiments: bool = False

    def __post_init__(self):
        if self.mode not in {"economical", "balanced", "research"}:
            raise ValueError("Unknown opportunity mode")
        if self.hourly_limit < 0 or self.cooldown_seconds < 1:
            raise ValueError("AI limit must be nonnegative and cooldown positive")

    @classmethod
    def from_env(cls):
        return cls(
            mode=os.getenv("SCALPER_AI_MODE", "balanced"),
            hourly_limit=int(os.getenv("SCALPER_AI_HOURLY_LIMIT", "12")),
            cooldown_seconds=int(os.getenv("SCALPER_AI_COOLDOWN_SECONDS", "300")),
            early=os.getenv("SCALPER_AI_EARLY", "true").lower() == "true",
            experiments=os.getenv("SCALPER_AI_EXPERIMENTS", "false").lower() == "true",
        )

    def classify(self, snapshot, native, now):
        m = snapshot["metrics"]
        if not 0 <= now - snapshot["captured_at"] <= 10:
            return None, "stale_snapshot"
        # Check timestamps as well as quality flags, including at dispatch.
        for name, age in (("price", 20), ("ofi", min(60, MAX_AGE[snapshot["window"]])), ("spread", 15)):
            item = m.get(name, {})
            stamp = item.get("updated_at")
            if (fresh(m, name) is None or not isinstance(stamp, (float, int))
                    or not -5 <= now - stamp <= age):
                return None, "stale_data"
        if native["direction"] not in {"BULLISH", "BEARISH"}:
            return None, "neutral"
        if wide_spread(snapshot):
            return None, "wide_spread"
        if finding_strength(snapshot, native)["rank"] >= 3:
            return "confirmed", "eligible"
        if self.mode == "economical" or not self.early:
            return None, "early_disabled"
        sign = 1 if native["direction"] == "BULLISH" else -1
        ofi, pace, count = (fresh(m, name) for name in ("ofi", "activity_ratio", "trade_count"))
        threshold = 1.5 if self.mode == "research" else 2.0
        if ofi is None or sign * ofi < 0.6 or pace is None or pace < threshold or not count or count < 10:
            return None, "ordinary_flow"
        # OFI and pace are one evidence family. Require price action OR book.
        start, end, spread, imbalance = (fresh(m, name) for name in
                                        ("first_trade_price", "last_trade_price", "spread", "imbalance"))
        price_ok = bool(start and end and spread is not None and sign * (end - start) > max(spread, start * 0.0001))
        book_stamp = m.get("imbalance", {}).get("updated_at")
        book_ok = (imbalance is not None and sign * imbalance >= 0.25
                   and isinstance(book_stamp, (int, float)) and -5 <= now - book_stamp <= 15)
        if price_ok or book_ok:
            return "early", "eligible"
        return None, "no_independent_confirmation"
