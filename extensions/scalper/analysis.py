"""Independent model analysis and deterministic comparison of two votes."""
import asyncio
import json
import re
from copy import deepcopy

import aiohttp

DIRECTIONAL = {"BULLISH", "BEARISH"}
AI_DIRECTIONS = ("BULLISH", "BEARISH", "NEUTRAL", "INSUFFICIENT_DATA")
TEXT_LIMITS = {"summary": 720, "scenario": 200}
KEY_FIELDS = ("symbol", "exchange", "contract", "window", "captured_at")
PROMPT = """Дай развёрнутую характеристику текущей ситуации Hyperliquid perpetual.
В summary напиши связный текст из нескольких предложений: как реагирует цена на поток,
что показывают интенсивность, волатильность и стакан, подтверждают ли картину другие окна.
Если метрики или окна расходятся, назови обе стороны расхождения и объясни,
что оно меняет в трактовке. Если сопоставление невозможно, укажи, каких данных не хватает.
Объединяй факты по смыслу. Не перечисляй все метрики подряд и не предлагай действий.
В scenario оставь одну короткую условную гипотезу. При слабой опоре на данные оставь пустую строку.
Не давай рекомендаций, торговых инструкций или целевой цены.
instrument_profile, если есть, описывает историческую норму инструмента и размер выборки.
Используй профиль как фон для сравнения, а не как доказательство текущего направления.
Используй как доказательства только метрики с quality=ok. Ссылайся на них по metric.
Направление требует подтверждения из нескольких независимых групп метрик.
CVD, OFI, число сделок и скорость ленты описывают один поток. Торговый объём даёт контекст.
NATR показывает типичный относительный диапазон, activity_ratio и range_ratio — всплески.
Всплески и волатильность показывают активность, направление ими не подтверждается.
first_trade_price и last_trade_price — наблюдавшиеся сделки на границах текущего окна,
используй их для контекста реакции цены. Для направления сопоставь поток с независимым
фактором, например с реакцией цены или стаканом.
OI, funding и стакан по отдельности не задают направление.
При нехватке данных выбери INSUFFICIENT_DATA. Раздели аргументы и контраргументы.
Не пересчитывай индикаторы, не добавляй числа или уровни, не давай команд на сделки.
Пиши по-русски. Характеристика — до 720 символов, гипотеза — до 200.
В каждом аргументе metric — ровно один точный ключ из metrics, без списков и объединений.
Формулируй сценарий как гипотезу.
Проверяй разные роли метрик: движение цены, направленный поток, интенсивность, волатильность и ликвидность.
Не считай несколько метрик одной роли отдельными подтверждениями.
context_1m описывает текущий импульс; context_5m даёт более широкий контекст.
flow_quality отдельно показывает доступность OFI и CVD. При статусе, отличном от ok,
не делай вывод о направлении или нейтральности потока этого окна.
movement_context содержит наблюдаемые возраст, расстояние и откат от максимума в NATR.
Положительное расстояние означает движение по наблюдаемому импульсу. Null означает неизвестное значение.
Учитывай, сколько движение уже прошло; отсутствие наблюдаемого начала не означает ранний импульс.
context_15m содержит фон по закрытым свечам: изменение за четыре свечи, их диапазон и положение последнего закрытия.
Если окна заметно расходятся, объясни это; старшее окно не отменяет автоматически краткий импульс.
Если есть btc_context, сравни последнюю закрытую свечу инструмента с BTC.
Корреляция доходностей описывает связь в прошедшем окне и сама не задаёт направление или причину движения.
market_context, если есть, — это рыночный фон: long/short-соотношение счетов (lsr),
spot-perp базис (basis), подразумеваемая волатильность (iv), ликвидации за час,
крупнейшие отслеживаемые позиции (whales) и экстремальный финансинг (extreme_funding).
Это контекст, а не доказательство направления текущего окна: используй его для оценки
риск-аппетита и уровня рынка, но не как аргумент направления в argument/contradiction.
В списках аргументов ссылайся только на ключи основного окна из metrics."""
ARGUMENT = {
    "type": "object", "required": ["metric", "reason"], "additionalProperties": False,
    "properties": {"metric": {"type": "string"}, "reason": {"type": "string", "maxLength": 240}},
}
SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["direction", *TEXT_LIMITS, "arguments", "contradictions"],
    "properties": {
        "direction": {"type": "string", "enum": list(AI_DIRECTIONS)},
        **{field: {"type": "string", "maxLength": limit} for field, limit in TEXT_LIMITS.items()},
        **{field: {"type": "array", "maxItems": 4, "items": deepcopy(ARGUMENT)}
           for field in ("arguments", "contradictions")},
    },
}
GROUPS = {
    "first_trade_price": "price_action", "last_trade_price": "price_action",
    "cvd": "flow", "ofi": "flow",
    "volume": "intensity", "volume_24h": "intensity", "trade_count": "intensity",
    "trade_rate": "intensity", "activity_ratio": "intensity",
    "natr": "volatility", "range_ratio": "volatility",
    "imbalance": "book", "spread": "liquidity", "best_bid": "liquidity", "best_ask": "liquidity",
    "funding_rate": "positioning", "open_interest": "positioning",
}


def snapshot_key(snapshot):
    return {field: snapshot[field] for field in KEY_FIELDS}


def blind_payload(snapshot):
    metrics = snapshot["metrics"]
    payload = {**snapshot_key(snapshot), "metrics": metrics,
               "metric_roles": {key: GROUPS[key] for key in metrics if key in GROUPS}}
    if profile := snapshot.get("instrument_profile"):
        payload["instrument_profile"] = profile
    if context := snapshot.get("btc_context"):
        payload["btc_context"] = {
            key: context[key] for key in ("reference", "window", "samples", "updated_at", "correlation",
                                          "btc_last_return_pct", "asset_last_return_pct", "relative_last_return_pp")
        }
    if movement := snapshot.get("movement_context"):
        payload["movement_context"] = {field: movement[field] for field in
                                       ("status", "age_seconds", "distance_natr", "retracement_natr", "natr_at_start")
                                       if field in movement}
    for key in ("context_1m", "context_5m", "context_15m"):
        if context := snapshot.get(key):
            payload[key] = {field: context[field] for field in ("window", "captured_at", "metrics")}
            if "flow_quality" in context:
                payload[key]["flow_quality"] = context["flow_quality"]
    if market := snapshot.get("market_context"):
        payload["market_context"] = market
    return payload


def response_schema(snapshot):
    schema = deepcopy(SCHEMA)
    usable = [key for key, item in snapshot["metrics"].items()
              if item["quality"] == "ok" and item["value"] is not None]
    for field in ("arguments", "contradictions"):
        evidence = schema["properties"][field]
        if usable:
            evidence["items"]["properties"]["metric"]["enum"] = usable
        else:
            evidence["maxItems"] = 0
    return schema


MODEL_NUMBER = re.compile(r"[+−-]?\d+(?:[.,:/–-]\d+)*(?:%|[mM](?!\w))?")


def clean_text(value, limit=240):
    if not isinstance(value, str):
        raise ValueError("Invalid model text")
    text = " ".join(MODEL_NUMBER.sub("…", value).split())
    if len(text) > limit:
        text = text[:limit - 1].rsplit(" ", 1)[0] + "…"
    if not any(char.isalpha() for char in text):
        raise ValueError("Invalid model text")
    return text


def validate(raw, snapshot):
    reply = json.loads(raw)
    if not isinstance(reply, dict) or set(reply) != set(SCHEMA["required"]):
        raise ValueError("Invalid model response structure")
    if reply["direction"] not in AI_DIRECTIONS:
        raise ValueError("Invalid model direction")
    reply["summary"] = clean_text(reply["summary"], TEXT_LIMITS["summary"])
    scenario = reply["scenario"]
    if isinstance(scenario, str) and not scenario.strip():
        reply["scenario"] = ""
    else:
        reply["scenario"] = clean_text(scenario, TEXT_LIMITS["scenario"])
    usable = {key for key, value in snapshot["metrics"].items()
              if value["quality"] == "ok" and value["value"] is not None}
    for field in ("arguments", "contradictions"):
        items = reply[field]
        if not isinstance(items, list) or len(items) > 4:
            raise ValueError("Invalid model evidence list")
        for arg in items:
            if (not isinstance(arg, dict) or set(arg) != {"metric", "reason"}
                    or not isinstance(arg["metric"], str) or arg["metric"] not in usable):
                raise ValueError("Model cited unavailable evidence")
            arg["reason"] = clean_text(arg["reason"])
    if reply["direction"] in DIRECTIONAL:
        groups = {GROUPS.get(arg["metric"], arg["metric"]) for arg in reply["arguments"]}
        if "price" not in usable or "flow" not in groups or not groups.intersection({"price_action", "book"}):
            raise ValueError("Model supplied insufficient directional evidence")
    if reply["direction"] == "NEUTRAL" and not reply["arguments"]:
        raise ValueError("Model supplied no evidence for neutrality")
    return reply


class Gemini:
    model_pattern = r"[a-zA-Z0-9._-]+"

    def __init__(self, session, model, key, *, proxy=None):
        if not re.fullmatch(self.model_pattern, model):
            raise ValueError(f"Invalid model ID for {self.provider}")
        self.session, self.model, self.key = session, model, key
        self.proxy = proxy

    provider = "gemini"

    def request(self, market, schema):
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        request = {
            "systemInstruction": {"parts": [{"text": PROMPT}]},
            "contents": [{"parts": [{"text": market}]}],
            "generationConfig": {
                "responseMimeType": "application/json", "responseJsonSchema": schema,
            },
        }
        return url, request, {"x-goog-api-key": self.key}

    def unpack(self, payload):
        candidate = payload["candidates"][0]
        if candidate.get("finishReason") != "STOP":
            return None, self.model
        raw = "".join(part.get("text", "") for part in candidate["content"]["parts"]
                      if not part.get("thought"))
        return raw, payload.get("modelVersion", self.model)

    async def analyze(self, snapshot):
        result = {"snapshot": snapshot_key(snapshot), "model": self.model, "provider": self.provider}
        if not self.key:
            return {**result, "status": "unconfigured"}
        market = json.dumps(blind_payload(snapshot), allow_nan=False, separators=(",", ":"))
        url, request, headers = self.request(market, response_schema(snapshot))
        try:
            async with self.session.post(
                url, json=request, headers=headers, proxy=self.proxy,
                timeout=aiohttp.ClientTimeout(total=25), allow_redirects=False,
            ) as response:
                if response.status != 200:
                    return {**result, "status": f"http_{response.status}"}
                payload = await response.json()
            raw, model = self.unpack(payload)
            if raw is None:
                return {**result, "status": "incomplete"}
            return {**result, **validate(raw, snapshot), "status": "ok", "model": model}
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return {**result, "status": "network_error"}
        except (KeyError, IndexError, ValueError, TypeError, AttributeError):
            return {**result, "status": "invalid_response"}


class OpenRouter(Gemini):
    provider = "openrouter"
    model_pattern = r"[a-zA-Z0-9._-]+/[a-zA-Z0-9._:-]+"

    def request(self, market, schema):
        request = {
            "model": self.model,
            "messages": [{"role": "system", "content": PROMPT}, {"role": "user", "content": market}],
            "provider": {"require_parameters": True},
            "max_tokens": 1000,
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "scalper_assessment", "strict": True, "schema": schema},
            },
        }
        if self.model.startswith("deepseek/deepseek-v4.1-flash"):
            request["reasoning"] = {"enabled": False}
        return "https://openrouter.ai/api/v1/chat/completions", request, {"Authorization": f"Bearer {self.key}"}

    def unpack(self, payload):
        choice = payload["choices"][0]
        if choice.get("finish_reason") != "stop":
            return None, self.model
        return choice["message"]["content"], payload.get("model", self.model)


def compare(snapshot, hyperdata, ai):
    identity = snapshot_key(snapshot)
    if hyperdata.get("snapshot") != identity or ai.get("snapshot") != identity:
        raise ValueError("Cannot compare votes from different market snapshots")
    native = hyperdata["direction"]
    model = ai.get("direction") if ai["status"] == "ok" else None
    if native in DIRECTIONAL and model in DIRECTIONAL:
        if native != model:
            category = "DIVERGENCE"
        else:
            category = "CONSENSUS_LONG" if native == "BULLISH" else "CONSENSUS_SHORT"
    elif native in DIRECTIONAL:
        category = "HYPERDATA_ONLY"
    elif model in DIRECTIONAL:
        category = "AI_ONLY"
    elif native == model == "NEUTRAL":
        category = "NEUTRAL"
    else:
        category = "INSUFFICIENT_DATA"

    native_evidence = {GROUPS.get(field, field) for field in hyperdata["evidence"]}
    model_evidence = {GROUPS.get(arg["metric"], arg["metric"]) for arg in ai.get("arguments", [])}
    shared = sorted(native_evidence & model_evidence)
    overlap = "UNKNOWN"
    if native_evidence and model_evidence and ai["status"] == "ok":
        if not shared:
            overlap = "LOW_OVERLAP"
        elif native == model and native_evidence <= model_evidence:
            overlap = "HIGH_OVERLAP"
        else:
            overlap = "PARTIAL_OVERLAP"
    return {"category": category, "overlap": overlap, "shared_evidence": shared}
