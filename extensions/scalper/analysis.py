"""Independent model analysis and deterministic comparison of two votes."""
import asyncio
import json
import re
from copy import deepcopy

import aiohttp

DIRECTIONAL = {"BULLISH", "BEARISH"}
AI_DIRECTIONS = ("BULLISH", "BEARISH", "NEUTRAL", "INSUFFICIENT_DATA")
TEXT_FIELDS = ("summary", "scenario", "invalidation", "watch")
KEY_FIELDS = ("symbol", "exchange", "contract", "window", "captured_at")
PROMPT = """Независимо оцени данный снимок Hyperliquid perpetual для ручного наблюдения.
Используй как доказательства только метрики с quality=ok. Ссылайся на них по metric.
Направление требует подтверждения из нескольких независимых групп метрик.
CVD, OFI, объём и число сделок относятся к одной группе потока.
first_trade_price и last_trade_price — наблюдавшиеся сделки на границах текущего окна,
используй их для контекста реакции цены. Для направления сопоставь поток с независимым
фактором, например с реакцией цены или стаканом.
OI, funding и стакан по отдельности не задают направление.
При нехватке данных выбери INSUFFICIENT_DATA. Раздели аргументы и контраргументы.
Не пересчитывай индикаторы, не добавляй числа или уровни, не давай команд на сделки.
Пиши по-русски. Каждое текстовое поле ограничено 240 символами.
В каждом аргументе metric — ровно один точный ключ из metrics, без списков и объединений.
Формулируй сценарий как гипотезу."""
ARGUMENT = {
    "type": "object", "required": ["metric", "reason"], "additionalProperties": False,
    "properties": {"metric": {"type": "string"}, "reason": {"type": "string", "maxLength": 240}},
}
SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["direction", *TEXT_FIELDS, "arguments", "contradictions"],
    "properties": {
        "direction": {"type": "string", "enum": list(AI_DIRECTIONS)},
        **{field: {"type": "string", "maxLength": 240} for field in TEXT_FIELDS},
        **{field: {"type": "array", "maxItems": 4, "items": deepcopy(ARGUMENT)}
           for field in ("arguments", "contradictions")},
    },
}
GROUPS = {
    "first_trade_price": "price_action", "last_trade_price": "price_action",
    "cvd": "flow", "ofi": "flow", "volume": "flow", "trade_count": "flow",
    "imbalance": "book", "spread": "book", "best_bid": "book", "best_ask": "book",
}


def snapshot_key(snapshot):
    return {field: snapshot[field] for field in KEY_FIELDS}


def blind_payload(snapshot):
    return {**snapshot_key(snapshot), "metrics": snapshot["metrics"]}


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


def valid_text(value):
    return (isinstance(value, str) and 0 < len(value.strip()) <= 240
            and not any(char.isdigit() for char in value))


def validate(raw, snapshot):
    reply = json.loads(raw)
    if not isinstance(reply, dict) or set(reply) != set(SCHEMA["required"]):
        raise ValueError("Invalid model response structure")
    if reply["direction"] not in AI_DIRECTIONS or not all(valid_text(reply[key]) for key in TEXT_FIELDS):
        raise ValueError("Invalid model response fields")
    usable = {key for key, value in snapshot["metrics"].items()
              if value["quality"] == "ok" and value["value"] is not None}
    for field in ("arguments", "contradictions"):
        items = reply[field]
        if not isinstance(items, list) or len(items) > 4:
            raise ValueError("Invalid model evidence list")
        for arg in items:
            if (not isinstance(arg, dict) or set(arg) != {"metric", "reason"}
                    or not isinstance(arg["metric"], str) or arg["metric"] not in usable
                    or not valid_text(arg["reason"])):
                raise ValueError("Model cited unavailable evidence")
    if reply["direction"] in DIRECTIONAL:
        groups = {GROUPS.get(arg["metric"], arg["metric"]) for arg in reply["arguments"]}
        if "price" not in usable or len(groups) < 2 or "flow" not in groups:
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
        market = json.dumps(blind_payload(snapshot), allow_nan=False)
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
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "scalper_assessment", "strict": True, "schema": schema},
            },
        }
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
