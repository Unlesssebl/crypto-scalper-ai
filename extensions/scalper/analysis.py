"""Independent Gemini analysis and deterministic comparison of two votes."""
import asyncio
import json
import re

import aiohttp

DIRECTIONAL = {"BULLISH", "BEARISH"}
AI_DIRECTIONS = ("BULLISH", "BEARISH", "NEUTRAL", "INSUFFICIENT_DATA")
TEXT_FIELDS = ("summary", "scenario", "invalidation", "watch")
KEY_FIELDS = ("symbol", "exchange", "contract", "window", "captured_at")
PROMPT = """Независимо оцени данный снимок Hyperliquid perpetual для ручного наблюдения.
Используй как доказательства только метрики с quality=ok. Ссылайся на них по metric.
Направление требует подтверждения из нескольких независимых групп метрик.
CVD, OFI, объём и число сделок относятся к одной группе потока.
OI, funding и стакан по отдельности не задают направление.
При нехватке данных выбери INSUFFICIENT_DATA. Раздели аргументы и контраргументы.
Не пересчитывай индикаторы, не добавляй числа или уровни, не давай команд на сделки.
Пиши кратко по-русски, формулируй сценарий как гипотезу."""
ARGUMENT = {
    "type": "object", "required": ["metric", "reason"], "additionalProperties": False,
    "properties": {"metric": {"type": "string"}, "reason": {"type": "string"}},
}
SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["direction", *TEXT_FIELDS, "arguments", "contradictions"],
    "properties": {
        "direction": {"type": "string", "enum": list(AI_DIRECTIONS)},
        **{field: {"type": "string"} for field in TEXT_FIELDS},
        **{field: {"type": "array", "maxItems": 4, "items": ARGUMENT}
           for field in ("arguments", "contradictions")},
    },
}
GROUPS = {
    "cvd": "flow", "ofi": "flow", "volume": "flow", "trade_count": "flow",
    "imbalance": "book", "spread": "book", "best_bid": "book", "best_ask": "book",
}


def snapshot_key(snapshot):
    return {field: snapshot[field] for field in KEY_FIELDS}


def blind_payload(snapshot):
    return {**snapshot_key(snapshot), "metrics": snapshot["metrics"]}


def valid_text(value):
    return (isinstance(value, str) and 0 < len(value.strip()) <= 240
            and not any(char.isdigit() for char in value))


def validate(raw, snapshot):
    reply = json.loads(raw)
    if not isinstance(reply, dict) or set(reply) != set(SCHEMA["required"]):
        raise ValueError("Invalid Gemini response structure")
    if reply["direction"] not in AI_DIRECTIONS or not all(valid_text(reply[key]) for key in TEXT_FIELDS):
        raise ValueError("Invalid Gemini response fields")
    usable = {key for key, value in snapshot["metrics"].items()
              if value["quality"] == "ok" and value["value"] is not None}
    for field in ("arguments", "contradictions"):
        items = reply[field]
        if not isinstance(items, list) or len(items) > 4:
            raise ValueError("Invalid Gemini evidence list")
        for arg in items:
            if (not isinstance(arg, dict) or set(arg) != {"metric", "reason"}
                    or not isinstance(arg["metric"], str) or arg["metric"] not in usable
                    or not valid_text(arg["reason"])):
                raise ValueError("Gemini cited unavailable evidence")
    if reply["direction"] in DIRECTIONAL:
        groups = {GROUPS.get(arg["metric"], arg["metric"]) for arg in reply["arguments"]}
        if "price" not in usable or len(groups) < 2 or "flow" not in groups:
            raise ValueError("Gemini supplied insufficient directional evidence")
    if reply["direction"] == "NEUTRAL" and not reply["arguments"]:
        raise ValueError("Gemini supplied no evidence for neutrality")
    return reply


class Gemini:
    def __init__(self, session, model, key):
        if not re.fullmatch(r"[a-zA-Z0-9._-]+", model):
            raise ValueError("GEMINI_MODEL must be a model ID")
        self.session, self.model, self.key = session, model, key

    async def analyze(self, snapshot):
        result = {"snapshot": snapshot_key(snapshot), "model": self.model}
        if not self.key:
            return {**result, "status": "unconfigured"}
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        request = {
            "systemInstruction": {"parts": [{"text": PROMPT}]},
            "contents": [{"parts": [{"text": json.dumps(blind_payload(snapshot), allow_nan=False)}]}],
            "generationConfig": {
                "responseMimeType": "application/json", "responseJsonSchema": SCHEMA,
            },
        }
        try:
            async with self.session.post(
                url, json=request, headers={"x-goog-api-key": self.key},
                timeout=aiohttp.ClientTimeout(total=25), allow_redirects=False,
            ) as response:
                if response.status != 200:
                    return {**result, "status": f"http_{response.status}"}
                payload = await response.json()
            candidate = payload["candidates"][0]
            if candidate.get("finishReason") != "STOP":
                return {**result, "status": "incomplete"}
            raw = "".join(part.get("text", "") for part in candidate["content"]["parts"]
                          if not part.get("thought"))
            return {**result, **validate(raw, snapshot), "status": "ok",
                    "model": payload.get("modelVersion", self.model)}
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return {**result, "status": "network_error"}
        except (KeyError, IndexError, ValueError, TypeError, AttributeError):
            return {**result, "status": "invalid_response"}


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
