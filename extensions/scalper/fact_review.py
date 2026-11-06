"""Independent validation of model assertions, not voting on LONG/SHORT."""

import asyncio
import json
import math
import re

import aiohttp

from .fact_packet import canonical, checksum, verify_packet

STATUSES = ("supported", "contradicted", "unverifiable", "hypothesis")
VALIDATOR_VERSION = "context-checks-v2"
LABELS = {
    "supported": "Подтверждено измерениями",
    "contradicted": "Противоречит измерениям",
    "unverifiable": "Недостаточно данных",
    "hypothesis": "Гипотеза",
}
PROMPT = """Ты помощник наблюдения за рынком. Не прогнозируй направление и не рекомендуй входы.
Верни JSON по схеме. packet_id скопируй точно. Используй только facts этого пакета.
observations: проверяемые копии значений фактов; не создавай новые числа или уровни.
contradictions: только проверяемое сравнение двух имеющихся фактов одинаковой единицы и роли;
relation описывает утверждаемое отношение, а не доказательство рыночной причинности.
interpretations: предположения со ссылками на факты. text по-русски, без чисел и торговых команд.
Поглощение, пополнение и связь контекстов всегда гипотезы, не установленные измерения.
missing_data: id фактов с unknown/stale. Отсутствие наблюдения не означает отсутствие явления.
Не выдумывай возраст заявки, намерения участника и непрерывность до начала наблюдения.
Одинаковые исходные данные не являются независимыми подтверждениями. При недостатке пригодных
данных abstention=true. Итоговые статусы твоих утверждений определяет валидатор, не ты."""
PROMPT += """
Строгое правило abstention: если observations, interpretations или contradictions не пусты,
abstention ОБЯЗАТЕЛЬНО false. Если abstention=true, ВСЕ эти три массива ОБЯЗАТЕЛЬНО пусты;
тогда можно заполнить только missing_data. Не путай отсутствие торговой рекомендации с воздержанием:
торговых рекомендаций нет ни в одном ответе, но доступные факты можно отметить при abstention=false.
Выбирай несколько важных наблюдений; не переписывай все поля подряд. При наличии history.* учитывай
только зафиксированные прошлые наблюдения того же эпизода; их устаревание для текущего рынка явно отмечено.
Для id утверждений используй только латинские буквы, цифры, '_' и '-', например obs_1, interp_1, cmp_1.
Все id уникальны между секциями. Не копируй dotted fact_id в id; точки разрешены только в ссылках на факты.
"""


def schema(packet):
    ids = [f["id"] for f in packet["facts"]]
    reference = {"type": "string", "enum": ids}

    def obj(properties):
        return dict(type="object", additionalProperties=False, properties=properties, required=list(properties))

    def array(items, maximum):
        return dict(type="array", items=items, maxItems=maximum)

    identity = {"type": "string", "maxLength": 64, "pattern": "^[a-zA-Z0-9_-]{1,64}$"}
    return obj(
        dict(
            packet_id={"type": "string", "enum": [packet["packet_id"]]},
            observations=array(
                obj(
                    dict(
                        id=identity,
                        fact_id=reference,
                        value={"anyOf": [{"type": t} for t in ("number", "string", "boolean", "null")]},
                    )
                ),
                12,
            ),
            interpretations=array(
                obj(
                    dict(
                        id=identity,
                        kind={
                            "type": "string",
                            "enum": [
                                "possible_absorption",
                                "possible_replenishment",
                                "flow_price_mismatch",
                                "context_link",
                                "other",
                            ],
                        },
                        evidence=array(reference, 6),
                        text={"type": "string", "maxLength": 400},
                    )
                ),
                4,
            ),
            contradictions=array(
                obj(
                    dict(
                        id=identity,
                        left=reference,
                        right=reference,
                        relation={"type": "string", "enum": ["equal", "greater", "less"]},
                    )
                ),
                6,
            ),
            missing_data=array(reference, 12),
            abstention={"type": "boolean"},
        )
    )


def validate_review(raw, packet, now):
    """Syntax errors reject the response; false factual claims remain visible errors.

    Free-text interpretations NEVER receive a machine-supported truth label.
    """
    verify_packet(packet)
    reply = json.loads(raw) if isinstance(raw, str) else raw
    keys = {"packet_id", "observations", "interpretations", "contradictions", "missing_data", "abstention"}
    if not isinstance(reply, dict) or set(reply) != keys or reply["packet_id"] != packet["packet_id"]:
        raise ValueError("Response belongs to another fact packet or has invalid fields")
    if type(reply["abstention"]) is not bool:
        raise ValueError("Invalid abstention")
    for field, maximum in (("observations", 12), ("interpretations", 4), ("contradictions", 6), ("missing_data", 12)):
        if not isinstance(reply[field], list) or len(reply[field]) > maximum:
            raise ValueError("Invalid claim collection")
    if reply["abstention"] and any(reply[f] for f in ("observations", "interpretations", "contradictions")):
        raise ValueError("Abstention cannot contain asserted conclusions")
    facts = {f["id"]: f for f in packet["facts"]}
    claims, seen = [], set()

    def check_shape(item, expected):
        if not isinstance(item, dict) or set(item) != set(expected):
            raise ValueError("Invalid claim fields")
        key = item["id"]
        if not isinstance(key, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", key) or key in seen:
            raise ValueError("Invalid or duplicate claim ID")
        seen.add(key)

    def usable(ref):
        fact = facts.get(ref) if isinstance(ref, str) else None
        return (
            fact is not None
            and fact["quality"] == "ok"
            and fact["observed_at"] is not None
            and 0
            <= packet["captured_at"] - fact["observed_at"]
            <= (900 if fact["role"] == "history" else 30 if fact["role"] == "activity" else 15)
        )

    def emit(item, section, status, reason, refs):
        signature = {k: v for k, v in item.items() if k not in {"id", "text"}}
        if section == "observation" and status == "supported":
            signature["value"] = facts[item["fact_id"]]["value"]
        claims.append(
            dict(
                id=item["id"],
                section=section,
                status=status,
                reason=reason,
                references=refs,
                signature=checksum(dict(section=section, claim=signature)),
                assertion=item,
            )
        )

    for item in reply["observations"]:
        check_shape(item, ("id", "fact_id", "value"))
        ref = item["fact_id"]
        if not isinstance(ref, str) or isinstance(item["value"], (list, dict)):
            raise ValueError("Invalid observation")
        canonical(item["value"])
        if not usable(ref):
            emit(item, "observation", "unverifiable", "unknown_or_stale_fact", [ref])
        else:
            supplied, measured = item["value"], facts[ref]["value"]
            equal = (
                supplied == measured
                if type(supplied) in (int, float) and type(measured) in (int, float)
                else canonical(supplied) == canonical(measured)
            )
            emit(
                item,
                "observation",
                "supported" if equal else "contradicted",
                "exact_value" if equal else "value_mismatch",
                [ref],
            )
    for item in reply["contradictions"]:
        check_shape(item, ("id", "left", "right", "relation"))
        left, right = item["left"], item["right"]
        if (
            not isinstance(left, str)
            or not isinstance(right, str)
            or item["relation"] not in {"equal", "greater", "less"}
        ):
            raise ValueError("Invalid comparison")
        refs = [left, right]
        if not all(usable(r) for r in refs):
            emit(item, "comparison", "unverifiable", "unknown_or_stale_fact", refs)
            continue
        a, b = facts[left], facts[right]
        # Cross-role/cross-window arithmetic does not establish a meaningful relation.
        if (
            a["unit"] != b["unit"]
            or a["role"] != b["role"]
            or a.get("window") != b.get("window")
            or left == right
            or a.get("measurement") != b.get("measurement")
            or type(a["value"]) not in (int, float)
            or type(b["value"]) not in (int, float)
            or abs(a["observed_at"] - b["observed_at"]) > 1
        ):
            emit(item, "comparison", "unverifiable", "incompatible_facts", refs)
            continue
        result = {
            "equal": a["value"] == b["value"],
            "greater": a["value"] > b["value"],
            "less": a["value"] < b["value"],
        }[item["relation"]]
        emit(item, "comparison", "supported" if result else "contradicted", "value_relation", refs)
    for item in reply["interpretations"]:
        check_shape(item, ("id", "kind", "evidence", "text"))
        if item["kind"] not in {
            "possible_absorption",
            "possible_replenishment",
            "flow_price_mismatch",
            "context_link",
            "other",
        }:
            raise ValueError("Invalid interpretation")
        refs, text = item["evidence"], item["text"]
        if (
            not isinstance(refs, list)
            or not 1 <= len(refs) <= 6
            or not all(isinstance(r, str) for r in refs)
            or not isinstance(text, str)
            or not 1 <= len(text.strip()) <= 400
            or any(c.isdigit() for c in text)
            or re.search(r"\b(buy|sell|long|short|tp|sl)\b|покупай|продавай|открыть позици|вход в позици", text, re.I)
        ):
            raise ValueError("Interpretation contains invalid text, numbers or a trading command")
        enough = all(usable(r) for r in refs)
        roles = {facts[r]["role"] for r in refs if r in facts}
        if item["kind"] in {"possible_absorption", "flow_price_mismatch"}:
            windows = {facts[r].get("window") for r in refs if r in facts and facts[r].get("window")}
            enough = enough and {"flow", "price_action"} <= roles and len(windows) == 1
            enough = enough and any(
                r.endswith(".flow_price_conflict") and facts[r]["value"] is True for r in refs if r in facts
            )
        elif item["kind"] in {"possible_replenishment", "context_link"}:
            # Current packets cannot establish execution at a zone or relations between instruments.
            enough = False
        emit(
            item,
            "interpretation",
            "hypothesis" if enough else "unverifiable",
            "interpretation_not_proven" if enough else "insufficient_hypothesis_evidence",
            refs,
        )
    for index, ref in enumerate(reply["missing_data"]):
        if not isinstance(ref, str) or ref in reply["missing_data"][:index]:
            raise ValueError("Invalid missing-data reference")
        key = f"missing_{index}"
        if key in seen:
            raise ValueError("Duplicate claim ID")
        seen.add(key)
        if ref not in facts:
            status, reason = "unverifiable", "unknown_reference"
        elif usable(ref):
            status, reason = "contradicted", "fact_is_available"
        else:
            status, reason = "supported", "limitation_correctly_identified"
        emit(dict(id=key, fact_id=ref), "missing_data", status, reason, [ref])
    if not claims and not reply["abstention"]:
        raise ValueError("Empty response must explicitly abstain")
    referenced = {r for c in claims for r in c["references"] if r in facts}
    groups = sorted({facts[r]["source"] for r in referenced})
    return dict(
        validator_version=VALIDATOR_VERSION,
        packet_id=packet["packet_id"],
        claims=claims,
        abstention=reply["abstention"],
        referenced_facts=len(referenced),
        total_facts=len(facts),
        sources=groups,
        independence="shared_input_facts",
        validated_at=now,
        assessment_basis="frozen_packet",
        expired_at_completion=not 0 <= now - packet["captured_at"] <= 15,
    )


class FactReviewer:
    """Reuse configured provider transport without reusing its direction prompt."""

    def __init__(self, analyst):
        self.analyst = analyst

    async def review(self, packet):
        analyst = self.analyst
        if not analyst.key:
            return dict(status="unconfigured")
        url, request, headers = analyst.request(canonical(packet), schema(packet))
        if analyst.provider == "openrouter":
            request["messages"][0]["content"] = PROMPT
            request["max_tokens"] = 2200
            request["response_format"]["json_schema"]["name"] = "fact_review"
        else:
            request["systemInstruction"] = {"parts": [{"text": PROMPT}]}
        try:
            async with analyst.session.post(
                url,
                json=request,
                headers=headers,
                proxy=analyst.proxy,
                timeout=aiohttp.ClientTimeout(
                    total=getattr(analyst, "timeout_seconds", 60),
                    connect=10,
                    sock_read=getattr(analyst, "timeout_seconds", 50),
                ),
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    return dict(
                        status=f"http_{response.status}",
                        retry_after=int(response.headers.get("Retry-After", "3600"))
                        if response.status == 429
                        else None,
                    )
                payload = await response.json()
            raw, model = analyst.unpack(payload)
            if raw is not None and (not isinstance(raw, str) or len(raw) > 32000):
                return dict(status="invalid_response")
            reported = payload.get("usage")
            usage = {
                key: value
                for key, value in (reported if isinstance(reported, dict) else {}).items()
                if key in {"prompt_tokens", "completion_tokens", "total_tokens"} and type(value) is int and value >= 0
            }
            cost = (reported or {}).get("cost") if isinstance(reported, dict) else None
            if type(cost) in (int, float) and math.isfinite(cost) and cost >= 0:
                usage["cost"] = cost
            return dict(status="ok" if raw is not None else "incomplete", raw=raw, model=model, usage=usage)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            return dict(
                status="network_error",
                reason="timeout" if isinstance(exc, asyncio.TimeoutError) else type(exc).__name__,
            )
        except (ValueError, KeyError, IndexError, TypeError):
            return dict(status="invalid_response")
