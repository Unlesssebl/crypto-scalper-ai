"""Offline, causal replay of local LLM reviews and market meta-analysis.

Reads a frozen copy of the screener journal. Never writes to the live journal,
changes production policy, sends Telegram, or supplies forward prices to models.
All API attempts and final responses are recorded; there are no silent retries.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import sqlite3
import statistics
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from dotenv import dotenv_values

WEAK_MODELS = [
    "~z-ai/glm-flash-latest", "nvidia/nemotron-3.5-lightning:free",
    "google/gemma-4-26b-a4b-it", "nvidia/nemotron-3.5-super-vl:free",
]
METRICS = ("price", "ofi", "cvd", "activity_ratio", "trade_count", "trade_rate", "spread", "imbalance",
           "first_trade_price", "last_trade_price", "natr", "range_ratio")
FLAGS = ["flow_price_conflict", "flow_book_conflict", "cross_window_conflict", "late_move",
         "weak_data", "history_unavailable", "unstable_flow"]
LOCAL_ENUMS = {
    "flow_price": ["aligned", "opposed", "stalled", "insufficient"],
    "flow_book": ["supports", "opposes", "balanced", "unavailable"],
    "cross_window": ["aligned", "opposed", "neutral", "unavailable"],
    "history": ["same_side", "reversed", "mixed", "unknown"],
}
LOCAL_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {
        **{k: {"type": "string", "enum": v} for k, v in LOCAL_ENUMS.items()},
        "risk_flags": {"type": "array", "items": {"type": "string", "enum": FLAGS}},
        "evidence": {"type": "array", "maxItems": 6, "items": {"type": "string"}},
        "summary": {"type": "string", "maxLength": 400},
        "invalidation": {"type": "string", "maxLength": 200},
    },
}
LOCAL_SCHEMA["required"] = list(LOCAL_SCHEMA["properties"])
LOCAL_PROMPT = """Ты локальный аналитик эпизода, а не генератор торговых команд.
Дай только JSON по приложенной схеме. Не прогнозируй цену, не давай сигнал BUY/SELL.
Используй только приложенные данные. summary и invalidation — по-русски, без чисел и уровней.
Сформулируй наблюдаемое противоречие либо объясни согласованность; не объявляй поглощение,
манипуляцию или намерения участников установленной причиной.
Для проверяемости используй следующие точные правила классификации:
В основном окне OFI с quality=ok >0.15 означает покупки, <-0.15 продажи, иначе поток нейтрален.
flow_price: insufficient при невалидных OFI, spread, first_trade_price, last_trade_price или natr,
либо нейтральном потоке. Невалидность — quality!=ok либо value=null; natr должен быть положителен.
Сравни last-first со знаком потока. Порог значимости max(2*spread, first*natr/100*0.1).
Если модуль изменения не больше порога — stalled, иначе aligned либо opposed.
flow_book: unavailable при невалидном OFI/imbalance либо нейтральном потоке;
при модуле imbalance <0.25 — balanced; иначе supports/opposes относительно потока.
cross_window: unavailable, если OFI основного или другого окна невалиден или основной нейтрален;
если другое окно нейтрально — neutral; иначе aligned/opposed. Статус прогрева другого окна
НЕ относится к метрикам основного окна. Данные со статусом warming_up нельзя использовать.
history: unknown без хотя бы двух пригодных прошлых записей (не считая current) с направленным OFI.
Если направления прошлых записей различаются — mixed. Иначе same_side при совпадении с current,
reversed при противоположном текущем потоке. При нейтральном current — unknown.
История содержит редкие события, а не непрерывные тики: same_side не доказывает непрерывную устойчивость.
risk_flags: flow_price_conflict при opposed ИЛИ stalled; flow_book_conflict при opposes;
cross_window_conflict при opposed; late_move только при distance_natr>=2;
weak_data при flow_price=insufficient; history_unavailable при history=unknown;
unstable_flow при history=mixed либо reversed. Других флагов не добавляй.
evidence — только реальные пути пригодных метрик: current.metrics.ofi, current.metrics.imbalance,
context.metrics.ofi или history.0.metrics.ofi и т.п.; не ссылайся на метрики с quality!=ok.
Объясни, какие наблюдения способны изменить трактовку. Не пересказывай все метрики подряд.
"""
META_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {
        "scope": {"type": "string", "enum": ["market", "partial_market"]},
        "breadth": {"type": "string", "enum": ["buyers", "sellers", "mixed", "unavailable"]},
        "conflict_symbols": {"type": "array", "items": {"type": "string"}},
        "unreliable_review_ids": {"type": "array", "items": {"type": "string"}},
        "summary": {"type": "string", "maxLength": 600},
        "alternative": {"type": "string", "maxLength": 300},
        "invalidation": {"type": "string", "maxLength": 240},
        "evidence": {"type": "array", "items": {"type": "string"}},
    },
}
META_SCHEMA["required"] = list(META_SCHEMA["properties"])
META_PROMPT = """Ты метааналитик рыночной обстановки, а не торговый агент.
Верни только JSON по схеме. summary, alternative, invalidation — по-русски, без выдуманных чисел и уровней.
Сопоставь эпизоды, опиши общий фон, альтернативу и условия потери актуальности трактовки.
Первичные данные authoritative_episodes имеют приоритет над производными local_reviews.
Ошибки дешёвых моделей нельзя наследовать. Будущих цен здесь нет. Не изображай независимый
консенсус по одинаковым источникам. Не утверждай причинность, намерения участников или прибыльность.
scope=market только при coverage>=0.8, иначе partial_market.
breadth: учитывай только эпизоды с flow_price != insufficient. Если их нет — unavailable.
Если доля покупательских OFI среди них >=0.7 — buyers; если доля продающих >=0.7 — sellers;
иначе mixed. Это описание выборки, не прогноз всего рынка.
conflict_symbols: точный набор символов, где flow_price=opposed/stalled ИЛИ flow_book=opposes
ИЛИ cross_window=opposed, согласно определениям в local_task_rules.
unreliable_review_ids: точные id тех local_reviews, чьи flow_price/flow_book/cross_window/history
либо risk_flags противоречат соответствующим первичным данным; недоступный ответ модели не считать выводом.
Не включай id надёжных разборов. evidence — существующие id authoritative_episodes.
Отличай недостающий контекст от нейтрального рынка. Укажи ограничения покрытия.
"""


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def value(snap, name):
    item = snap.get("metrics", {}).get(name, {})
    return item.get("value") if item.get("quality") == "ok" else None


def side(snap):
    ofi = value(snap, "ofi")
    return 1 if ofi is not None and ofi > 0.15 else -1 if ofi is not None and ofi < -0.15 else 0


def reference(packet):
    current, other = packet["current"], packet.get("context") or {}
    sign = side(current)
    start, end, spread, natr = (value(current, k) for k in
                                ("first_trade_price", "last_trade_price", "spread", "natr"))
    price = "insufficient"
    if sign and all(v is not None for v in (start, end, spread, natr)) and start > 0 and natr > 0:
        delta = (end - start) * sign
        threshold = max(2 * spread, start * natr / 100 * 0.1)
        price = "stalled" if abs(delta) <= threshold else "aligned" if delta > 0 else "opposed"
    imbalance = value(current, "imbalance")
    book = "unavailable"
    if sign and imbalance is not None:
        book = "balanced" if abs(imbalance) < 0.25 else "supports" if sign * imbalance > 0 else "opposes"
    cross = "unavailable"
    if sign and value(other, "ofi") is not None:
        cross = "neutral" if not side(other) else "aligned" if side(other) == sign else "opposed"
    previous = [side(s) for s in packet.get("history", []) if side(s)]
    history = "unknown"
    if sign and len(previous) >= 2:
        history = "mixed" if len(set(previous)) > 1 else "same_side" if previous[0] == sign else "reversed"
    flags = []
    for flag, active in (
        ("flow_price_conflict", price in {"opposed", "stalled"}),
        ("flow_book_conflict", book == "opposes"), ("cross_window_conflict", cross == "opposed"),
        ("late_move", (current.get("movement", {}).get("distance_natr") or 0) >= 2),
        ("weak_data", price == "insufficient"), ("history_unavailable", history == "unknown"),
        ("unstable_flow", history in {"mixed", "reversed"}),
    ):
        if active:
            flags.append(flag)
    return dict(flow_price=price, flow_book=book, cross_window=cross, history=history, risk_flags=flags)


def compact(snapshot):
    # Only public market observations, never owner/configuration fields or model answers.
    result = {k: snapshot[k] for k in ("symbol", "window", "captured_at")}
    result["metrics"] = {k: {f: v.get(f) for f in ("value", "quality", "updated_at")}
                         for k, v in snapshot["metrics"].items() if k in METRICS}
    movement = snapshot.get("movement_context", {})
    result["movement"] = {k: movement[k] for k in ("distance_natr", "retracement_natr", "age_seconds")
                          if k in movement}
    return result


def history_for(db, symbol, at):
    rows = db.execute("""SELECT snapshot FROM finding_candidates
        WHERE symbol=? AND window='1m' AND detected_at>=? AND detected_at<?
        ORDER BY detected_at DESC LIMIT 4""", (symbol, at - 120, at)).fetchall()
    return [compact(json.loads(row[0])) for row in reversed(rows)]


def packet_for(db, snapshot):
    at, symbol = snapshot["captured_at"], snapshot["symbol"]
    row = db.execute("""SELECT snapshot FROM finding_candidates WHERE symbol=? AND window='5m'
        AND detected_at<=? AND detected_at>=? ORDER BY detected_at DESC LIMIT 1""",
                     (symbol, at, at - 60)).fetchone()
    context = compact(json.loads(row[0])) if row else None
    return {"current": compact(snapshot), "context": context, "history": history_for(db, symbol, at)}


def synthetic_cases():
    def make(at, **updates):
        vals = dict(price=100, ofi=0.8, cvd=1000, activity_ratio=3, trade_count=100, trade_rate=4,
                    spread=0.01, imbalance=0.5, first_trade_price=100, last_trade_price=100.1,
                    natr=0.2, range_ratio=1.5)
        vals.update(updates)
        return dict(symbol="CONTROL", window="1m", captured_at=at, movement={},
                    metrics={k: dict(value=v, quality="ok", updated_at=at) for k, v in vals.items()})
    base = dict(current=make(1000), context=make(1000), history=[make(990), make(995)])
    base["context"]["window"] = "5m"
    cases = []
    for name in ("aligned", "opposition", "other_window_warming", "primary_unavailable"):
        p = deepcopy(base)
        if name == "opposition":
            p["current"]["metrics"]["imbalance"]["value"] = -0.8
            p["current"]["metrics"]["last_trade_price"]["value"] = 99.9
            p["context"]["metrics"]["ofi"]["value"] = -0.8
            p["history"][0]["metrics"]["ofi"]["value"] = -0.8
        if name == "other_window_warming":
            p["context"]["metrics"]["ofi"]["quality"] = "warming_up"
        if name == "primary_unavailable":
            p["current"]["metrics"]["ofi"].update(value=None, quality="warming_up")
        cases.append(dict(id="control_" + name, origin="controlled", packet=p, observations=[]))
    return cases


def build_dataset(db, now):
    # A fixed past cutoff permits forward checks without leaking any outcome into prompts.
    cutoff = min(now - 1000, db.execute("SELECT MAX(detected_at) FROM finding_candidates").fetchone()[0])
    cases, bundles = [], []
    used = set()
    for index, as_of in enumerate((cutoff - 240, cutoff - 120, cutoff)):
        rows = db.execute("""SELECT id,symbol,snapshot FROM finding_candidates WHERE window='1m'
            AND detected_at BETWEEN ? AND ? ORDER BY detected_at DESC,id""", (as_of - 120, as_of)).fetchall()
        latest = {}
        for row in rows:
            latest.setdefault(row["symbol"], row)
        available = []
        for row in latest.values():
            snapshot = json.loads(row["snapshot"])
            packet = packet_for(db, snapshot)
            ref = reference(packet)
            if abs(value(packet["current"], "ofi") or 0) < 0.3 or len(packet["history"]) < 2:
                continue
            if (row["symbol"], snapshot["captured_at"]) in used:
                continue
            conflict = any(k in ref["risk_flags"] for k in
                           ("flow_price_conflict", "flow_book_conflict", "cross_window_conflict"))
            available.append((conflict, row, packet))
        chosen = []
        for conflict in (False, True):
            match = next((r for r in available if r[0] == conflict and r not in chosen), None)
            if match:
                chosen.append(match)
        for item in available:
            if len(chosen) >= 2:
                break
            if item not in chosen:
                chosen.append(item)
        if len(chosen) != 2:
            raise RuntimeError("Insufficient causal real episodes for three paired bundles")
        ids = []
        for _, row, packet in chosen:
            cid = f"real_{index}_{row['symbol']}"
            used.add((row["symbol"], packet["current"]["captured_at"]))
            observations = [dict(r) for r in db.execute(
                "SELECT minutes,price,status FROM candidate_observations WHERE event_id=?", (row["id"],),
            )]
            cases.append(dict(id=cid, origin="real", source_id=row["id"], packet=packet, observations=observations))
            ids.append(cid)
        bundles.append(dict(id=f"market_{index}", as_of=as_of, case_ids=ids,
                            tracked_symbols=20, coverage=len(ids) / 20))
    return dict(version=1, built_at=now, cutoff=cutoff, cases=cases + synthetic_cases(), bundles=bundles)


def packets(case, arm):
    p = deepcopy(case["packet"])
    if arm == "snapshot":
        p["history"] = []
    return p


def validate_structure(reply, schema):
    if not isinstance(reply, dict) or set(reply) != set(schema["required"]):
        return False
    for key, spec in schema["properties"].items():
        v = reply[key]
        if spec["type"] == "string":
            if not isinstance(v, str) or ("enum" in spec and v not in spec["enum"]):
                return False
            if len(v) > spec.get("maxLength", 100000):
                return False
        elif spec["type"] == "array":
            if not isinstance(v, list) or len(v) > spec.get("maxItems", 100000):
                return False
            if any(not isinstance(item, str) or ("enum" in spec["items"] and item not in spec["items"]["enum"])
                   for item in v):
                return False
    return True


def evidence_paths(packet):
    allowed = set()
    for prefix, snap in [("current", packet["current"]), ("context", packet.get("context") or {}),
                         *[(f"history.{i}", s) for i, s in enumerate(packet.get("history", []))]]:
        for name, item in snap.get("metrics", {}).items():
            if item.get("quality") == "ok" and item.get("value") is not None:
                allowed.add(f"{prefix}.metrics.{name}")
    return allowed


def score_local(reply, packet):
    if not validate_structure(reply, LOCAL_SCHEMA):
        return {"valid": False}
    ref = reference(packet)
    matches = {k: reply[k] == ref[k] for k in LOCAL_ENUMS}
    return dict(valid=True, fields=matches, core_accuracy=sum(matches[k] for k in
                ("flow_price", "flow_book", "cross_window")) / 3,
                flags_exact=set(reply["risk_flags"]) == set(ref["risk_flags"]),
                grounded=bool(reply["evidence"]) and set(reply["evidence"]) <= evidence_paths(packet),
                reference=ref)


def meta_reference(bundle, cases, reviews):
    refs = {cid: reference(cases[cid]["packet"]) for cid in bundle["case_ids"]}
    eligible = [side(cases[cid]["packet"]["current"]) for cid in bundle["case_ids"]
                if refs[cid]["flow_price"] != "insufficient"]
    breadth = "unavailable"
    if eligible:
        breadth = ("buyers" if eligible.count(1) / len(eligible) >= 0.7 else
                   "sellers" if eligible.count(-1) / len(eligible) >= 0.7 else "mixed")
    conflicts = [cases[cid]["packet"]["current"]["symbol"] for cid, r in refs.items()
                 if set(r["risk_flags"]) & {"flow_price_conflict", "flow_book_conflict", "cross_window_conflict"}]
    unreliable = []
    for review in reviews:
        ref = refs[review["case_id"]]
        r = review["assessment"]
        if (any(r.get(k) != ref[k] for k in LOCAL_ENUMS)
                or set(r.get("risk_flags", [])) != set(ref["risk_flags"])):
            unreliable.append(review["id"])
    return dict(scope="market" if bundle["coverage"] >= 0.8 else "partial_market", breadth=breadth,
                conflict_symbols=sorted(set(conflicts)), unreliable_review_ids=sorted(unreliable))


def score_meta(reply, bundle, cases, reviews):
    if not validate_structure(reply, META_SCHEMA):
        return {"valid": False}
    ref = meta_reference(bundle, cases, reviews)
    matches = {k: (set(reply[k]) == set(ref[k]) if isinstance(ref[k], list) else reply[k] == ref[k]) for k in ref}
    return dict(valid=True, fields=matches, accuracy=sum(matches.values()) / len(matches), reference=ref,
                grounded=bool(reply["evidence"]) and set(reply["evidence"]) <= set(bundle["case_ids"]))


class Runner:
    def __init__(self, output, catalog, cfg):
        self.output, self.catalog, self.cfg = output, catalog, cfg
        self.limit = asyncio.Semaphore(4)
        self.free_lock = asyncio.Lock()
        self.last_free = 0
        self.records = []

    async def call(self, session, *, task_id, model, system, data, schema):
        async with self.limit:
            if model.endswith(":free"):
                async with self.free_lock:
                    await asyncio.sleep(max(0, 3.2 - (time.monotonic() - self.last_free)))
                    self.last_free = time.monotonic()
            start = time.perf_counter()
            supported = self.catalog.get(model, {}).get("supported_parameters", [])
            body = dict(model=model, temperature=0, max_tokens=1400, reasoning={"enabled": False},
                        messages=[dict(role="system", content=system + "\nJSON Schema:\n" + json.dumps(schema)),
                                  dict(role="user", content=json.dumps(data, ensure_ascii=False))])
            if "structured_outputs" in supported:
                body["response_format"] = dict(type="json_schema", json_schema=dict(
                    name="local_review" if schema is LOCAL_SCHEMA else "market_review", strict=True, schema=schema))
                body["provider"] = {"require_parameters": True}
            record = dict(task_id=task_id, requested_model=model, request=body,
                          structured_output="response_format" in body, started_at=time.time())
            try:
                async with session.post(
                    "https://openrouter.ai/api/v1/chat/completions", json=body,
                    headers={"Authorization": "Bearer " + self.cfg["OPENROUTER_API_KEY"]},
                    proxy=self.cfg.get("OPENROUTER_PROXY") or None,
                    timeout=aiohttp.ClientTimeout(total=60), allow_redirects=False,
                ) as response:
                    payload = await response.json()
                    choice = (payload.get("choices") or [{}])[0]
                    raw = choice.get("message", {}).get("content")
                    record.update(http=response.status, resolved_model=payload.get("model"),
                                  provider=payload.get("provider"), finish=choice.get("finish_reason"),
                                  content=raw, usage=payload.get("usage"),
                                  error=payload.get("error", {}).get("message"))
                    if response.status == 200 and choice.get("finish_reason") == "stop" and raw:
                        try:
                            record["reply"] = json.loads(raw)
                        except (ValueError, TypeError):
                            record["error"] = "invalid_json"
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                record["error"] = type(exc).__name__
            record["latency_s"] = round(time.perf_counter() - start, 3)
            self.records.append(record)
            with (self.output / "attempts.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
            print(json.dumps({k: record.get(k) for k in ("task_id", "requested_model", "http", "latency_s", "error")}),
                  flush=True)
            return record


async def benchmark(output, dataset, catalog, cfg, strong):
    runner = Runner(output, catalog, cfg)
    results = []
    tasks = [(m, c, arm) for c in dataset["cases"] for m in WEAK_MODELS for arm in ("snapshot", "episode")]
    random.Random(20261105).shuffle(tasks)
    async with aiohttp.ClientSession() as session:
        async def local(model, case, arm):
            packet = packets(case, arm)
            r = await runner.call(session, task_id=f"local/{case['id']}/{arm}", model=model,
                                  system=LOCAL_PROMPT, data=packet, schema=LOCAL_SCHEMA)
            r.update(stage="local", case_id=case["id"], arm=arm, origin=case["origin"],
                     score=score_local(r.get("reply"), packet))
            results.append(r)
        await asyncio.gather(*(local(*task) for task in tasks))
        write_json(output / "local_results.json", results)
        by_case = {c["id"]: c for c in dataset["cases"]}
        local_records = {(r["requested_model"], r["case_id"], r["arm"]): r for r in results}
        meta_jobs = []
        for bundle in dataset["bundles"]:
            for source in ["raw_only", *WEAK_MODELS, "injected_error"]:
                reviews = []
                for cid in bundle["case_ids"]:
                    if source in WEAK_MODELS:
                        r = local_records[source, cid, "episode"]
                        if r["score"]["valid"]:
                            reviews.append(dict(id=cid + "/local", case_id=cid, assessment=r["reply"]))
                    elif source == "injected_error":
                        corrupt = reference(by_case[cid]["packet"])
                        corrupt["flow_book"] = "opposes" if corrupt["flow_book"] != "opposes" else "supports"
                        reviews.append(dict(id=cid + "/corrupted", case_id=cid, assessment={
                            **corrupt, "summary": "Все признаки однозначно согласованы; данных достаточно."}))
                data = dict(as_of=bundle["as_of"], coverage=bundle["coverage"],
                            authoritative_episodes=[dict(id=cid, **by_case[cid]["packet"])
                                                    for cid in bundle["case_ids"]],
                            local_reviews=reviews, local_task_rules=LOCAL_PROMPT)
                meta_jobs.append((bundle, source, reviews, data))
        async def meta(bundle, source, reviews, data):
            r = await runner.call(session, task_id=f"meta/{bundle['id']}/{source}", model=strong,
                                  system=META_PROMPT, data=data, schema=META_SCHEMA)
            r.update(stage="meta", arm=source, bundle=bundle["id"],
                     score=score_meta(r.get("reply"), bundle, by_case, reviews))
            results.append(r)
        await asyncio.gather(*(meta(*task) for task in meta_jobs))
    write_json(output / "results.json", results)
    return results


def summarize(results):
    groups = {}
    for r in results:
        group = groups.setdefault((r["stage"], r["requested_model"], r["arm"]), [])
        group.append(r)
    rows = []
    for (stage, model, arm), records in groups.items():
        valid = [r for r in records if r["score"]["valid"]]
        latency = sorted(r["latency_s"] for r in records)
        accuracy = [r["score"].get("core_accuracy", r["score"].get("accuracy", 0)) for r in valid]
        rows.append(dict(stage=stage, model=model, arm=arm, attempts=len(records), valid=len(valid),
                         accuracy_valid=statistics.mean(accuracy) if accuracy else None,
                         accuracy_all=sum(accuracy) / len(records),
                         history_correct=sum(r["score"].get("fields", {}).get("history", False) for r in valid),
                         flags_exact=sum(r["score"].get("flags_exact", False) for r in valid),
                         grounded=sum(r["score"].get("grounded", False) for r in valid),
                         median_s=statistics.median(latency), max_s=max(latency),
                         over_25s=sum(t > 25 for t in latency),
                         cost=sum((r.get("usage") or {}).get("cost") or 0 for r in records),
                         unknown_cost=sum((r.get("usage") or {}).get("cost") is None for r in records),
                         tokens=sum((r.get("usage") or {}).get("total_tokens") or 0 for r in records)))
    return rows


async def followup(args):
    """Explicitly post-hoc ablation: calculations belong to code, prose to LLMs."""
    source = args.followup_from
    args.output.mkdir(parents=True, exist_ok=False)
    dataset = json.loads((source / "dataset.json").read_text(encoding="utf-8"))
    catalog = json.loads((source / "model_catalog.json").read_text(encoding="utf-8"))
    cfg = {**dotenv_values(args.env), **{k: os.environ[k] for k in
           ("OPENROUTER_API_KEY", "OPENROUTER_PROXY", "OPENROUTER_MODEL") if os.getenv(k)}}
    selected = [c for c in dataset["cases"] if c["id"] in {"control_aligned", "control_opposition"}]
    write_json(args.output / "dataset.json", {"source_sha256": dataset["sha256"],
                                            "case_ids": [c["id"] for c in selected]})
    (args.output / "protocol.md").write_text(
        "# Post-hoc responsibility ablation\n\nAfter the primary run found arithmetic/relational errors, "
        "test two fixed controls (aligned, opposition) with deterministic verified_facts provided. "
        "Two cases/model are exploratory, not a held-out generalization test.\n"
        "Meta: same three real bundles, raw-only and injected-errors, with a verified fact table. "
        "No future prices or new market observations. No retries or model substitutions.\n", encoding="utf-8")
    runner = Runner(args.output, catalog, cfg)
    results = []
    async with aiohttp.ClientSession() as session:
        async def local(model, case):
            packet = deepcopy(case["packet"])
            packet["verified_facts"] = reference(packet)
            system = LOCAL_PROMPT + """\nИзменение границы ответственности: verified_facts уже рассчитаны
детерминированным кодом и проверены. Перенеси эти классификации и risk_flags в ответ без пересчёта.
Твоя задача — объяснение и условие пересмотра. summary — максимум два коротких предложения;
invalidation — одно короткое предложение. Факты не меняй. Не добавляй уверенности из совпадения окон."""
            r = await runner.call(session, task_id=f"prepared/{case['id']}", model=model,
                                  system=system, data=packet, schema=LOCAL_SCHEMA)
            r.update(stage="local", case_id=case["id"], arm="verified_facts", origin="controlled",
                     score=score_local(r.get("reply"), packet))
            results.append(r)
        await asyncio.gather(*(local(model, case) for model in WEAK_MODELS for case in selected))
        cases = {c["id"]: c for c in dataset["cases"]}
        async def meta(bundle, corrupted):
            reviews = []
            if corrupted:
                for cid in bundle["case_ids"]:
                    ref = reference(cases[cid]["packet"])
                    ref["flow_book"] = "supports" if ref["flow_book"] == "opposes" else "opposes"
                    reviews.append(dict(id=cid + "/corrupted", case_id=cid, assessment={
                        **ref, "summary": "Все признаки однозначно согласованы; данных достаточно."}))
            episodes = [dict(id=cid, **cases[cid]["packet"], verified_facts=reference(cases[cid]["packet"]))
                        for cid in bundle["case_ids"]]
            data = dict(as_of=bundle["as_of"], coverage=bundle["coverage"], authoritative_episodes=episodes,
                        local_reviews=reviews, local_task_rules=LOCAL_PROMPT)
            system = META_PROMPT + """\nВ каждом authoritative_episode есть verified_facts,
рассчитанные кодом и проверенные: это эталонные факты, не пересчитывай их заново.
Для breadth используй flow_price из verified_facts и знак OFI; для conflict_symbols —
risk_flags из verified_facts; для проверки local_reviews сравнивай их поля с verified_facts.
Сосредоточься на сопоставлении и ограничениях интерпретации."""
            arm = "verified_injected" if corrupted else "verified_raw"
            r = await runner.call(session, task_id=f"prepared_meta/{bundle['id']}/{arm}",
                                  model=cfg["OPENROUTER_MODEL"], system=system, data=data, schema=META_SCHEMA)
            r.update(stage="meta", bundle=bundle["id"], arm=arm,
                     score=score_meta(r.get("reply"), bundle, cases, reviews))
            results.append(r)
        await asyncio.gather(*(meta(b, corrupted) for b in dataset["bundles"] for corrupted in (False, True)))
    write_json(args.output / "results.json", results)
    write_json(args.output / "summary.json", summarize(results))
    print("FOLLOWUP:", str(args.output.resolve()))


async def live_meta(args):
    """Read-only full-universe context capture; inference still runs offline."""
    args.output.mkdir(parents=True, exist_ok=False)
    catalog = json.loads((args.followup_from / "model_catalog.json").read_text(encoding="utf-8"))
    cfg = {**dotenv_values(args.env), **{k: os.environ[k] for k in
           ("OPENROUTER_API_KEY", "OPENROUTER_PROXY", "OPENROUTER_MODEL") if os.getenv(k)}}
    frames = []
    (args.output / "protocol.md").write_text(
        "# Full Top-20 context check\n\nRead-only capture of three successive 1m/5m frames, six seconds apart. "
        "Compare raw metrics vs verified facts at the last two cutoffs. No local model summaries. "
        "This checks meta context coverage, not forecast accuracy or trading value.\n", encoding="utf-8")
    async with aiohttp.ClientSession() as session:
        async def fetch(window):
            async with session.get("http://127.0.0.1:8421/v1/scalper", params={"window": window},
                                   timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
                result = {}
                for row in data["rows"]:
                    snap = deepcopy(row["snapshot"])
                    phase = row.get("movement_phase") or {}
                    snap["movement_context"] = {k: phase[k] for k in
                                                ("distance_natr", "retracement_natr", "age_seconds") if k in phase}
                    result[snap["symbol"]] = compact(snap)
                return result
        for _ in range(3):
            if frames:
                await asyncio.sleep(6)
            one, five = await asyncio.gather(fetch("1m"), fetch("5m"))
            frames.append({"one": one, "five": five})
        write_json(args.output / "frames.json", frames)
        cases, bundles = {}, []
        for index in (1, 2):
            frame = frames[index]
            ids = []
            for symbol, current in frame["one"].items():
                cid = f"live_{index}_{symbol}"
                history = [f["one"][symbol] for f in frames[:index] if symbol in f["one"]
                           and f["one"][symbol]["captured_at"] < current["captured_at"]]
                context = frame["five"].get(symbol)
                if context and context["captured_at"] > current["captured_at"]:
                    context = None
                packet = dict(current=current, context=context, history=history)
                cases[cid] = dict(id=cid, origin="live", packet=packet)
                ids.append(cid)
            coverage = sum(reference(cases[cid]["packet"])["flow_price"] != "insufficient" for cid in ids) / 20
            bundles.append(dict(id=f"live_market_{index}", case_ids=ids, coverage=coverage,
                                as_of=max(cases[cid]["packet"]["current"]["captured_at"] for cid in ids)))
        write_json(args.output / "dataset.json", dict(cases=cases, bundles=bundles))
        runner = Runner(args.output, catalog, cfg)
        results = []
        async def analyze(bundle, verified):
            episodes = [dict(id=cid, **cases[cid]["packet"]) for cid in bundle["case_ids"]]
            if verified:
                for ep in episodes:
                    ep["verified_facts"] = reference(ep)
            data = dict(as_of=bundle["as_of"], coverage=bundle["coverage"], authoritative_episodes=episodes,
                        local_reviews=[], local_task_rules=LOCAL_PROMPT)
            system = META_PROMPT
            if verified:
                system += "\nverified_facts рассчитаны кодом. Используй их без пересчёта для классификаций и флагов."
            arm = "verified_full" if verified else "raw_full"
            r = await runner.call(session, task_id=f"full/{bundle['id']}/{arm}",
                                  model=cfg["OPENROUTER_MODEL"], system=system, data=data, schema=META_SCHEMA)
            r.update(stage="meta", arm=arm, bundle=bundle["id"],
                     score=score_meta(r.get("reply"), bundle, cases, []))
            results.append(r)
        await asyncio.gather(*(analyze(b, v) for b in bundles for v in (False, True)))
    write_json(args.output / "results.json", results)
    write_json(args.output / "summary.json", summarize(results))
    print("LIVE META:", str(args.output.resolve()))


async def corrected_controls(args):
    """Preserve the original run, explicitly repair an ambiguous control window label."""
    args.output.mkdir(parents=True, exist_ok=False)
    root = args.followup_from
    catalog = json.loads((root / "model_catalog.json").read_text(encoding="utf-8"))
    cfg = {**dotenv_values(args.env), **{k: os.environ[k] for k in
           ("OPENROUTER_API_KEY", "OPENROUTER_PROXY", "OPENROUTER_MODEL") if os.getenv(k)}}
    dataset = json.loads((root / "dataset.json").read_text(encoding="utf-8"))
    dataset["cases"] = [c for c in dataset["cases"] if c["origin"] == "real"] + synthetic_cases()
    write_json(args.output / "dataset.json", dataset)
    (args.output / "protocol.md").write_text(
        "# Corrected controlled fixtures\n\nThe primary run's controlled context object was named context "
        "but its window metadata incorrectly said 1m, matching the main window. "
        "Correct it to 5m, preserve all values, and repeat all four controls in both paired arms/model. "
        "Also repeat verified_facts on aligned/opposition controls. Original attempts remain preserved; "
        "only corrected controls enter the final combined table. Real-case and meta results are unaffected.\n",
        encoding="utf-8")
    runner = Runner(args.output, catalog, cfg)
    results = []
    tasks = [(model, c, arm) for c in dataset["cases"] if c["origin"] == "controlled"
             for model in WEAK_MODELS for arm in ("snapshot", "episode")]
    tasks += [(model, c, "verified_facts") for c in dataset["cases"]
              if c["id"] in {"control_aligned", "control_opposition"} for model in WEAK_MODELS]
    random.Random(20261106).shuffle(tasks)
    async with aiohttp.ClientSession() as session:
        async def one(model, case, arm):
            p = packets(case, arm)
            system = LOCAL_PROMPT
            if arm == "verified_facts":
                p["verified_facts"] = reference(p)
                system += """\nverified_facts рассчитаны кодом и проверены. Перенеси классификации и risk_flags
без пересчёта; объясни смысл двумя короткими предложениями и дай одно короткое условие пересмотра."""
            r = await runner.call(session, task_id=f"corrected/{case['id']}/{arm}", model=model,
                                  system=system, data=p, schema=LOCAL_SCHEMA)
            r.update(stage="local", arm=arm, case_id=case["id"], origin="controlled",
                     score=score_local(r.get("reply"), p))
            results.append(r)
        await asyncio.gather(*(one(*t) for t in tasks))
    write_json(args.output / "results.json", results)
    write_json(args.output / "summary.json", summarize(results))
    print("CORRECTED CONTROLS:", str(args.output.resolve()))


async def aggregate_meta(args):
    """Third responsibility ablation: compute market aggregates and exhaustive lists in code."""
    args.output.mkdir(parents=True, exist_ok=False)
    catalog = json.loads((args.followup_from / "model_catalog.json").read_text(encoding="utf-8"))
    dataset = json.loads((args.aggregate_from / "dataset.json").read_text(encoding="utf-8"))
    cases, bundles = dataset["cases"], dataset["bundles"]
    cfg = {**dotenv_values(args.env), **{k: os.environ[k] for k in
           ("OPENROUTER_API_KEY", "OPENROUTER_PROXY", "OPENROUTER_MODEL") if os.getenv(k)}}
    (args.output / "protocol.md").write_text(
        "# Post-hoc market aggregation ablation\n\nFull-universe raw/verified-episode runs omitted or "
        "invented entries in exhaustive conflict lists. Calculate these lists and breadth upstream, "
        "send compact verified facts and aggregates only, and keep the same two recorded cutoffs. "
        "Two tests are exploratory; no new future observations or retries.\n", encoding="utf-8")
    runner = Runner(args.output, catalog, cfg)
    results = []
    async with aiohttp.ClientSession() as session:
        async def one(bundle):
            facts = meta_reference(bundle, cases, [])
            episodes = [dict(id=cid, symbol=cases[cid]["packet"]["current"]["symbol"],
                             flow_side=side(cases[cid]["packet"]["current"]),
                             verified_facts=reference(cases[cid]["packet"])) for cid in bundle["case_ids"]]
            data = dict(as_of=bundle["as_of"], coverage=bundle["coverage"], computed_market_facts=facts,
                        authoritative_episodes=episodes, local_reviews=[])
            system = META_PROMPT + """\nГраница ответственности: computed_market_facts уже проверены кодом,
включая scope, breadth, полный conflict_symbols и unreliable_review_ids. Перенеси эти поля без изменений.
Не пересчитывай показатели и не составляй списки заново. На уровне эпизодов также даны verified_facts.
Твоя задача — связное сопоставление, осторожная альтернативная трактовка и условие её пересмотра.
Данных о движении внутри диапазона, ликвидности вне spread и причинах действий участников здесь нет.
Не придумывай положения у границ диапазона, тонкую ликвидность или намерения участников."""
            r = await runner.call(session, task_id=f"aggregate/{bundle['id']}", model=cfg["OPENROUTER_MODEL"],
                                  system=system, data=data, schema=META_SCHEMA)
            r.update(stage="meta", arm="computed_market_facts", bundle=bundle["id"],
                     score=score_meta(r.get("reply"), bundle, cases, []))
            results.append(r)
        await asyncio.gather(*(one(b) for b in bundles))
    write_json(args.output / "results.json", results)
    write_json(args.output / "summary.json", summarize(results))
    print("AGGREGATE META:", str(args.output.resolve()))


def report(output, dataset, results):
    rows = summarize(results)
    write_json(output / "summary.json", rows)
    lines = ["# Pilot: three AI layers", "", f"Generated: {datetime.now(timezone.utc).isoformat()}", "",
             "10 matched cases: 6 recorded market episodes + 4 explicitly controlled cases. "
             "Snapshot vs causal episode history. One response per case/arm/model; no retries. "
             "Three meta bundles, each with two episodes (10% of Top-20), deliberately incomplete coverage.", "",
             "These scores measure schema compliance and factual interpretation against explicit deterministic rules. "
             "They are not a prediction benchmark, trading backtest, independent human rating, "
             "or proof of profitability. "
             "The rule baseline calculates the same labels exactly; matching it is not added analytical value.", "",
             "## Local review", "",
             "| Model | Arm | Valid | Core facts, all attempts | Flags exact | History correct | Grounded refs | "
             "Median/max s | >25s | Cost USD |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in sorted((r for r in rows if r["stage"] == "local"), key=lambda r: (r["model"], r["arm"])):
        n = r["attempts"]
        lines.append(f"| {r['model']} | {r['arm']} | {r['valid']}/{n} | {r['accuracy_all']:.1%} | "
                     f"{r['flags_exact']}/{n} | {r['history_correct']}/{n} | {r['grounded']}/{n} | "
                     f"{r['median_s']:.2f}/{r['max_s']:.2f} | {r['over_25s']} | {r['cost']:.6f} |")
    lines += ["", "## Meta-analysis (DeepSeek from environment)", "",
              "| Local input | Valid | Factual checks, all attempts | Grounded refs | Median/max s | Cost USD |",
              "|---|---:|---:|---:|---:|---:|"]
    for r in sorted((r for r in rows if r["stage"] == "meta"), key=lambda r: r["arm"]):
        lines.append(f"| {r['arm']} | {r['valid']}/{r['attempts']} | {r['accuracy_all']:.1%} | "
                     f"{r['grounded']}/{r['attempts']} | {r['median_s']:.2f}/{r['max_s']:.2f} | {r['cost']:.6f} |")
    lines += ["", "## Errors / failed assertions", ""]
    for r in results:
        s = r["score"]
        failed = [k for k, v in s.get("fields", {}).items() if not v]
        if not s["valid"] or failed or s.get("flags_exact") is False or s.get("grounded") is False:
            lines.append(f"- {r['requested_model']} / {r['task_id']}: valid={s['valid']}; "
                         f"failed={failed}; flags={s.get('flags_exact')}; grounded={s.get('grounded')}; "
                         f"HTTP={r.get('http')}; error={r.get('error')}")
    lines += ["", "## Reproducibility and limitations", "",
              "- dataset.json freezes observations and source IDs; journal.sqlite is the consistent source backup.",
              "- attempts.jsonl preserves prompts, final responses, resolved models, timing and reported usage/cost.",
              "- Request bodies contain public market data only. No API keys, environment contents or future prices.",
              "- Derived metrics are graded by independent local code, not by the tested model or an LLM judge.",
              "- Real history is event-sampled; it cannot establish uninterrupted book support or continuous flow.",
              "- Snapshot history=unknown is the correct answer, not a failure to infer invisible history.",
              "- Meta raw-only is the ablation baseline. Injected-error is a controlled robustness test.",
              "- One API sample cannot characterize model variance, tail latency or different market regimes.",
              "- Grounding checks only referenced fields, not every sentence; prose requires review.",
              "- Failures count against all-attempt accuracy; missing billing data are not assumed free.",
              "- Test calls are separate from the production request journal and its hourly budget.", ""]
    (output / "report.md").write_text("\n".join(lines), encoding="utf-8")


def prepare(args):
    args.output.mkdir(parents=True, exist_ok=False)
    cfg = {**dotenv_values(args.env), **{k: os.environ[k] for k in
           ("OPENROUTER_API_KEY", "OPENROUTER_PROXY", "OPENROUTER_MODEL") if os.getenv(k)}}
    if not cfg.get("OPENROUTER_API_KEY") or not cfg.get("OPENROUTER_MODEL"):
        raise RuntimeError("Configured OpenRouter credentials and meta model required")
    source = sqlite3.connect(args.journal.resolve().as_uri() + "?mode=ro", uri=True)
    frozen = sqlite3.connect(args.output / "journal.sqlite")
    source.backup(frozen)
    source.close()
    frozen.row_factory = sqlite3.Row
    dataset = build_dataset(frozen, time.time())
    frozen.close()
    encoded = json.dumps(dataset, ensure_ascii=False, sort_keys=True).encode()
    dataset["sha256"] = hashlib.sha256(encoded).hexdigest()
    write_json(args.output / "dataset.json", dataset)
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    selected = {m["id"]: m for m in catalog["data"] if m["id"] in [*WEAK_MODELS, cfg["OPENROUTER_MODEL"]]}
    write_json(args.output / "model_catalog.json", selected)
    (args.output / "protocol.md").write_text(
        "# Predeclared pilot\n\nH1: four requested small models can produce grounded local reviews.\n"
        "H2: event history improves temporal interpretation without corrupting current-window facts.\n"
        "H3: meta-analysis adds market synthesis and resists errors in local reviews.\n\n"
        "Primary objective metrics: schema valid, three current-fact fields, temporal field, exact risk flags, "
        "reference grounding, latency, actual reported cost. Compare paired arms on identical current data.\n"
        "No forecast/PnL claim; semantic usefulness and causal market explanations are not objectively scored.\n"
        "Meta ablation: raw-only, raw+each weak model, raw+deliberately incorrect review.\n"
        "No production mutations or Telegram. No model substitution or retries.\n", encoding="utf-8")
    return cfg, dataset, selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, default=Path(os.environ.get("LOCALAPPDATA", ".")) /
                        "hyperdata-screener" / "scalper.sqlite")
    parser.add_argument("--env", type=Path, default=Path(".env"))
    parser.add_argument("--catalog", type=Path, default=Path(".runtime/hypothesis-preflight/catalog.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--followup-from", type=Path, help="Explore verified facts on a completed primary run")
    parser.add_argument("--live-meta", action="store_true", help="Read-only full Top-20 capture and meta comparison")
    parser.add_argument("--correct-controls", action="store_true", help="Repeat explicitly corrected controlled cases")
    parser.add_argument("--aggregate-from", type=Path, help="Explore compact market facts on full-universe captures")
    args = parser.parse_args()
    if args.correct_controls:
        if not args.followup_from:
            parser.error("--correct-controls requires --followup-from")
        asyncio.run(corrected_controls(args))
        return
    if args.aggregate_from:
        if not args.followup_from:
            parser.error("--aggregate-from requires --followup-from for the pinned model catalog")
        asyncio.run(aggregate_meta(args))
        return
    if args.live_meta:
        if not args.followup_from:
            parser.error("--live-meta requires --followup-from for the pinned model catalog")
        asyncio.run(live_meta(args))
        return
    if args.followup_from:
        asyncio.run(followup(args))
        return
    cfg, dataset, catalog = prepare(args)
    results = asyncio.run(benchmark(args.output, dataset, catalog, cfg, cfg["OPENROUTER_MODEL"]))
    report(args.output, dataset, results)
    print("REPORT:", str((args.output / "report.md").resolve()))


if __name__ == "__main__":
    main()
