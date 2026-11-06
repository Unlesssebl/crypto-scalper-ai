"""Export a reproducible P4 study through the read-only API, or replay its input."""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict
from pathlib import Path

import aiohttp

from .models import MINUTE, Parameters, digest, latest_rows, research


async def fetch_windows(session, url, start_ms, end_ms):
    rows, cursor = [], start_ms
    while cursor < end_ms:
        async with session.get(url.rstrip("/") + "/windows", params={
            "from_ms": cursor, "to_ms": end_ms, "limit": 10_000,
        }, timeout=aiohttp.ClientTimeout(total=15)) as response:
            response.raise_for_status()
            page = await response.json()
        if page.get("schema_version") != "market-pulse.window.v1" or not isinstance(page.get("windows"), list):
            raise ValueError("unsupported history contract")
        rows.extend(page["windows"])
        next_cursor = page.get("next_from_ms")
        if next_cursor is None:
            break
        if type(next_cursor) is not int or next_cursor <= cursor:
            raise ValueError("history cursor did not advance")
        cursor = next_cursor
    return latest_rows(rows)


def render(report):
    lines = ["# P4: исследование фактической истории", "",
             "Статус: исследовательский режим. Допуск новых live-сигналов: выключен.", "",
             f"Границы UTC (ms): {report['from_ms']}–{report['to_ms']}; разделение: {report['split_ms']}.",
             "Первые 70% времени — база, последние 30% — отложенная проверка.", "",
             "| Источник / показатель | База | Наблюдения / покрытие | Хвост | Проверка |",
             "|---|---|---|---|---|"]
    for item in report["reports"]:
        profile = item["profile"]
        lines.append(f"| {' / '.join(item['cohort'][:2])} / {item['metric']} | {profile['status']} | "
                     f"{profile['samples']} / {profile['coverage']:.1%} | {profile['tail_samples']} | "
                     f"{item['holdout_usable_minutes']}/{item['holdout_expected_minutes']} |")
    lines += ["", "## Частота кандидатов до ограничения сообщений", "",
              "| Источник / показатель | Метод | Кандидаты | Эпизоды | Подавлено | Эпизодов/сутки |",
              "|---|---|---|---|---|---|"]
    for item in report["reports"]:
        for method, count in item["methods"].items():
            lines.append(f"| {' / '.join(item['cohort'][:2])} / {item['metric']} | {method} | "
                         f"{count['candidates']} | {count['episodes']} | {count['suppressed']} | "
                         f"{count['episodes_per_calendar_day']:.2f} |")
    lines += ["", "Недостаточная база блокирует оценку редкости. Нули MAD/медианы не заменяются бесконечностью.",
              "Старые окна без версии спецификации исключены. Сезонность и прибыльность не подтверждены.",
              "Пересчёт использует последние версии агрегатов; воспроизведение фактического времени доставки отдельно.",
              "", "Формулы: [CUSUM, NIST](https://www.itl.nist.gov/div898/handbook/pmc/section3/pmc323.htm),",
              "[масштабированный MAD, GSL](https://www.gnu.org/software/gsl/doc/html/movstat.html).", ""]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8431")
    parser.add_argument("--input", type=Path, help="replay saved input, without network")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args(argv)
    if args.days < 1 or args.days > 90:
        parser.error("days must be 1..90")
    if args.input:
        evidence = json.loads(args.input.read_text(encoding="utf-8"))
        if digest(evidence["windows"]) != evidence["windows_sha256"]:
            raise ValueError("input checksum mismatch")
    else:
        end = (int(time.time() * 1000) - 5000) // MINUTE * MINUTE

        async def fetch():
            async with aiohttp.ClientSession() as session:
                return await fetch_windows(session, args.url, end - args.days * 1440 * MINUTE, end)

        rows = asyncio.run(fetch())
        # Preserve requested period, including absent streams and missing prefixes.
        evidence = {"from_ms": end - args.days * 1440 * MINUTE, "to_ms": end,
                    "parameters": asdict(Parameters()),
                    "windows": rows, "windows_sha256": digest(rows)}
    report = research(evidence["windows"], evidence["from_ms"], evidence["to_ms"],
                      Parameters(**evidence["parameters"]))
    report["input_sha256"] = evidence["windows_sha256"]
    args.output.mkdir(parents=True, exist_ok=True)
    for name, payload in (("input.json", evidence), ("report.json", report)):
        (args.output / name).write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False),
                                        encoding="utf-8")
    (args.output / "report.md").write_text(render(report), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "windows": len(evidence["windows"]),
                      "cohorts": len(report["reports"]), "live_approved": False}))


if __name__ == "__main__":
    main()
