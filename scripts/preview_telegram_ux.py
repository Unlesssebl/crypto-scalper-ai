"""Offline rendering of real frozen packets; no models, Telegram or terminal calls."""

import argparse
import html
import json
import sqlite3
from contextlib import closing
from pathlib import Path

from extensions.scalper.context_auto import summarize
from extensions.scalper.fact_packet import packet_ready, verify_packet
from extensions.scalper.telegram_ux import analytical_details, operational, scenarios


def preview(journal, output, context_version=None):
    cards, lengths, symbols, shown = [], [], set(), set()
    with closing(sqlite3.connect(journal.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        for payload, reason in db.execute(
            "SELECT p.payload,COALESCE(r.trigger_reason,'density_context') FROM fact_packets p "
            "LEFT JOIN fact_reviews r ON r.packet_id=p.id WHERE p.symbol<>'MARKET' ORDER BY p.at DESC LIMIT 1000"
        ):
            packet = verify_packet(json.loads(payload))
            if context_version and packet.get("context_version") != context_version:
                continue
            if not packet_ready(packet, packet["captured_at"]):
                continue
            state = summarize(packet)
            text, rich = operational(packet, state, reason, "situation")
            lengths.append(len(text))
            symbols.add(packet["symbol"])
            if len(cards) < 9 and packet["symbol"] not in shown:
                shown.add(packet["symbol"])
                extra = "<br>".join(html.escape(s) for s in analytical_details(packet, state) + scenarios(state))
                cards.append(
                    "<article>" + rich + "<details><summary>Сценарии наблюдения</summary>"
                    "<p>" + extra + "</p></details></article>"
                )
    if not lengths:
        raise ValueError("No usable historical packets")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        '<!doctype html><html lang="ru"><meta charset="utf-8"><title>Telegram · реальные примеры</title>'
        "<style>body{background:#111820;color:#e5edf5;font:16px/1.55 system-ui;margin:32px auto;"
        "max-width:1100px;padding:0 20px}main{display:grid;grid-template-columns:repeat(auto-fit,"
        "minmax(300px,1fr));gap:20px}article{background:#202e3c;padding:20px;border-radius:14px}"
        "summary{cursor:pointer;color:#77bded}h1{font-size:25px}header p{color:#a8b8c8}</style>"
        "<header><h1>Telegram · карточки на реальных данных</h1><p>Исторические снимки, не текущие сигналы."
        " Проверка содержания и длины; внешний вид Telegram зависит от клиента.</p></header><main>"
        + "".join(cards)
        + "</main></html>",
        encoding="utf-8",
    )
    report = dict(
        packets_rendered=len(lengths),
        symbols=len(symbols),
        max_characters=max(lengths),
        model_calls=0,
        telegram_calls=0,
        terminal_commands=0,
        limitation="Rendering replay, not signal-selection or trading-effectiveness proof",
        context_version=context_version,
    )
    output.with_suffix(".json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--context-version")
    args = parser.parse_args()
    print(json.dumps(preview(args.journal, args.output, args.context_version)))
