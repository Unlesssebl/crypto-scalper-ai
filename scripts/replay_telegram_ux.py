"""Causal notification-selection replay on a complete native archive, without external effects."""

import argparse
import hashlib
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

from extensions.scalper.consistency_store import ConsistencyStore
from extensions.scalper.context_auto import ContextMemory
from extensions.scalper.delivery import CardOutbox
from extensions.scalper.fact_packet import build_packet
from extensions.scalper.history.archive import read_archive
from extensions.scalper.journal import Journal
from extensions.scalper.telegram_ux import TelegramUX


def run(path):
    with tempfile.TemporaryDirectory(prefix="telegram-replay-") as folder:
        journal = Journal(Path(folder) / "replay.sqlite")
        try:
            service = SimpleNamespace(
                journal=journal,
                context_memory=ContextMemory(journal.db),
                consistency=ConsistencyStore(journal.db),
                hub=SimpleNamespace(alerts=SimpleNamespace(telegram_chat_id="offline"), attention={}),
                attention=SimpleNamespace(activity={}, plan={}),
                context_packets={},
                pulse=None,
            )
            service.outbox = CardOutbox(journal, service.hub.alerts)
            ux = TelegramUX(service)
            ux.watch_at = float("inf")
            assignments, facts = {}, 0
            for event in read_archive(path):
                if event["kind"] == "attention_plan":
                    assignments = event["payload"]["applied"]
                if event["kind"] != "facts":
                    continue
                card = event["payload"]
                assigned = assignments.get(card["symbol"])
                if assigned is None:
                    continue
                now = event["received_at"]
                packet = build_packet(dict(policy_version=4, archive=dict(error=None)), dict(assigned, facts=card), now)
                facts += 1
                service.context_memory.observe(packet)
                service.context_packets = {card["symbol"]: packet}
                ux.observe(now)
            rows = list(
                journal.db.execute(
                    "SELECT episode_id,text FROM episode_cards WHERE destination='operations' ORDER BY episode_id"
                )
            )
            digest = hashlib.sha256(json.dumps([tuple(r) for r in rows], ensure_ascii=False).encode()).hexdigest()
            return dict(fact_snapshots=facts, notifications=len(rows), checksum=digest)
        finally:
            journal.close()


def replay(path, output):
    first, second = run(path), run(path)
    if first != second:
        raise ValueError("Notification replay is not deterministic")
    report = dict(
        archive=path.name,
        deterministic=True,
        **first,
        model_calls=0,
        telegram_calls=0,
        terminal_commands=0,
        limitation="Native L2/tape selection only: broad candle ratios are absent from this archive",
    )
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(replay(args.archive, args.output)))
