"""Audit legacy HL recovery against exact raw identities; preserve suspect rows in quarantine."""
from __future__ import annotations

import argparse
import asyncio
import json
import time
import zlib
from pathlib import Path

from .collectors.hyperliquid import normalize_hyperliquid_trades
from .contracts import ExecutionEvent, InstrumentSpec
from .postgres import PostgresStore
from .service import database_url, raw_root


def audit_rows(rows, specs, root):
    handles, bad, errors, verified = {}, [], {}, 0
    root = (Path(root) / "hyperliquid").resolve()
    try:
        for row in rows:
            try:
                event = ExecutionEvent.from_payload(json.loads(row["payload"]) if isinstance(row["payload"], str)
                                                    else row["payload"])
                segment, separator, offset = (event.raw_ref or "").rpartition(":")
                path = (root / segment).resolve()
                if not separator or path.parent != root:
                    raise ValueError("invalid raw reference")
                if path not in handles:
                    handles[path] = path.open('rb')
                handle = handles[path]
                handle.seek(int(offset))
                record = json.loads(handle.readline())
                envelope = record["payload"]
                encoded = json.dumps(envelope, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode()
                if record["crc32"] != f"{zlib.crc32(encoded) & 0xFFFFFFFF:08x}":
                    raise ValueError("raw checksum mismatch")
                spec = specs[(event.native_market_id, event.instrument_spec_version)]
                candidates = normalize_hyperliquid_trades(envelope["payload"], spec,
                    receive_time_ms=envelope["receive_time_ms"], source_epoch=envelope["source_epoch"],
                    raw_ref=event.raw_ref)
                matches = any(c.content_fingerprint() == event.content_fingerprint() for c in candidates)
                if matches:
                    verified += 1
                else:
                    bad.append(row["event_seq"])
                    errors["identity_or_content_mismatch"] = errors.get("identity_or_content_mismatch", 0) + 1
            except (OSError, ValueError, TypeError, KeyError) as exc:
                bad.append(row["event_seq"])
                key = "unverifiable_" + type(exc).__name__
                errors[key] = errors.get(key, 0) + 1
    finally:
        for handle in handles.values():
            handle.close()
    return {"version": "raw-identity-audit.v1", "checked": len(rows), "verified": verified,
            "suspect": len(bad), "reasons": errors, "suspect_sequences": bad}


async def run(apply, output):
    store = await PostgresStore.connect(database_url())
    try:
        async with store.pool.acquire() as connection:
            rows = await connection.fetch("""SELECT event_seq,payload FROM execution_events e
                WHERE venue='hyperliquid' AND delivery_kind='recovery'
                AND NOT EXISTS(SELECT 1 FROM event_quarantine q WHERE q.event_seq=e.event_seq)
                ORDER BY raw_ref,event_seq""")
            raw_specs = await connection.fetch("SELECT payload FROM instrument_specs WHERE venue='hyperliquid'")
        specs = [InstrumentSpec.from_payload(json.loads(r["payload"]) if isinstance(r["payload"], str)
                                            else r["payload"]) for r in raw_specs]
        report = await asyncio.to_thread(audit_rows, rows,
                                         {(s.native_market_id, s.spec_version): s for s in specs}, raw_root())
        report["quarantined"] = 0
        if apply:
            now = int(time.time()*1000)
            report["quarantined"] = await store.quarantine_events(report["suspect_sequences"],
                                                                   "raw identity/content audit failed", now)
            await store.set_control("integrity_audit", {k: v for k, v in report.items()
                                                        if k != "suspect_sequences"}, now)
        output.write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps({k: v for k, v in report.items() if k != "suspect_sequences"}, indent=2))
    finally:
        await store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(run(args.apply, args.output))


if __name__ == '__main__':
    main()
