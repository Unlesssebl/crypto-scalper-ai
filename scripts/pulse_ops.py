"""Local PostgreSQL backups with a verified, isolated restore target."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASE = ["docker", "compose", "-f", str(ROOT / "deploy/market-pulse/compose.yml"), "exec", "-T", "postgres"]


def database(command, *, data=None, source=None, destination=None):
    result = subprocess.run([*BASE, *command], cwd=ROOT, input=data, stdin=source,
                            stdout=destination or subprocess.PIPE, stderr=subprocess.PIPE, check=True,
                            creationflags=subprocess.CREATE_NO_WINDOW if __import__('os').name == 'nt' else 0)
    return result.stdout or b''


def file_hash(path):
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def backup(directory):
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = directory / ("market-pulse-" + stamp + "-" + uuid.uuid4().hex[:6] + ".dump")
    temporary = path.with_suffix(".partial")
    with temporary.open('wb') as handle:
        database(["pg_dump", "-U", "marketpulse", "-d", "marketpulse", "-Fc"], destination=handle)
    temporary.replace(path)
    manifest = {"file": path.name, "bytes": path.stat().st_size, "sha256": file_hash(path),
                "created_at": datetime.now(timezone.utc).isoformat(), "restore_verified": False}
    manifest_tmp = path.with_suffix('.json.partial')
    manifest_tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    manifest_tmp.replace(path.with_suffix('.json'))
    return path


def verify(path):
    manifest = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
    if file_hash(path) != manifest["sha256"]:
        raise ValueError("backup checksum mismatch")
    target = "pulse_restore_" + uuid.uuid4().hex
    # This generated target is the only database this command can create or remove.
    database(["createdb", "-U", "marketpulse", target])
    try:
        with path.open('rb') as handle:
            database(["pg_restore", "-U", "marketpulse", "--exit-on-error", "-d", target], source=handle)
        sql = ("SELECT json_build_object('events',(SELECT count(*) FROM execution_events),"
               "'windows',(SELECT count(*) FROM windows),'controls',(SELECT count(*) FROM pulse_control));")
        counts = json.loads(database(["psql", "-U", "marketpulse", "-d", target, "-Atc", sql]))
        quarantine_exists = database(["psql", "-U", "marketpulse", "-d", target, "-Atc",
                                       "SELECT to_regclass('event_quarantine') IS NOT NULL;"]).strip() == b't'
        counts['quarantined'] = int(database(["psql", "-U", "marketpulse", "-d", target, "-Atc",
                                              "SELECT COUNT(*) FROM event_quarantine;"])) if quarantine_exists else None
        overview_exists = database(["psql", "-U", "marketpulse", "-d", target, "-Atc",
                                    "SELECT to_regclass('overview_candles') IS NOT NULL;"]).strip() == b't'
        counts['overview_candles'] = (int(database(["psql", "-U", "marketpulse", "-d", target, "-Atc",
                                                   "SELECT COUNT(*) FROM overview_candles;"]))
                                      if overview_exists else None)
        manifest.update(restore_verified=True, restored_counts=counts,
                        verified_at=datetime.now(timezone.utc).isoformat())
        path.with_suffix(".json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest
    finally:
        if not target.startswith("pulse_restore_") or len(target) != len("pulse_restore_") + 32:
            raise ValueError("invalid isolated restore target")
        database(["dropdb", "-U", "marketpulse", target])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("backup", "verify"))
    parser.add_argument("--directory", type=Path, default=ROOT / ".runtime/backups")
    parser.add_argument("--file", type=Path)
    args = parser.parse_args()
    if args.command == "verify":
        if args.file is None:
            parser.error("verify requires --file")
        print(json.dumps(verify(args.file), indent=2))
    else:
        path = backup(args.directory)
        # Keep seven completed local snapshots. Never touch unrelated files or partial writes.
        completed = [p for p in args.directory.glob("market-pulse-*.dump") if p.with_suffix(".json").is_file()]
        for stale in sorted(completed, key=lambda p: p.stat().st_mtime)[:-7]:
            stale.unlink()
            stale.with_suffix(".json").unlink(missing_ok=True)
        print(json.dumps({"backup": str(path), "restore_verified": False}))


if __name__ == "__main__":
    main()
