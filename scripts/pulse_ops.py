"""Local PostgreSQL backups with a verified, isolated restore target."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import subprocess
import tempfile
import uuid
from contextlib import closing
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


def sqlite_snapshot(source, destination):
    with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True, timeout=10)) as live:
        with closing(sqlite3.connect(destination)) as snapshot:
            live.backup(snapshot)
            if snapshot.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('SQLite backup integrity check failed')
    return {'file': destination.name, 'sha256': file_hash(destination), 'bytes': destination.stat().st_size}


def verify_sqlite(path, metadata):
    snapshot = path.with_suffix('.sqlite')
    if metadata['file'] != snapshot.name or file_hash(snapshot) != metadata['sha256']:
        raise ValueError('SQLite backup checksum mismatch')
    with tempfile.TemporaryDirectory(prefix='scalper-restore-') as folder:
        restored = Path(folder) / 'journal.sqlite'
        shutil.copyfile(snapshot, restored)
        with closing(sqlite3.connect(restored)) as connection:
            if connection.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('SQLite restore integrity check failed')
            return {'integrity': 'ok', 'tables': connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0]}


def backup(directory, sqlite_path=None):
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = directory / ("market-pulse-" + stamp + "-" + uuid.uuid4().hex[:6] + ".dump")
    temporary = path.with_suffix(".partial")
    with temporary.open('wb') as handle:
        database(["pg_dump", "-U", "marketpulse", "-d", "marketpulse", "-Fc"], destination=handle)
    temporary.replace(path)
    manifest = {"file": path.name, "bytes": path.stat().st_size, "sha256": file_hash(path),
                "created_at": datetime.now(timezone.utc).isoformat(), "restore_verified": False}
    if sqlite_path is None:
        from apps.screener_web.__main__ import journal_path
        sqlite_path = journal_path(None, False)
    if sqlite_path.is_file():
        manifest['sqlite'] = sqlite_snapshot(sqlite_path, path.with_suffix('.sqlite'))
        manifest['sqlite']['restore'] = verify_sqlite(path, manifest['sqlite'])
    manifest_tmp = path.with_suffix('.json.partial')
    manifest_tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    manifest_tmp.replace(path.with_suffix('.json'))
    return path


def verify(path):
    manifest = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
    if file_hash(path) != manifest["sha256"]:
        raise ValueError("backup checksum mismatch")
    if manifest.get('sqlite'):
        manifest['sqlite']['restore'] = verify_sqlite(path, manifest['sqlite'])
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
    parser.add_argument("--sqlite-file", type=Path)
    args = parser.parse_args()
    if args.command == "verify":
        if args.file is None:
            parser.error("verify requires --file")
        print(json.dumps(verify(args.file), indent=2))
    else:
        path = backup(args.directory, args.sqlite_file)
        # Keep seven completed local snapshots. Never touch unrelated files or partial writes.
        completed = [p for p in args.directory.glob("market-pulse-*.dump") if p.with_suffix(".json").is_file()]
        for stale in sorted(completed, key=lambda p: p.stat().st_mtime)[:-7]:
            stale.unlink()
            stale.with_suffix(".json").unlink(missing_ok=True)
            stale.with_suffix(".sqlite").unlink(missing_ok=True)
        print(json.dumps({"backup": str(path), "restore_verified": False}))


if __name__ == "__main__":
    main()
