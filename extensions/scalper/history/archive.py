"""Bounded off-loop raw writer with explicit failure and verifiable segments."""
import gzip
import hashlib
import json
import os
import queue
import shutil
import sqlite3
import threading
import time
import uuid
from pathlib import Path


class Archive:
    def __init__(self, root, *, queue_size=8192, segment_records=10000):
        self.root = Path(root)
        self._space_check = 0
        self.path = Path(root) / (time.strftime("%Y%m%dT%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:12])
        self.path.mkdir(parents=True, exist_ok=False)
        self.queue = queue.Queue(queue_size)
        self.segment_records = segment_records
        self.sequence = 0
        self.written = 0
        self.dropped = 0
        self.error = None
        self.closed = False
        self.last_flush_at = None
        self.segments = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._write, name="market-history-writer", daemon=True)
        self._thread.start()

    def check_space(self, now):
        if now-self._space_check < 5:
            return
        self._space_check = now
        budget = int(os.getenv("HYPERDATA_HISTORY_MAX_BYTES", str(5*1024**3)))
        runs = []
        total = 0
        for folder in self.root.iterdir():
            if not folder.is_dir():
                continue
            size = 0
            for p in folder.rglob("*"):
                try:
                    if p.is_file():
                        size += p.stat().st_size
                except FileNotFoundError:
                    pass
            total += size
            manifest = folder/"manifest.json"
            if folder != self.path and manifest.exists() and not any(folder.rglob("*.pin")):
                runs.append((manifest.stat().st_mtime, folder, size))
        for at, folder, size in sorted(runs):
            if total <= budget and now-at <= 48*3600:
                continue
            target = folder.resolve()
            if target.parent != self.root.resolve():
                raise ValueError("archive retention path escaped")
            # Only complete closed runs are eligible; interrupted/pinned evidence stays protected.
            if json.loads((target/"manifest.json").read_text(encoding="utf-8")).get("complete"):
                shutil.rmtree(target)
                total -= size
        if total > budget or shutil.disk_usage(self.root).free < 512*1024**2:
            self.error = "storage_budget_exhausted"

    def record(self, kind, payload, now, *, epoch="", monotonic_ns=None):
        self.check_space(time.time())
        self.sequence += 1
        envelope = dict(schema_version=1, run=self.path.name, sequence=self.sequence,
                        received_at=now, monotonic_ns=monotonic_ns, epoch=epoch, kind=kind, payload=payload)
        if self.closed or self.error:
            self.dropped += 1
            return None
        try:
            line = json.dumps(envelope, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n"
            self.queue.put_nowait((line, envelope if kind != "raw" else None))
        except (queue.Full, ValueError, TypeError) as exc:
            self.dropped += 1
            self.error = f"archive_incomplete: {type(exc).__name__}"
            return None
        return envelope

    def _write(self):
        handle = None
        count = 0
        last_flush = time.monotonic()
        opened_at = None
        db = None

        def finish():
            nonlocal handle, count
            if handle is None:
                return
            handle.flush()
            os.fsync(handle.fileno())
            db.commit()
            name = Path(handle.name)
            handle.close()
            handle = None
            target = name.with_suffix(".ndjson.gz")
            compressed = name.with_suffix(".compressing")
            with name.open("rb") as source, compressed.open("xb") as destination:
                with gzip.GzipFile(fileobj=destination, mode="wb", mtime=0) as packed:
                    shutil.copyfileobj(source, packed)
                destination.flush()
                os.fsync(destination.fileno())
            compressed.rename(target)
            name.unlink()
            self.segments.append(dict(file=target.name, records=count,
                                      sha256=hashlib.sha256(target.read_bytes()).hexdigest()))
            self.last_flush_at = time.time()
            count = 0

        try:
            db = sqlite3.connect(self.path / "index.sqlite")
            db.execute("CREATE TABLE events(sequence INTEGER PRIMARY KEY, at REAL NOT NULL, "
                       "kind TEXT NOT NULL, payload TEXT NOT NULL)")
            db.execute("CREATE INDEX events_kind_at ON events(kind, at)")
            while not self._stop.is_set() or not self.queue.empty():
                try:
                    queued = self.queue.get(timeout=0.1)
                except queue.Empty:
                    queued = None
                if queued is not None:
                    line, index = queued
                    if handle is None:
                        name = self.path / f"{len(self.segments):06d}.partial"
                        handle = name.open("x", encoding="utf-8", newline="\n")
                        opened_at = time.monotonic()
                    handle.write(line)
                    if index is not None:
                        db.execute("INSERT INTO events VALUES(?,?,?,?)",
                                   (index["sequence"], index["received_at"], index["kind"],
                                    json.dumps(index["payload"], allow_nan=False)))
                    count += 1
                    self.written += 1
                if handle is not None and (count >= self.segment_records or time.monotonic() - opened_at >= 3600):
                    finish()
                    last_flush = time.monotonic()
                elif handle is not None and time.monotonic() - last_flush >= 1:
                    handle.flush()
                    os.fsync(handle.fileno())
                    db.commit()
                    self.last_flush_at = time.time()
                    last_flush = time.monotonic()
            finish()
        except Exception as exc:
            self.error = f"writer_failed: {type(exc).__name__}: {exc}"
        finally:
            if handle is not None:
                handle.close()
            if db is not None:
                db.close()

    def report(self):
        return dict(run=self.path.name, accepted=self.sequence - self.dropped, written=self.written,
                    dropped=self.dropped, backlog=self.queue.qsize(), error=self.error,
                    last_flush_at=self.last_flush_at, closed=self.closed,
                    complete=self.closed and not self.error and self.written == self.sequence)

    def close(self):
        if self.closed:
            return
        self.closed = True
        self._stop.set()
        self._thread.join(timeout=15)
        if self._thread.is_alive():
            self.error = "writer_shutdown_timeout"
            return
        manifest = dict(schema_version=1, **self.report(), segments=self.segments)
        temp = self.path / "manifest.tmp"
        with temp.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(manifest, indent=2, allow_nan=False))
            handle.flush()
            os.fsync(handle.fileno())
        temp.rename(self.path / "manifest.json")


def read_archive(path, *, allow_incomplete=False):
    """Strict replay rejects incomplete runs, tampered segments and missing records."""
    path = Path(path)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if (manifest.get("schema_version") != 1 or not manifest.get("complete")
            and not (allow_incomplete and manifest.get("recovered"))):
        raise ValueError("Archive is incomplete; research replay refused")
    sequence = 0
    for segment in manifest["segments"]:
        name = segment["file"]
        if Path(name).name != name or not name.endswith((".ndjson", ".ndjson.gz")):
            raise ValueError("Invalid segment path")
        content = (path / name).read_bytes()
        if hashlib.sha256(content).hexdigest() != segment["sha256"]:
            raise ValueError("Archive checksum mismatch")
        count = 0
        decoded = gzip.decompress(content) if name.endswith(".gz") else content
        for line in decoded.decode("utf-8").splitlines():
            item = json.loads(line)
            sequence += 1
            count += 1
            if item["schema_version"] != 1 or item["sequence"] != sequence or item["run"] != manifest["run"]:
                raise ValueError("Archive ordering/version mismatch")
            yield item
        if count != segment["records"]:
            raise ValueError("Archive segment count mismatch")
    if sequence != manifest["written"] or sequence != manifest["accepted"]:
        raise ValueError("Archive count mismatch")
