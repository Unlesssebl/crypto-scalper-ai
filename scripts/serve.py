"""Supervise the native HyperData API and the external screener.

The launchers (`start.ps1`, `start.sh`) only prepare the environment and call
this script. It starts the native hub, waits for `/v1/live`, starts the
screener, restarts the native hub if it dies, and shuts both down on Ctrl+C.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

from hyperdata_terminal.runtime_mode import UNIFIED_ENV

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / ".runtime"
LOG = logging.getLogger("serve")
MAX_RESTARTS = 5
RESTART_WINDOW = 600.0
STOP_FILES: dict[int, Path] = {}


def managed_start(command, **kwargs):
    marker = RUNTIME / ("stop-" + uuid.uuid4().hex)
    env = {**os.environ, **(kwargs.pop("env", None) or {}), "HYPERDATA_STOP_FILE": str(marker)}
    process = subprocess.Popen(command, env=env, **kwargs)
    STOP_FILES[process.pid] = marker
    return process


def _health_url(port: int) -> str:
    return f"http://127.0.0.1:{port}/v1/live"


def health_ok(port: int, timeout: float = 1.5) -> bool:
    try:
        with urllib.request.urlopen(_health_url(port), timeout=timeout) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError, ValueError):
        return False


def wait_for_health(port: int, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if health_ok(port):
            return True
        time.sleep(0.5)
    return False


def _rotate(path: Path, max_bytes: int = 50 * 1024 * 1024) -> None:
    if path.exists() and path.stat().st_size >= max_bytes:
        backup = path.with_suffix(path.suffix + ".1")
        backup.unlink(missing_ok=True)
        path.rename(backup)


def start_native(port: int, demo: bool) -> subprocess.Popen:
    RUNTIME.mkdir(parents=True, exist_ok=True)
    log_path = RUNTIME / "native.log"
    _rotate(log_path)
    command = [sys.executable, "-m", "hyperdata_terminal.cli", "api", "--port", str(port)]
    if demo:
        command.append("--demo")
    LOG.info("Starting native API: %s (log: %s)", " ".join(command), log_path)
    with log_path.open("ab") as log:
        return managed_start(
            command, cwd=ROOT, env={UNIFIED_ENV: "1"}, stdout=log, stderr=subprocess.STDOUT
        )


def start_screener(api_port: int, web_port: int, forwarded: list[str]) -> subprocess.Popen:
    command = [
        sys.executable, "-m", "apps.screener_web",
        "--api-url", f"http://127.0.0.1:{api_port}",
        "--port", str(web_port),
        *forwarded,
    ]
    LOG.info("Starting screener: %s", " ".join(command))
    return managed_start(command, cwd=ROOT, env={UNIFIED_ENV: "1"})


def stop(process: subprocess.Popen | None, name: str) -> None:
    if process is None or process.poll() is not None:
        return
    LOG.info("Stopping %s (pid %s)", name, process.pid)
    try:
        marker = STOP_FILES.get(process.pid)
        if marker is not None:
            marker.touch()
            process.wait(timeout=30)
            marker.unlink(missing_ok=True)
            STOP_FILES.pop(process.pid, None)
            return
        if sys.platform != "win32":
            # SIGINT lets asyncio.run() unwind and close SQLite/sessions.
            process.send_signal(signal.SIGINT)
        else:
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                           capture_output=True, check=False)
        process.wait(timeout=10)
    except (subprocess.TimeoutExpired, OSError):
        LOG.warning("%s did not stop cleanly; killing", name)
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True, check=False)
        else:
            process.kill()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            LOG.error("%s did not die after kill", name)


def run(args: argparse.Namespace) -> int:
    RUNTIME.mkdir(parents=True, exist_ok=True)
    native: subprocess.Popen | None = None
    screener: subprocess.Popen | None = None
    native_restarts: list[float] = []
    screener_restarts: list[float] = []
    try:
        if health_ok(args.api_port):
            LOG.info("Using the already running native API on port %d", args.api_port)
        else:
            native = start_native(args.api_port, args.demo)
            if not wait_for_health(args.api_port):
                LOG.error("Native API did not become healthy on port %d", args.api_port)
                stop(native, "native API")
                return 1

        screener = start_screener(args.api_port, args.web_port, args.forwarded)
        LOG.info("Dashboard: http://127.0.0.1:%d/scalper", args.web_port)
        while True:
            if args.stop_file is not None and args.stop_file.is_file():
                LOG.info("Cooperative shutdown requested")
                break
            now = time.monotonic()
            if native is not None and native.poll() is not None:
                native_restarts = [stamp for stamp in native_restarts if now - stamp < RESTART_WINDOW]
                if len(native_restarts) >= MAX_RESTARTS:
                    LOG.error("Native API restarted %d times in %.0fs; giving up", len(native_restarts), RESTART_WINDOW)
                    stop(screener, "screener")
                    return 1
                native_restarts.append(now)
                LOG.warning("Native API exited with %s; restarting", native.returncode)
                native = start_native(args.api_port, args.demo)
                if not wait_for_health(args.api_port):
                    LOG.error("Native API did not recover")
                    stop(screener, "screener")
                    return 1
            if screener.poll() is not None:
                screener_restarts = [stamp for stamp in screener_restarts if now - stamp < RESTART_WINDOW]
                if len(screener_restarts) >= MAX_RESTARTS:
                    LOG.error("Screener restarted %d times in %.0fs; giving up", len(screener_restarts), RESTART_WINDOW)
                    return 1
                screener_restarts.append(time.monotonic())
                LOG.warning("Screener exited with %s; restarting", screener.returncode)
                screener = start_screener(args.api_port, args.web_port, args.forwarded)
            time.sleep(2)
    except KeyboardInterrupt:
        LOG.info("Interrupted")
        return 0
    finally:
        stop(screener, "screener")
        stop(native, "native API")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo", action="store_true", help="run the native API on synthetic data")
    parser.add_argument("--api-port", type=int, default=8420, help="native API port (default: 8420)")
    parser.add_argument("--web-port", type=int, default=8421, help="screener web port (default: 8421)")
    parser.add_argument("--stop-file", type=Path, default=RUNTIME / "serve.stop",
                        help="create this local file to shut down both components cleanly")
    args, forwarded = parser.parse_known_args(argv)
    args.forwarded = forwarded
    args.stop_file.unlink(missing_ok=True)
    if args.forwarded and args.forwarded[0] == "--":
        args.forwarded = args.forwarded[1:]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    def _interrupt(_signum, _frame):
        raise KeyboardInterrupt

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _interrupt)
        except (ValueError, OSError):
            pass
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
