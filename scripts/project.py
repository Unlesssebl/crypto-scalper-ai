"""One local lifecycle for native API, screener and the Market Pulse Compose stack."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from dotenv import dotenv_values, load_dotenv

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / ".runtime"
STATE = RUNTIME / "project.json"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "deploy/market-pulse/compose.yml")]


def fetch(url, token=None):
    request = urllib.request.Request(
        url, data=b"" if token else None, headers={"Authorization": "Bearer " + token} if token else {}
    )
    try:
        response = urllib.request.urlopen(request, timeout=3)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return {"http_status": response.status, "data": json.load(response)}


def control(path="/status", token=None):
    if not STATE.exists():
        return None
    try:
        state = json.loads(STATE.read_text(encoding="utf-8"))
        result = fetch(state["url"] + path, token)
    except (OSError, ValueError):
        return None
    if result["data"].get("root") != str(ROOT):
        raise RuntimeError("management port belongs to a different project")
    return result["data"]


def compose(*args):
    subprocess.run([*COMPOSE, *args], cwd=ROOT, check=True)


def stop_tree(process):
    if process is None or process.poll() is not None:
        return
    if os.name == "nt":
        # Windows venv launchers have child interpreters: terminate our whole
        # known child tree, not just the small launcher executable.
        subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True, check=False)
    else:
        process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()


def stop_application(process):
    if process is None or process.poll() is not None:
        return
    (RUNTIME / "serve.stop").touch()
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        stop_tree(process)


def request_stop():
    state = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else None
    if not state or not control():
        return
    control("/stop", state["token"])
    for _ in range(120):
        if control() is None:
            return
        time.sleep(0.5)
    raise RuntimeError("project shutdown timed out; inspect .runtime/project.log")


def remove_owned_state(token):
    """An older supervisor must never delete its successor's credentials."""
    try:
        saved = json.loads(STATE.read_text(encoding="utf-8"))
        if saved.get("token") == token:
            STATE.unlink(missing_ok=True)
    except (OSError, ValueError):
        pass


def worker(args, forwarded):
    shutdown = threading.Event()
    token = secrets.token_urlsafe(32)
    state = {"root": str(ROOT), "phase": "starting", "pid": os.getpid()}
    child = None
    backup_process = None
    pulse_started = False

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *unused):
            pass

        def reply(self, status, body):
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(encoded)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                pass

        def do_GET(self):
            if self.path != "/status":
                self.reply(404, {"root": str(ROOT)})
                return
            services = {}
            for name, url in {
                "native": f"http://127.0.0.1:{args.api_port}/v1/live",
                "screener": f"http://127.0.0.1:{args.web_port}/healthz",
                **({"market_pulse": "http://127.0.0.1:8431/health"} if not args.without_pulse else {}),
            }.items():
                try:
                    result = fetch(url)
                    data = result["data"]
                    services[name] = {
                        "http_status": result["http_status"],
                        "status": data.get("status", "ok" if data.get("healthy") else "starting"),
                    }
                    if name == "market_pulse":
                        services[name].update(
                            open_gaps=data.get("storage", {}).get("open_gaps"),
                            stale_channels=len(data.get("stale_channels", [])),
                        )
                except (OSError, ValueError):
                    services[name] = {"status": "unavailable"}
            self.reply(200, {**state, "services": services})

        def do_POST(self):
            if self.path != "/stop" or self.headers.get("Authorization") != "Bearer " + token:
                self.reply(403, {"root": str(ROOT)})
                return
            self.reply(202, {"root": str(ROOT), "phase": "stopping"})
            shutdown.set()

    server = ThreadingHTTPServer(("127.0.0.1", 8422), Handler)
    temporary = STATE.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            {
                "url": "http://127.0.0.1:8422",
                "token": token,
                "pid": os.getpid(),
                "forwarded": forwarded,
                "api_port": args.api_port,
                "web_port": args.web_port,
                "without_pulse": args.without_pulse,
                "demo": args.demo,
            }
        ),
        encoding="utf-8",
    )
    temporary.replace(STATE)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: shutdown.set())
    try:
        for port, path in ((args.api_port, "/v1/live"), (args.web_port, "/healthz")):
            try:
                fetch(f"http://127.0.0.1:{port}{path}")
            except OSError:
                continue
            raise RuntimeError(f"port {port} is already used by an unmanaged process; stop the old launcher first")
        if not args.without_pulse:
            env_path = ROOT / "deploy/market-pulse/.env"
            if not env_path.exists():
                env_path.write_text(
                    "POSTGRES_PASSWORD="
                    + secrets.token_hex(24)
                    + "\nPULSE_INGEST_TOKEN="
                    + secrets.token_hex(32)
                    + "\nPULSE_MARKETS=BTC,XRP\n"
                )
            pulse_started = True
            compose("up", "-d", *([] if args.no_build else ["--build"]))
        env = os.environ.copy()
        env["PULSE_UNIFIED"] = "0" if args.without_pulse else "1"
        pulse_config = dotenv_values(ROOT / "deploy/market-pulse/.env")
        env["PULSE_MARKETS"] = os.getenv("PULSE_MARKETS") or pulse_config.get("PULSE_MARKETS") or "BTC,XRP"
        env["PULSE_API_URL"] = "" if args.without_pulse else "http://127.0.0.1:8431"
        command = [
            sys.executable,
            str(ROOT / "scripts/serve.py"),
            "--api-port",
            str(args.api_port),
            "--web-port",
            str(args.web_port),
            *(["--demo"] if args.demo else []),
            *forwarded,
        ]
        child = subprocess.Popen(command, cwd=ROOT, env=env)
        backup_process = None
        last_backup_attempt = 0
        deadline = time.monotonic() + 45
        while not shutdown.wait(1):
            if not args.without_pulse and time.time() - last_backup_attempt >= 3600:
                last_backup_attempt = time.time()
                snapshots = [
                    p for p in (RUNTIME / "backups").glob("market-pulse-*.dump") if p.with_suffix(".json").is_file()
                ]
                latest_backup = max((p.stat().st_mtime for p in snapshots), default=0)
                if time.time() - latest_backup >= 86400 and (
                    backup_process is None or backup_process.poll() is not None
                ):
                    with (RUNTIME / "backups.log").open("ab") as log:
                        backup_flags = []
                        if "--journal" in forwarded:
                            backup_flags = ["--sqlite-file", forwarded[forwarded.index("--journal") + 1]]
                        backup_process = subprocess.Popen(
                            [sys.executable, str(ROOT / "scripts/pulse_ops.py"), "backup", *backup_flags],
                            cwd=ROOT,
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                        )
            if child.poll() is not None:
                raise RuntimeError("application supervisor exited; inspect .runtime/project.log")
            if state["phase"] == "starting":
                try:
                    if fetch(f"http://127.0.0.1:{args.web_port}/healthz")["http_status"] == 200:
                        state["phase"] = "running"
                except OSError:
                    pass
                if time.monotonic() > deadline:
                    raise RuntimeError("screener readiness timed out")
    finally:
        state["phase"] = "stopping"
        stop_tree(backup_process)
        stop_application(child)
        try:
            if pulse_started:
                compose("stop")
        finally:
            server.shutdown()
            server.server_close()
            remove_owned_state(token)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--stop", action="store_true")
    action.add_argument("--restart", action="store_true")
    action.add_argument("--status", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--without-pulse", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--api-port", type=int, default=8420)
    parser.add_argument("--web-port", type=int, default=8421)
    args, forwarded = parser.parse_known_args()
    load_dotenv(ROOT / ".env")
    args.without_pulse = args.without_pulse or args.demo
    RUNTIME.mkdir(exist_ok=True)
    if args.worker:
        worker(args, forwarded)
        return
    if args.status:
        print(json.dumps(control() or {"phase": "stopped", "root": str(ROOT)}, indent=2))
        return
    if args.stop or args.restart:
        if args.restart and STATE.exists():
            saved = json.loads(STATE.read_text(encoding="utf-8"))
            if not forwarded:
                forwarded = saved.get("forwarded", [])
            if "--api-port" not in sys.argv:
                args.api_port = saved.get("api_port", args.api_port)
            if "--web-port" not in sys.argv:
                args.web_port = saved.get("web_port", args.web_port)
            args.without_pulse = args.without_pulse or saved.get("without_pulse", False)
            args.demo = args.demo or saved.get("demo", False)
        request_stop()
        if args.stop:
            print("Project stopped; database and raw archives retained.")
            return
    running = control()
    if running:
        print(json.dumps(running, indent=2))
        return
    if "--tui" in forwarded:
        worker(args, forwarded)
        return
    flags = ["--worker", "--api-port", str(args.api_port), "--web-port", str(args.web_port)]
    flags += [
        name
        for enabled, name in (
            (args.without_pulse, "--without-pulse"),
            (args.no_build, "--no-build"),
            (args.demo, "--demo"),
        )
        if enabled
    ]
    with (RUNTIME / "project.log").open("ab") as log:
        child = subprocess.Popen(
            [sys.executable, __file__, *flags, *forwarded],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
    for _ in range(360):
        if child.poll() is not None:
            raise RuntimeError("project startup failed; inspect .runtime/project.log")
        status = control()
        if status and status["phase"] == "running":
            print(json.dumps(status, indent=2))
            print(f"Dashboard: http://127.0.0.1:{args.web_port}/scalper")
            return
        time.sleep(0.5)
    raise RuntimeError("startup still pending; use --status and .runtime/project.log")


if __name__ == "__main__":
    main()
