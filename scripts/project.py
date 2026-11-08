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


def fetch(url, token=None, timeout=3):
    request = urllib.request.Request(
        url, data=b"" if token else None, headers={"Authorization": "Bearer " + token} if token else {}
    )
    try:
        # These probes and stop credentials belong to local services. System
        # proxy settings must never route them through an external proxy.
        response = urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return {"http_status": response.status, "data": json.load(response)}


def control(path="/status", token=None):
    if not STATE.exists():
        return None
    try:
        state = json.loads(STATE.read_text(encoding="utf-8"))
        # /status probes three services, each allowed its own timeout. Give the
        # whole request more than one probe so a busy supervisor never looks
        # like a stopped one (that used to start a duplicate worker).
        result = fetch(state["url"] + path, token, 15)
    except (OSError, ValueError):
        return None
    if result["data"].get("root") != str(ROOT):
        raise RuntimeError("management port belongs to a different project")
    return result["data"]


def pid_alive(pid):
    if not pid:
        return False
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True, check=False
        )
        return str(pid) in result.stdout
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


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


def start_binance():
    """Use the native app's network, with raw files shared with Pulse ingest."""
    (RUNTIME / "pulse-raw" / "binance").mkdir(parents=True, exist_ok=True)
    marker = RUNTIME / "binance.stop"
    marker.unlink(missing_ok=True)
    token = dotenv_values(ROOT / "deploy/market-pulse/.env").get("PULSE_INGEST_TOKEN")
    if not token:
        raise RuntimeError("Binance collector requires the configured Pulse ingest token")
    env = {**os.environ, "PULSE_DYNAMIC_ATTENTION": "1", "PULSE_MARKETS": "",
           "PULSE_INGEST_URL": "http://127.0.0.1:8430", "PULSE_INGEST_TOKEN": token,
           "PULSE_RAW_DIR": str(RUNTIME / "pulse-raw"), "HYPERDATA_STOP_FILE": str(marker)}
    with (RUNTIME / "binance.log").open("ab") as log:
        return subprocess.Popen([sys.executable, "-m", "hyperdata_terminal.market_pulse", "collector",
                                 "--venue", "binance"], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)


def stop_binance(process):
    if process is None or process.poll() is not None:
        return
    (RUNTIME / "binance.stop").touch()
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        stop_tree(process)


def request_stop():
    if not STATE.exists():
        return
    try:
        state = json.loads(STATE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if not pid_alive(state.get("pid")):
        STATE.unlink(missing_ok=True)
        return
    control("/stop", state.get("token"))
    for _ in range(120):
        if not STATE.exists():
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


def service_ports_free(api_port, web_port):
    for port, path in ((api_port, "/v1/live"), (web_port, "/healthz")):
        try:
            fetch(f"http://127.0.0.1:{port}{path}", None, 3)
        except OSError:
            continue
        except ValueError as exc:
            raise RuntimeError(
                f"port {port} answered with a non-project payload; stop the old process first"
            ) from exc
        raise RuntimeError(f"port {port} is already used by an unmanaged process; stop the old launcher first")


def management_port_free():
    try:
        fetch("http://127.0.0.1:8422/status", None, 3)
    except OSError:
        return
    except ValueError as exc:
        raise RuntimeError("management port 8422 is used by another process") from exc
    raise RuntimeError("project management port 8422 is already in use; stop the old launcher first")


def worker(args, forwarded):
    # Refuse a duplicate start before touching STATE: an overwritten state file
    # orphans the live supervisor (its stop token becomes unreachable).
    management_port_free()
    service_ports_free(args.api_port, args.web_port)
    shutdown = threading.Event()
    token = secrets.token_urlsafe(32)
    state = {"root": str(ROOT), "phase": "starting", "pid": os.getpid()}
    child = None
    backup_process = None
    binance_process = None
    binance_restarts = []
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
                **({"market_pulse": "http://127.0.0.1:8431/health"} if not args.demo else {}),
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
        if not args.demo:
            env_path = ROOT / "deploy/market-pulse/.env"
            if not env_path.exists():
                env_path.write_text(
                    "POSTGRES_PASSWORD="
                    + secrets.token_hex(24)
                    + "\nPULSE_INGEST_TOKEN="
                    + secrets.token_hex(32)
                    + "\nPULSE_MARKETS=\n"
                )
            pulse_started = True
            (RUNTIME / "pulse-raw" / "binance").mkdir(parents=True, exist_ok=True)
            compose("up", "-d", *([] if args.no_build else ["--build"]))
            binance_process = start_binance()
        env = os.environ.copy()
        env["PULSE_MARKETS"] = ""
        env["PULSE_API_URL"] = "" if args.demo else "http://127.0.0.1:8431"
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
            if binance_process is not None and binance_process.poll() is not None:
                binance_restarts = [at for at in binance_restarts if time.monotonic() - at < 600]
                if len(binance_restarts) >= 5:
                    raise RuntimeError("Binance collector restart limit reached; inspect .runtime/binance.log")
                binance_restarts.append(time.monotonic())
                binance_process = start_binance()
            if not args.demo and time.time() - last_backup_attempt >= 3600:
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
        stop_binance(binance_process)
        stop_application(child)
        try:
            if pulse_started:
                compose("stop")
        finally:
            server.shutdown()
            server.server_close()
            remove_owned_state(token)


# Windows creation flags, spelled out so the module imports on POSIX too.
CREATE_NEW_PROCESS_GROUP = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
CREATE_BREAKAWAY_FROM_JOB = getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0x01000000)
CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)


def spawn_supervisor(command, log, windows=None):
    """Start the --worker supervisor so it outlives the shell that launched it.

    On Windows the child would otherwise stay in the launcher's Job object and
    die with it when that shell's process tree is torn down. It breaks away from
    the job, gets its own process group and its own hidden console (no
    DETACHED_PROCESS: a console-less supervisor would make every console child
    it spawns -- serve.py, docker compose, taskkill -- open a visible window).
    If the parent job forbids breakaway, it falls back to staying in the job.
    On POSIX it starts a new session, away from the terminal's process group.
    """
    windows = os.name == "nt" if windows is None else windows
    options = {"cwd": ROOT, "stdout": log, "stderr": subprocess.STDOUT}
    if not windows:
        return subprocess.Popen(command, start_new_session=True, **options)
    flags = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
    try:
        return subprocess.Popen(command, creationflags=flags | CREATE_BREAKAWAY_FROM_JOB, **options)
    except PermissionError as exc:
        log.write(
            (
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} WARNING project: parent job forbids breakaway ({exc}); "
                "the supervisor will stop if the launching shell's process tree is killed\n"
            ).encode()
        )
        log.flush()
        return subprocess.Popen(command, creationflags=flags, **options)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--stop", action="store_true")
    action.add_argument("--restart", action="store_true")
    action.add_argument("--status", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--api-port", type=int, default=8420)
    parser.add_argument("--web-port", type=int, default=8421)
    args, forwarded = parser.parse_known_args()
    load_dotenv(ROOT / ".env")
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
            args.demo = args.demo or saved.get("demo", False)
        request_stop()
        if args.stop:
            print("Project stopped; database and raw archives retained.")
            return
    running = control()
    if running:
        print(json.dumps(running, indent=2))
        return
    if STATE.exists():
        # A live pid without a working /status means the supervisor is busy or
        # half-started. Never start a second worker over it: the duplicate
        # would bind the management port and clobber the owner's state.
        try:
            saved = json.loads(STATE.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            saved = {}
        if pid_alive(saved.get("pid")):
            raise RuntimeError(
                "project supervisor is alive but /status is unavailable; retry --status or --stop"
            )
        STATE.unlink(missing_ok=True)
    if "--tui" in forwarded:
        worker(args, forwarded)
        return
    flags = ["--worker", "--api-port", str(args.api_port), "--web-port", str(args.web_port)]
    flags += [
        name
        for enabled, name in (
            (args.no_build, "--no-build"),
            (args.demo, "--demo"),
        )
        if enabled
    ]
    with (RUNTIME / "project.log").open("ab") as log:
        child = spawn_supervisor([sys.executable, __file__, *flags, *forwarded], log)
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
