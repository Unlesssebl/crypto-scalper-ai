"""Non-blocking single-key reader for the full-screen Rich dashboards.

Rich's Live(screen=True) owns the screen but not the keyboard, so a daemon
thread polls stdin and forwards printable keys. It never runs when stdin is
not a TTY (tests, redirected output), and it restores the terminal mode on
stop.
"""
from __future__ import annotations

import os
import sys
import threading
from typing import Callable

POLL_SECONDS = 0.05


class KeyPager:
    def __init__(self, on_key: Callable[[str], None]) -> None:
        self._on_key = on_key
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._restore: Callable[[], None] | None = None

    @property
    def active(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> bool:
        if self._thread is not None:
            return self.active
        try:
            interactive = sys.stdin is not None and sys.stdin.isatty()
        except (ValueError, OSError):
            interactive = False
        if not interactive:
            return False
        self._thread = threading.Thread(target=self._run, name="tui-keys", daemon=True)
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop.set()
        if self._restore is not None:
            try:
                self._restore()
            except Exception:
                pass
            self._restore = None

    def _run(self) -> None:
        try:
            if os.name == "nt":
                self._windows()
            else:
                self._posix()
        except Exception:
            # A key reader must never take the dashboard down.
            pass

    def _emit(self, char: str) -> None:
        try:
            self._on_key(char)
        except Exception:
            pass

    def _windows(self) -> None:
        import msvcrt

        while not self._stop.is_set():
            if not msvcrt.kbhit():
                self._stop.wait(POLL_SECONDS)
                continue
            char = msvcrt.getwch()
            if char in ("\x00", "\xe0"):  # arrow/function-key prefix
                msvcrt.getwch()
                continue
            self._emit(char.lower() if char.isprintable() else char)

    def _posix(self) -> None:
        import select
        import termios
        import tty

        fd = sys.stdin.fileno()
        previous = termios.tcgetattr(fd)
        self._restore = lambda: termios.tcsetattr(fd, termios.TCSADRAIN, previous)
        # cbreak keeps ISIG enabled, so Ctrl+C still raises KeyboardInterrupt.
        tty.setcbreak(fd)
        try:
            while not self._stop.is_set():
                ready, _, _ = select.select([sys.stdin], [], [], POLL_SECONDS)
                if not ready:
                    continue
                raw = os.read(fd, 1)
                if not raw:
                    continue
                char = raw.decode(errors="ignore")
                if not char:
                    continue
                self._emit(char.lower() if char.isprintable() else char)
        finally:
            if self._restore is not None:
                self._restore()
                self._restore = None
