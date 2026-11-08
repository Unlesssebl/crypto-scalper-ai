"""The project supervisor must be spawned detached from the launching shell."""

import io

from scripts import project


class FakePopen:
    calls = []
    deny_breakaway = False

    def __init__(self, command, **kwargs):
        FakePopen.calls.append(kwargs)
        if FakePopen.deny_breakaway and kwargs.get("creationflags", 0) & project.CREATE_BREAKAWAY_FROM_JOB:
            raise PermissionError(13, "Access is denied")


def _spawn(monkeypatch, windows, deny=False):
    FakePopen.calls = []
    FakePopen.deny_breakaway = deny
    monkeypatch.setattr(project.subprocess, "Popen", FakePopen)
    log = io.BytesIO()
    child = project.spawn_supervisor(["python", "project.py", "--worker"], log, windows=windows)
    assert isinstance(child, FakePopen)
    return FakePopen.calls, log.getvalue().decode()


def test_windows_supervisor_breaks_away_from_launcher_job(monkeypatch):
    calls, log = _spawn(monkeypatch, windows=True)
    assert len(calls) == 1
    flags = calls[0]["creationflags"]
    assert flags & project.CREATE_BREAKAWAY_FROM_JOB
    assert flags & project.CREATE_NEW_PROCESS_GROUP
    assert flags & project.CREATE_NO_WINDOW
    assert "start_new_session" not in calls[0]
    assert calls[0]["stderr"] is project.subprocess.STDOUT and calls[0]["cwd"] == project.ROOT
    assert log == ""


def test_windows_supervisor_falls_back_when_breakaway_denied(monkeypatch):
    calls, log = _spawn(monkeypatch, windows=True, deny=True)
    assert len(calls) == 2
    assert calls[0]["creationflags"] & project.CREATE_BREAKAWAY_FROM_JOB
    retry = calls[1]["creationflags"]
    assert not retry & project.CREATE_BREAKAWAY_FROM_JOB
    assert retry & project.CREATE_NEW_PROCESS_GROUP and retry & project.CREATE_NO_WINDOW
    assert "WARNING" in log and "breakaway" in log


def test_posix_supervisor_starts_new_session(monkeypatch):
    calls, log = _spawn(monkeypatch, windows=False)
    assert len(calls) == 1
    assert calls[0]["start_new_session"] is True
    assert "creationflags" not in calls[0]
    assert log == ""
