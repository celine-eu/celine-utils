"""Tests for the CLI's optional debugpy bootstrap.

Imports ``celine.utils.cli.debugger`` and nothing else from the CLI. That module is
stdlib-only on purpose, so this file still runs under `task test:thin`, where
``typer`` is absent and ``celine.utils.cli.app`` cannot be imported.
"""

import sys
import types

import pytest

from celine.utils.cli.debugger import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    _is_truthy,
    start_debugger_if_requested,
)


class FakeDebugpy(types.ModuleType):
    """Stand-in for debugpy that records what the bootstrap asked it to do."""

    def __init__(self, listen_error=None):
        super().__init__("debugpy")
        self.listened = None
        self.waited = False
        self._listen_error = listen_error

    def listen(self, address):
        self.listened = address
        if self._listen_error is not None:
            raise self._listen_error

    def wait_for_client(self):
        self.waited = True


@pytest.fixture
def fake_debugpy(monkeypatch):
    module = FakeDebugpy()
    monkeypatch.setitem(sys.modules, "debugpy", module)
    return module


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("DEBUGGER", "DEBUGGER_WAIT", "DEBUGGER_HOST", "DEBUGGER_PORT"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", " on "])
def test_truthy_values(value):
    assert _is_truthy(value) is True


@pytest.mark.parametrize("value", [None, "", "0", "false", "no", "off", "maybe"])
def test_falsy_values(value):
    """Unrecognised values are false, so a typo cannot silently open a listener."""
    assert _is_truthy(value) is False


def test_does_nothing_when_unset(fake_debugpy):
    assert start_debugger_if_requested() is False
    assert fake_debugpy.listened is None


def test_listens_without_waiting(monkeypatch, fake_debugpy):
    """`task cli-dbg` sets DEBUGGER=1 DEBUGGER_WAIT=0 — listen, then run."""
    monkeypatch.setenv("DEBUGGER", "1")
    monkeypatch.setenv("DEBUGGER_WAIT", "0")

    assert start_debugger_if_requested() is True
    assert fake_debugpy.listened == (DEFAULT_HOST, DEFAULT_PORT)
    assert fake_debugpy.waited is False


def test_waits_for_client_when_asked(monkeypatch, fake_debugpy):
    monkeypatch.setenv("DEBUGGER", "1")
    monkeypatch.setenv("DEBUGGER_WAIT", "1")

    assert start_debugger_if_requested() is True
    assert fake_debugpy.waited is True


def test_host_and_port_overrides(monkeypatch, fake_debugpy):
    monkeypatch.setenv("DEBUGGER", "1")
    monkeypatch.setenv("DEBUGGER_HOST", "0.0.0.0")
    monkeypatch.setenv("DEBUGGER_PORT", "6000")

    start_debugger_if_requested()
    assert fake_debugpy.listened == ("0.0.0.0", 6000)


def test_non_numeric_port_falls_back(monkeypatch, fake_debugpy, capsys):
    monkeypatch.setenv("DEBUGGER", "1")
    monkeypatch.setenv("DEBUGGER_PORT", "not-a-port")

    assert start_debugger_if_requested() is True
    assert fake_debugpy.listened == (DEFAULT_HOST, DEFAULT_PORT)
    assert "not a number" in capsys.readouterr().err


def test_missing_debugpy_does_not_fail_the_cli(monkeypatch, capsys):
    """debugpy is a dev dependency — its absence must not stop the command."""
    monkeypatch.setenv("DEBUGGER", "1")
    monkeypatch.setitem(sys.modules, "debugpy", None)  # import returns None -> ImportError

    assert start_debugger_if_requested() is False
    assert "debugpy is not installed" in capsys.readouterr().err


def test_listen_failure_does_not_fail_the_cli(monkeypatch, capsys):
    """A port already held by an earlier run is not a reason to abort."""
    monkeypatch.setenv("DEBUGGER", "1")
    monkeypatch.setitem(
        sys.modules, "debugpy", FakeDebugpy(listen_error=OSError("address in use"))
    )

    assert start_debugger_if_requested() is False
    assert "could not listen" in capsys.readouterr().err
