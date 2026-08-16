"""Optional ``debugpy`` bootstrap for the CLI.

``task cli-dbg`` has always set ``DEBUGGER=1 DEBUGGER_WAIT=0``, and until this
module existed **nothing read either variable**. The task was ``task cli`` plus a
``__pycache__`` clean: no listener was opened, no debugger could attach, and the
failure was silent because a debugger that never attaches looks exactly like a
debugger you forgot to connect to.

Stdlib imports only. This module is reachable from a core-only install — where
``typer`` is absent and ``celine.utils.cli.app`` cannot be imported at all — so it
must not reach for anything outside the standard library, including
``celine.utils.common.logger`` (which imports ``urllib3``).

Notices go to stderr rather than through ``logging``: this runs before the CLI has
configured logging, and a developer waiting on a listener needs to see it regardless
of how logging is eventually routed.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

#: The port VS Code and PyCharm both default to for an attach configuration.
DEFAULT_PORT = 5678

#: Loopback, not ``0.0.0.0``. A debug listener accepts arbitrary code execution from
#: whoever connects to it, so binding it to every interface would expose that to the
#: network. Override deliberately via ``DEBUGGER_HOST`` when debugging inside a
#: container, where loopback is not reachable from the host.
DEFAULT_HOST = "127.0.0.1"

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def _is_truthy(value: Optional[str]) -> bool:
    """Whether an environment variable means *yes*.

    Unset and empty are false. Anything unrecognised is false rather than true, so a
    typo does not silently open a listener.
    """
    return value is not None and value.strip().lower() in _TRUTHY


def start_debugger_if_requested() -> bool:
    """Start a ``debugpy`` listener when ``DEBUGGER`` is set. Return whether it did.

    Environment:

    ``DEBUGGER``
        Truthy to open the listener. Absent means this function does nothing at all
        and never imports ``debugpy``.
    ``DEBUGGER_WAIT``
        Truthy to block until a client attaches. ``task cli-dbg`` sets it to ``0``,
        which starts the listener and runs the command immediately.
    ``DEBUGGER_HOST`` / ``DEBUGGER_PORT``
        Where to listen. Defaults are loopback and 5678.

    **Never raises.** ``debugpy`` is a development dependency, and the port may
    already be held by an earlier run; neither is a reason for the CLI to fail. Both
    report to stderr and return ``False``, because a debugger that silently declined
    to start is the failure this module was written to remove.
    """
    if not _is_truthy(os.getenv("DEBUGGER")):
        return False

    try:
        import debugpy
    except ImportError:
        print(
            "DEBUGGER is set but debugpy is not installed — continuing without it. "
            "Install it with: uv sync --group dev",
            file=sys.stderr,
        )
        return False

    host = os.getenv("DEBUGGER_HOST", DEFAULT_HOST)
    try:
        port = int(os.getenv("DEBUGGER_PORT", str(DEFAULT_PORT)))
    except ValueError:
        print(
            f"DEBUGGER_PORT={os.getenv('DEBUGGER_PORT')!r} is not a number — "
            f"using {DEFAULT_PORT}",
            file=sys.stderr,
        )
        port = DEFAULT_PORT

    try:
        debugpy.listen((host, port))
    except Exception as exc:  # noqa: BLE001 — a failed listener must not fail the CLI
        print(
            f"debugpy could not listen on {host}:{port} ({exc}) — "
            "continuing without a debugger.",
            file=sys.stderr,
        )
        return False

    print(f"debugpy listening on {host}:{port}", file=sys.stderr)

    if _is_truthy(os.getenv("DEBUGGER_WAIT")):
        print("waiting for a debugger to attach…", file=sys.stderr)
        debugpy.wait_for_client()

    return True
