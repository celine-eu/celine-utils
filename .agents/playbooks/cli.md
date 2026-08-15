# Playbook — running the CLI and the local stack

What the CLI does and what its options are is [published documentation](../../docs/cli.md).
This file is only how to run it *from this checkout*.

## From the working tree

```bash
task cli -- <args>          # e.g. task cli -- governance generate marquez --help
task cli -- --help
```

`task cli` runs `uv run -m celine.utils.cli.main`, which rebuilds and reinstalls the
package into `.venv` first, so it always reflects the working tree.

**The module is `celine.utils.cli`, not `celine.cli`.** The package moved when
governance was split out; the task kept the old path and raised
`ModuleNotFoundError` for every argument it was given until 2026-08-15. If you see
that error, something has reintroduced the old path.

To skip the rebuild when you know the install is current:

```bash
uv run --no-sync celine-utils <args>
```

That is also how to run the **installed entry point** rather than the module, which
is what an end user gets.

## Under a debugger

```bash
task cli-dbg -- <args>
```

Clears `__pycache__`, sets `DEBUGGER=1 DEBUGGER_WAIT=0`, and prints
`debugpy listening on 127.0.0.1:5678` before running the command. Attach with the
**"celine-utils CLI attach"** configuration in `.vscode/launch.json`.

`DEBUGGER_WAIT=0` means the command runs immediately rather than blocking for a
client — so a fast command finishes before you can attach. To catch it, wait:

```bash
DEBUGGER=1 DEBUGGER_WAIT=1 uv run -m celine.utils.cli.main <args>
```

The listener starts **before** the Typer app is imported, so `DEBUGGER_WAIT=1`
catches breakpoints in module-level code and not only inside command bodies.

| Variable | Default | Meaning |
|---|---|---|
| `DEBUGGER` | unset | Truthy opens the listener. Unset means `debugpy` is never imported |
| `DEBUGGER_WAIT` | unset | Truthy blocks until a client attaches |
| `DEBUGGER_HOST` | `127.0.0.1` | Loopback by default — a debug listener accepts arbitrary code execution from whoever connects |
| `DEBUGGER_PORT` | `5678` | |

Debugging inside a container needs `DEBUGGER_HOST=0.0.0.0` for the host to reach the
listener. That is a deliberate exposure, not a default.

Nothing here can fail the command: a missing `debugpy` (it is in the `dev` group
only) or a port already held by an earlier run reports to stderr and continues.
`.vscode/launch.json` also carries a configuration for pytest against the integration
tests.

## The integration stack

Integration tests need Postgres, Marquez and the rest, from
`integration-tests/docker-compose.yaml`.

```bash
cd integration-tests
task start           # docker compose up -d
task run             # uv run pytest -vv
task stop            # docker compose down
task reset           # docker compose down -v  — DESTROYS volumes
```

From the repository root, `task integration-tests -- <pytest args>` runs the suite
but **does not start the stack**. Start it first.

`task reset` removes volumes, which discards the Marquez lineage history and the
seeded database. That is usually what you want when a test run has left inconsistent
state, and never what you want if you were about to inspect why a run failed.

## Commands that reach a live system

Nothing in this CLI mutates a live identity provider any more — the
`celine-utils admin *` tree was removed in 3.0.0 as dead code, superseded by
`celine-policies`' Keycloak CLI. Look there for anything that provisions clients,
roles, groups, users or organizations.

What remains reads: `governance generate marquez` calls a real Marquez, and obtains a
token from a real Keycloak when `KEYCLOAK_CLIENT_ID` / `KEYCLOAK_CLIENT_SECRET` are
set. Check which one you are pointed at before running it against anything you care
about:

```bash
celine-utils pipeline run envs | grep -E "KEYCLOAK|OPENLINEAGE"
```

The defaults are `http://keycloak:8080` and realm `celine`, so it will silently try
whatever answers on that name.
