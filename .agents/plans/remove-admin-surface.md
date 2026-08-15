---
slug: remove-admin-surface
created: 2026-08-15
status: complete
requires-new-spec: false
---

# Remove the admin surface; keep pipelines and governance only

## Why

`celine-utils admin *` was a replicable-setup tool. It is dead: a workspace-wide scan
of all 20 repositories found **no invocation, no import, no subprocess call, and no
reference from any compose file, chart, workflow or script**. The last commit
touching it is `8da170a` (2026-02-04) — "chore: move to src", a mechanical move.

Its replacement is live and actively developed: `celine-policies` has a full Keycloak
CLI (`bootstrap`, `sync`, `sync-orgs`, `sync-users`, `set-password`,
`set-user-organization`, `status`). `celine-policies` imports exactly one thing from
this package, `celine.governance.validate_owners`.

Requested scope: **keep only pipelines and governance.**

## Decisions taken

**`python-keycloak` goes too, and that is the interesting part.** It was declared
only in the `[admin]` extra, yet `governance generate marquez` imports
`KeycloakClient` at module scope and `cli/app.py` imports the governance app — so the
*whole CLI* needed it. `uv add "celine-utils[pipelines]"` therefore shipped a CLI
that raised `ImportError` on `--help`. That bug predates this plan and is fixed here
rather than carried forward.

The dependency bought one thing: a client-credentials token POST with expiry
caching. It is reimplemented on `requests`, which `[pipelines]` already declares.

**Not `celine-sdk`'s `OidcClientCredentialsProvider`**, though it is already a
`[pipelines]` dependency and would be the obvious platform-consistent choice. It is
`async`, and it configures from `CELINE_OIDC_*` with OIDC discovery, while this path
configures from `KEYCLOAK_URL` + `KEYCLOAK_REALM` + `KEYCLOAK_CLIENT_ID` +
`KEYCLOAK_CLIENT_SECRET`. Adopting it would change the environment surface of
`governance generate marquez` and require `asyncio.run` in a synchronous CLI — a
behaviour change smuggled inside a removal. If that migration is wanted it should be
its own change, decided on its own merits.

**`KeycloakClient` / `KeycloakClientConfig` stay.** They are what authenticates the
Marquez request. `KeycloakAdminClient` / `KeycloakAdminConfig` go — nothing but the
admin tree used them.

**The legacy MQTT surface goes.** `common/mqtt.py` and `common/config/mqtt.py` have
zero importers. Pipeline events travel through `celine-sdk`'s broker configured by
`CELINE_MQTT_*`; the dead module read `MQTT_HOST` / `MQTT_USER`, which look like they
configure pipeline events and do not. Removing it removes the confusion at its
source rather than documenting it forever.

**The Superset helper goes.** `common/superset.py` and `common/config/superset.py`
were used only by `admin/setup/superset_setup.py`, which `run_setup()` already had
commented out — so it was dead before this plan reached it.

**`bcrypt` was declared in `[admin]` and imported nowhere**, so it leaves with the
extra it never earned.

## Breaking change

Removing a command tree and an extra is breaking: `feat!:` → **3.0.0**. Per the
release coupling in `.agents/knowledge/governance-is-a-thin-core.md`, it must be
released before any downstream repository adopts it.

`celine-pipelines` and `celine-dashboards/packages/celine-jupyter` declare
`celine-utils[all]`. Verified: neither imports the admin modules, nor `keycloak` or
`bcrypt` directly, so their behaviour is unchanged; `[all]` simply stops pulling
those two packages.

Anyone depending on `celine-utils[admin]` by name gets a **warning, not an error** —
verified with `uv pip install --dry-run '.[admin]'`, which resolves and installs
while printing `does not have an extra named 'admin'`. No repository in the workspace
names it, but that silence is why the check was by name rather than by install.

## Phases

1. Delete the admin tree, the Superset helper, the legacy MQTT surface, and
   `KeycloakAdminClient`.
2. Reimplement `KeycloakClient` on `requests`; drop `python-keycloak`.
3. `pyproject.toml`: remove the `[admin]` extra, narrow `all`.
4. Documentation and `.env.example`.
5. A test that the CLI imports without `python-keycloak`, so the packaging bug
   cannot come back.

## Deviations

Recorded here as they occur.
