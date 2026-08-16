# ADR-0004 — The CLI ships `governance` and `pipeline` only

**Date:** 2026-08-15
**Status:** accepted

## Context

`celine-utils` shipped a third command tree, `admin`, with `setup run`,
`keycloak client create|import|get-secret` and `keycloak accounts import`. It was
built as a replicable-setup tool: one command to stand a platform up from scratch.

By 2026-08 it was dead. A scan of all twenty repositories in the platform workspace
found no invocation, no import of `celine.utils.admin`, and no reference from any
compose file, chart, workflow or script. The last commit touching it was a mechanical
`chore: move to src` six months earlier. Its `setup run` entry point had already lost
half its body — the Superset step was commented out — so the command's own help
described behaviour it did not have.

Meanwhile `celine-policies` grew a real Keycloak CLI — `bootstrap`, `sync`,
`sync-orgs`, `sync-users`, `set-password`, `set-user-organization`, `status` — which
is where provisioning actually happens, and which reads the same `owners.yaml` this
repository defines the schema for.

The dead tree was not free. `python-keycloak` was declared **only** in the `[admin]`
extra, yet `celine.utils.common.keycloak` is imported by `governance generate
marquez`, which `cli/app.py` imports. So the whole CLI depended on a package only the
admin extra supplied, and `uv add "celine-utils[pipelines]"` produced a CLI that
raised `ImportError` on `--help`. A dead feature was breaking a live one.

## Decision

Remove the `admin` command tree, `celine.utils.admin`, the Superset client, the
`KeycloakAdminClient`, the unused legacy MQTT module, and the `[admin]` extra.

`celine-utils` ships two command trees — `governance` and `pipeline` — and platform
administration belongs to `celine-policies`.

Drop `python-keycloak` rather than move it. It provided one client-credentials token
POST with expiry caching, reimplemented on `requests`, which `[pipelines]` already
declares. `bcrypt`, also in the extra, was imported nowhere.

Deliberately **not** adopted: `celine.sdk.auth.OidcClientCredentialsProvider`, though
it is already a `[pipelines]` dependency. It is `async` and configures from
`CELINE_OIDC_*` via OIDC discovery, while this path configures from `KEYCLOAK_URL` +
`KEYCLOAK_REALM` + client id/secret. Adopting it would change the environment surface
of `governance generate marquez` and put `asyncio.run` in a synchronous CLI — a
behaviour change smuggled inside a removal. It remains available as its own decision.

## Consequences

**Released as a minor — `feat:` → 2.3.0 — not as a major.** This is a deliberate
departure from strict semver, and worth stating rather than leaving as an accident of
a commit message.

Removing importable modules, a CLI command tree and an extra is a major change by the
letter of it. The judgment is that the letter measures nothing here: a scan of all
twenty repositories found no importer, no invocation and no reference, so a major
version would signal a migration that nobody has to perform. Every downstream pin is
`>=`, so both a major and a minor are adopted automatically — the number changes what
a reader infers, not what a resolver does.

What that judgment accepts, so it is not rediscovered as a surprise:

- The package publishes to PyPI, so "no callers in the workspace" is not "no callers".
- `celine-utils[admin]` does **not** fail — it installs, warning `does not have an
  extra named 'admin'`, and silently omits the packages. A consumer requesting it
  finds out at an import, in whatever environment it reaches. A major version would
  have been the louder signal; the changelog entry is what carries it instead.
- Two repositories name `celine-utils[all]`, which still resolves and simply stops
  pulling `python-keycloak` and `bcrypt`. Verified: neither imports either.

If an external consumer does surface, the answer is a changelog pointer to this
record, not a restoration.

`celine-utils[pipelines]` now yields a CLI that runs. A test asserts the CLI imports
with the `keycloak` package blocked, so the dependency cannot creep back through a
module the CLI imports.

The platform loses its one-command setup story. That was already true in practice —
the command did half of what it claimed and nobody ran it — but it is now explicit,
and anyone wanting it must build it where provisioning lives rather than here.

**What will tempt someone to undo it:** needing to create a Keycloak client during a
pipeline's first run, and finding `KeycloakClient` already present in
`celine.utils.common.keycloak`. It fetches tokens; it does not administer. Extending
it back toward administration reintroduces `python-keycloak`, the extra, and
eventually the second provisioning implementation this removal deleted — while
`celine-policies` still holds the first.
