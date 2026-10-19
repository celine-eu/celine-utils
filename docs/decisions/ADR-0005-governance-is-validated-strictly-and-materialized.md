# ADR-0005 — Governance is validated strictly, as it will be served, on the thin core

**Date:** 2026-10-19
**Status:** accepted

Recorded after the fact; implemented in `1e7f974` (2.7.0) and `70d3d4b` (2.8.0).

## Context

The library had a validator — `validate()` for one document, `validate_owners()` for
one registry — and nothing on any import path called it. Each consumer loaded the
governance tree its own way, and the failure modes were all quiet:

- a file that failed to load was skipped, and the run exited 0;
- a path that matched nothing — a checkout missing in a deployment — checked fewer
  files and passed;
- a `row_filters` entry with a misspelled handler read as no filter;
- a dataset declaring no row policy was served whole.

Each produces a catalogue that loads cleanly and is smaller or more permissive than
its author wrote, with nothing to say why. Under a deny default the last case becomes
an outage at query time rather than a leak — better, but still discovered by a caller
rather than by the author.

Three further facts constrained the shape of a fix:

- **The unit a reader writes is not the unit that is served.** A deployment layers an
  overlay `governance.<app>.yaml` over the app's base file. An overlay has no `sources`
  of its own, so checked alone it fails the schema's root `required`; and a base can be
  valid while the merged result is not. Checking the files one by one tests neither
  what was written nor what will be served.
- **The gate must run where the governance package runs.** The `celine-utils` CLI
  needs the `pipelines` extra (dbt, Meltano, Prefect). A deployment task or a consumer's
  CI that only imports `celine.governance` cannot install that just to check a file —
  the reason the package was split into a thin core in the first place
  ([ADR-0001](ADR-0001-governance-is-a-thin-core.md)).
- **Owners are not all dataspace parties.** The first release checked every
  `ownership` name of every exposed dataset against the registry. A dataset listed only
  in the catalogue names its owners for attribution — a data source, a public body —
  and a deployment's registry holds the organizations that take part in the dataspace.
  Requiring every attribution to be a registered party fails the check on correct
  files, and a check that fails on correct files gets switched off.

## Decision

Ship `celine-governance validate` as its **own entry point on the thin core**:
`argparse`, not typer, and nothing beyond `pydantic`, `pyyaml` and `jsonschema`. The
same checks are a library call, `celine.governance.check.check_governance`, returning a
`CheckReport`.

**Validate the materialized result.** Each `governance.yaml` is merged with its
overlay — found beside it or in an `--overlay-dir` — by the resolver the exporter
uses, and the merged rules are what the row-policy, handler, consent and owners rules
read. An overlay alone is only type- and key-checked (the schema without the root
`required`), then checked again as part of its base. Two overlays for one base are
refused rather than merged in an order nobody chose. An overlay that applies to no
checked base is a warning: its app was not checked.

**Collect every finding, never raise.** Three severities: `error` fails the run,
`warning` fails it under `--strict`, `info` never does. Exit **0** when nothing fails,
**1** when something does, **2** on a usage error. A file that does not load, and an
argument matching no file, are errors.

**Owners: strict per file, then merged; names checked on dataspace offers only.**
Each `--owners` file is validated strictly against `owners.schema.json` *before* it is
merged; later files shadow earlier ones by `id`, a whole entry at a time — the loader
`celine-policies keycloak sync-orgs` uses. Ownership names are resolved only for
datasets offered into the dataspace (`dataspace.expose`): catalogue-only owners are
attribution, not dataspace parties (2.8.0). With `--dataspace`, every owner of a
dataspace offer narrowed by an organization filter must carry a DID.

The requirements this measures: REQ-0011 (checked as served, any failure fails),
REQ-0012 (both row-policy layers, known handlers), REQ-0013 (consent coherence on
dataspace offers), REQ-0014 (strict owners, resolving names) — see the
[specifications](../specifications/index.md).

## Consequences

A governance mistake now fails before an import instead of surfacing as a missing
dataset, an over-broad one, or an authorization error far from its cause. A deployment
can run the check with only the thin core installed, and so can every consumer of the
package.

The cost is that the gate must be given the **whole** input to be right. A deployment
that forgets an `--overlay-dir` checks the base files without its overlays and may pass
a tree it will not serve; the unused-overlay warning catches the overlay directory that
is passed but not matched, not the one that is never passed. Likewise the owners rule is
only as good as the registry handed to it.

Whole-entry shadowing forecloses a partial per-environment owners file: an override
must restate the entry, because each file is validated alone and the schema requires
`id`, `type` and `name`. The deployment passes its materialized registry instead. This
is deliberate — it is how `sync-orgs` reads the same files, and two loaders that merge
differently would disagree about who an owner is.

Narrowing the owners rule to dataspace offers means an unresolved attribution on a
catalogue-only dataset is not reported. The catalogue then shows the bare name rather
than a resolved identity; that is a presentation gap, not an access one.

**What will tempt someone to undo it:** a first run on an existing tree reports many
findings at once, and the quick fix is to validate files one by one, skip what does not
load, or downgrade errors to warnings. Each restores exactly the quiet failure this
exists to stop. The other temptation is to move the command into the `celine-utils`
CLI next to `governance graph` — which would put dbt, Meltano and Prefect between a
deployment and its gate.
