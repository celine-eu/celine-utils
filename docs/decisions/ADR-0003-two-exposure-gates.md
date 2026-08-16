# ADR-0003 — Catalogue exposure and dataspace exposure are two gates, ANDed

**Date:** 2026-08-15
**Status:** accepted

Recorded after the fact; implemented in `b98fba2`.

## Context

Only `dataspace.expose` existed, and the catalogue exporter copied it straight onto
the catalogue's own flag. One boolean therefore answered two different questions:

- is this dataset listed in the catalogue and queryable through the API?
- is it offered into the dataspace — reachable under a negotiated contract, by a
  third party, on a different legal footing?

There was no way to say yes to one and no to the other. A dataset that had to appear
in the catalogue was thereby offered into the dataspace, and one withheld from the
dataspace was also unqueryable through the API.

The live case is `grid_substations`: grid topology carries `dataspace.expose: true`
not because anyone decided it belongs in a dataspace, but because that was the only
way the dashboard could see it. Every `dataspace.expose: true` in the deployed files
is, for the same reason, a statement about the *catalogue*.

A related failure sat underneath. The merge used truthiness, so an overlay saying
`expose: false` — the obvious way to withdraw a dataset — dumped to nothing and the
base's `true` survived. The documented workaround was `access_level: secret`, which
is a different statement about a different thing.

## Decision

Introduce a top-level `expose` field for catalogue visibility, keep
`dataspace.expose` for the dataspace offer, and resolve both through
`celine.governance.exposure`.

- **`expose` is tri-state.** `None` means *not stated* and falls back to
  `dataspace.expose`, so every file written against the old grammar keeps its
  current catalogue behaviour exactly. That is what makes the split shippable ahead
  of the file migration rather than in lockstep with it.
- **`dataspace_expose` has no fallback in either direction.** A dataset is offered
  only where something actually said so.
- **The gates are AND.** Dataspace access requires both.
- **Offered but unlisted is a contradiction**, reported by `exposure_conflict`
  rather than resolved. It is reported rather than raised, so a caller can collect
  every conflict in a run instead of failing on the first.
- The merge moves from truthiness to `exclude_unset`, which is what makes
  `expose: false` expressible at all.

## Consequences

The two questions can now be answered independently, and a dataset can be catalogued
without being offered — which is what the deployed files actually meant.

Silently picking a direction for the contradiction was rejected because it is
security-relevant whichever way it goes: granting publishes data the catalogue never
advertised, withholding drops an offer someone deliberately made.

**The cost is a migration that has not happened.** Until the deployed files state
`expose` explicitly, they rely on the fallback, and the fallback is invisible in the
file — a reader sees only `dataspace.expose` and cannot tell whether the catalogue
behaviour was intended or inherited. New files should state `expose` explicitly.

`dataspace.expose` is deliberately **not** OR-merged, unlike `consent_required` and
`contract_required`. OR-ing it would mean *once offered, always offered* — a
loosening, and precisely the bug this replaces. Anyone reasoning "the dataspace
fields tighten under overlay, so `expose` should too" will be tempted to add it.

**What will tempt someone to undo it:** the fallback looks like dead weight once a
few files are migrated, and removing it early silently changes the catalogue
visibility of every file that has not been. The fallback goes when the files are
migrated, not before.
