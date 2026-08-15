# ADR-0002 — The JSON Schemas stay at `schema/` and reach the package by symlink

**Date:** 2026-08-15
**Status:** accepted

Recorded after the fact; implemented in `d1ae281`.

## Context

Two requirements pulled the schema files in opposite directions.

**They must stay at `schema/` in the repository root.** The documentation site
serves that directory directly, via the `links` block for this repository in
`celine-eu.github.io/repos.yaml`. That makes
`https://celine-eu.github.io/schema/GovernanceDatasetFacet.schema.json` the live
location of the `_schemaURL` embedded in every OpenLineage event already emitted and
sitting in Marquez. Moving the directory breaks that URL — and breaks it for
historical events, which no redeployment can fix.

**They must be inside the package.** `celine.governance.validation` executes the
schemas at runtime, and it reads them through `importlib.resources`, which can only
see files inside a package. Reading them by walking up from `__file__` was rejected:
it breaks the moment the code runs from a wheel or inside a container, which is
where it actually runs.

Copying the files into the package satisfies both mechanically and creates two
copies of a contract, which drift.

## Decision

Keep one authoritative copy at `schema/` in the repository root, and make
`src/celine/governance/schema` a **symlink** to `../../../schema`.

The package data is declared explicitly in `pyproject.toml` under
`[tool.setuptools.package-data]`, and `celine.governance*` is listed explicitly in
the packages `include` rather than left to a glob — an omission there produces a
wheel that imports fine from a source checkout and fails only at a consumer's
install.

`.github/workflows/governance-thin.yaml` triggers on `schema/**` as well as on
`src/celine/governance/**`, because a schema edit does not match the source path
through the symlink and would otherwise ship untested.

## Consequences

One file, two reachable paths, no drift. The published URL and the runtime resource
are the same bytes by construction rather than by discipline.

**The cost is that the symlink looks like a mistake.** It is the kind of thing a
tidying pass converts into a real directory or a copy, and doing so silently
recreates the duplication — after which the published schema and the executed schema
can disagree, and nothing reports it.

Symlinks also require the checkout to preserve them. Any packaging or CI step that
materialises the tree without symlink support will produce a wheel whose schema
directory is a broken link.

**What will tempt someone to undo it:** wanting the package to be self-contained, or
wanting the schemas beside the code that validates against them. Both are reasonable
instincts, and both cost the stable published URL that historical lineage depends on.
