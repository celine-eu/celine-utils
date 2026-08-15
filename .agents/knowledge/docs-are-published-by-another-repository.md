# Adding a file to `docs/` does not publish it

Verified against `celine-eu.github.io/repos.yaml` on 2026-08-15.

## The trap

`docs/**` is published to the CELINE documentation site, but **this repository does
not control what appears there**. The site's navigation is declared in a different
repository, `celine-eu.github.io`, in `repos.yaml`, as an explicit per-file list:

```yaml
    nav:
      - README
      - Pipeline Tutorial: docs/pipeline-tutorial.md
      - Governance: docs/governance.md
      - Governance Schemas: docs/schemas.md
      - CLI: docs/cli.md
```

So a new page under `docs/` is copied by the build and reachable by URL, but appears
in no navigation until `repos.yaml` is edited. Nothing here reports the omission, and
the local file looks entirely correct.

The consequence runs the other way too: **renaming or moving a listed file breaks its
nav entry and its published URL.** `docs/governance.md` may not become
`docs/governance/index.md` as a local tidy-up.

## What this means for a change

- Adding a page is a **two-repository change**. Add the file here, then add the nav
  entry in `celine-eu.github.io/repos.yaml`, or state plainly that the page is
  unlisted and why.
- Keep the filenames of listed pages stable. Restructure by adding files beside them,
  not by moving them.
- `update-docs.yaml` fires a repository-dispatch to `celine-eu.github.io` on pushes
  touching `README.md`, `docs/**`, `ontologies/**` or `schema/**`. That triggers a
  **rebuild**, which is not the same as registering a new page.

## `schema/` is published the same way, and matters more

The same `repos.yaml` entry has a `links` block serving this repository's `schema/`
directory at `https://celine-eu.github.io/schema/`. That URL is the `_schemaURL`
embedded in every OpenLineage event already sitting in Marquez, so the coupling is
not cosmetic — see `docs/decisions/ADR-0002-schemas-stay-at-the-repository-root.md`.

`update-docs.yaml` lists `schema/**` in its paths for exactly this reason: without
that line the published copy silently drifts from the source.
