# Playbook — cutting a release

## When a release is required, not optional

This repository **owns the governance schemas** other repositories validate against,
and ships `celine.governance`, which `dataset-api`, `ds` and `celine-superset`
import. A change to either is not usable downstream until it is released.

So: **release before asking a downstream repository to adopt something.** A
downstream floor is raised when the old version resolves to a package that breaks a
shipped path — not merely one that lacks a feature.

## What decides the version

`python-semantic-release`, from the commit messages since the last tag. Configured in
`pyproject.toml`:

| Commit prefix | Bump |
|---|---|
| `feat:` | minor |
| `fix:`, `perf:`, `up:` | patch |
| `feat!:` or `BREAKING CHANGE:` | major |

Squashed commits are parsed for these prefixes; merge commits are ignored. A commit
that matches nothing produces no release — which is the usual reason `task release`
appears to do nothing.

The version lives in `pyproject.toml:project.version`; tags are `v{version}`;
`CHANGELOG.md` is generated.

## Before

1. `task test` — and record the number.
2. `task test:thin` — mandatory if anything under `src/celine/governance/`,
   `schema/` or the dependency lists changed.
3. Check the documentation matches the change. `docs/` is published from `main`, and
   `update-docs.yaml` fires on `README.md`, `docs/**`, `ontologies/**` and
   `schema/**` — a schema change with no docs change still triggers the site rebuild.
4. Confirm you are on `main` and up to date. `semantic_release.branch` is `main`;
   running elsewhere will not produce the release you expect.

## Cutting it

```bash
task release
```

Which is:

```bash
uv run semantic-release -v version --no-vcs-release
git push
git push --tags
```

`semantic-release version` bumps `pyproject.toml`, regenerates `CHANGELOG.md`, runs
the build command (`uv lock --upgrade-package celine-utils`, `git add uv.lock`,
`uv build`), commits and tags. `--no-vcs-release` means it does **not** create the
GitHub release itself.

The tag push is what publishes: `.github/workflows/release.yaml` builds on every push
to `main` but its `publish` job runs only `if: startsWith(github.ref, 'refs/tags/')`.
**Forgetting `git push --tags` produces a version that exists in git and nowhere
else** — no wheel on PyPI, and downstream repositories cannot resolve it.

`python-semantic-release` is in the `dev` dependency group with a
`python_version >= '3.11'` marker, so releasing from a 3.10 environment will not have
the tool.

## After

1. Confirm the workflow's `publish` job actually ran, and check for the failure mode
   the workflow comments record: an upload rejected with
   `InvalidDistribution: '2.5' is not a valid metadata version` **after** the tag,
   the changelog and the GitHub release have all succeeded. It looks like a
   successful release from git alone.
2. Confirm the version resolves: `uv pip index versions celine-utils`, or install it
   somewhere clean.
3. If a schema changed, confirm
   `https://celine-eu.github.io/schema/<file>` serves the new content — the docs site
   rebuild is a separate repository-dispatch to `celine-eu.github.io`, not part of
   this release.

## Known trap in the release workflow

`release.yaml` still carries `## TODO: create a trusted publisher on PyPI` and the
`environment:` block is commented out. If publishing fails on authentication, that is
why — it is not a transient error, and retrying the workflow will not fix it.
