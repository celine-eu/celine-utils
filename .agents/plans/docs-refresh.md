---
slug: docs-refresh
created: 2026-08-15
status: complete
requires-new-spec: false
---

# Rebuild `docs/` against the code, and fill the harness directories

## Why now

The repository migrated to the agent harness with `.agents/` almost empty — one
knowledge file, one playbook still carrying its `<TODO>` template — while `docs/`
had not been revised since two changes that altered what the code means:

- `c281953` / `a6c8ff0` split governance parsing into `celine.governance`, a
  three-dependency package other repositories import. Nothing in `docs/`
  describes it as a library.
- `b98fba2` split one exposure boolean into two gates. `docs/governance.md`
  still documents the single-flag model, which is now wrong rather than
  incomplete.

Documentation that contradicts the code is worse than absent documentation,
because it is followed. That is what makes this a rewrite and not an edit pass.

## Decisions taken

**Two equal documentation tracks, not one.** The repository serves two audiences
that share almost nothing: consumers importing `celine.governance` (`dataset-api`,
`ds`, `celine-superset`) and developers building pipeline applications. A single
narrative would force each to read the other's. Decided with the requester.

**The broken taskfile entries are fixed, not documented.** `task cli` and
`task cli-dbg` invoke `celine.cli`, a module that does not exist; a playbook
citing them would be a procedure that cannot be performed. `dump-source` is
removed outright — its script `scripts/dump_source.py` is absent from the tree and
the task is obsolete. Decided with the requester.

**Requirements are a separate plan.** Introducing a requirement set means
authoring requirements, which is the requester's call and not derivable from the
code. It is `plans/requirements-baseline.md`, and it stays `proposed` until that
conversation has happened.

**`AGENTS.md` is left as it stands.** It is bespoke where the standard declares
that file identical across repositories, and the standard's own instruction is to
report the divergence rather than reconcile it. Normalising it is deferred at the
requester's direction; it is recorded here so the deferral is visible.

**Generated CLI text is not pasted by hand.** `docs/cli.md` states the command
that produces each help output. A pasted `--help` is stale on the next option
added, with nothing to detect it.

## Phases

1. **Ops fixes** — repair `task cli` / `cli-dbg`, remove `dump-source` and its
   orphaned `dump.yaml`.
2. **Library track** — `docs/governance/`: the file format rewritten against the
   current models, the public API, validation, owners, exposure gates.
3. **Platform track** — `docs/platform/`: CLI reference covering all three
   command trees, environment reference, pipeline tutorial reviewed.
4. **Decisions** — ADRs for the three choices currently recorded only in code
   comments: the thin core, the `schema/` symlink, the exposure split.
5. **Harness directories** — `playbooks/testing.md` filled in for real, plus
   `release.md` and `cli.md`; knowledge entries for the traps that keep being
   re-derived.

## Deviations

**Two tracks, one flat directory.** The plan assumed `docs/governance/` and
`docs/platform/` subdirectories. That was abandoned on discovering that the
documentation site's navigation lives in `celine-eu.github.io/repos.yaml` as an
explicit per-file list, so moving `docs/governance.md` would break both its nav entry
and its published URL. The two tracks are expressed by `docs/index.md` routing
instead, and every existing filename is preserved. The constraint is recorded in
`.agents/knowledge/docs-are-published-by-another-repository.md`.

**The five new pages are unlisted until another repository changes.** Adding them to
the site navigation is a change to `celine-eu.github.io`, outside this repository and
outside this plan. Recorded as owed work rather than done.

**`task cli-dbg` was repaired rather than documented as broken.** Its module path
was dead *and* `DEBUGGER` / `DEBUGGER_WAIT` were read by nothing, so no debugger had
ever attached. The requester chose to implement the bootstrap rather than file it,
which took this plan into `src/` — outside its stated scope — as
`celine/utils/cli/debugger.py`, with `tests/test_cli_debugger.py` covering it.

Two constraints shaped that module and are worth not undoing:

- **Stdlib imports only.** `task test:thin` runs the whole of `tests/` on a
  core-only install where `typer` is absent, so the test file must not reach
  `celine.utils.cli.app`, and the module it does import must not reach
  `celine.utils.common.logger` (which imports `urllib3`).
- **The listener starts before the Typer app is imported**, so `DEBUGGER_WAIT=1`
  catches module-level breakpoints. That is why `main()` imports `app` lazily.
