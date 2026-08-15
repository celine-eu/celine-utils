---
slug: requirements-baseline
created: 2026-08-15
status: in-progress
requirements: REQ-0001 REQ-0002 REQ-0003 REQ-0004 REQ-0005
requires-new-spec: false
---

# Introduce a requirement set and wire traceability to it

## What forced this

`.agents/harness.toml` has every section commented out, so `[traceability]` defaults
to `provider = "harness"`. That provider answers "is this requirement verified?" from
`docs/specifications/` plus `@verifies` tags on tests. Neither existed: no
`docs/specifications/`, no `@verifies` tag, and no `REQ-` identifier anywhere outside
the rulebook's own examples.

So the repository claimed a traceability provider it did not feed. The requester
chose to fix that by writing real requirements rather than by declaring the question
delegated.

## Decisions taken

**Scope is the shared surface only**, decided with the requester. Five requirements,
covering the properties other repositories depend on and where a regression here
breaks something there. Pipeline execution and platform administration are excluded:
they have integration coverage but no unit evidence, so requirements there would need
evidence written alongside them, which is a different and larger piece of work.

**The five were chosen because the evidence already existed.** Each was a property
enforced by a test or a CI job whose comment explained why it mattered — protected in
practice, unstateable in the abstract, and invisible to any report. Writing the
requirement was the only missing half. That is what makes this cheap and what makes
the resulting trace meaningful rather than ceremonial.

**One document, not one file per requirement.** `docs/specifications/index.md` holds
all five plus the register. Five files and an index that nobody updates is the
failure mode the rulebook warns about, and at this size the split buys nothing.

**A requirement states a property and its consequence, and links out for the rest.**
Rationale stays in `docs/decisions/`, behaviour stays in the published documentation,
traps stay in `.agents/knowledge/`. A requirement that restated any of them would
become the copy that goes stale.

**`harness.toml` is left untouched.** The defaults — `provider = "harness"` and the
`REQ-[0-9]{4}` pattern — are exactly what this repository now does. Declaring them
explicitly would add a claim a reviewer must check without changing behaviour.

## Deviations

**REQ-0003 needed evidence written, unlike the other four.** The supported Python
range was enforced only by `.github/workflows/test.yaml` generating its matrix from
the `pyproject.toml` classifiers — a workflow, not a test, and therefore invisible to
the checker. `tests/test_supported_python_range.py` was written to close the one gap
CI cannot check about itself: whether `requires-python` and the classifiers still
agree. The workflow remains the evidence that the code *runs* on each version; the
test is the evidence that the declaration is coherent.

It skips below Python 3.11, which has no `tomllib` — and 3.10 is a version this
package supports precisely because of the incident REQ-0003 exists to prevent. CI
runs it on three of the four supported versions.

## Owed

- A trace report has never been produced. `python -m harness` is not installed in
  this workspace, so the mapping is asserted by construction — every requirement in
  the register names its evidence, and every tag names an existing requirement — but
  not yet by the tool that is supposed to generate it.
- Whether to extend to the pipeline and admin surfaces remains open, and deliberately
  so.
