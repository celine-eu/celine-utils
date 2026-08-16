# ADR-0001 — `celine.governance` is a separate package with three dependencies

**Date:** 2026-08-15
**Status:** accepted

Recorded after the fact. The decision was taken and implemented across `a6c8ff0`,
`d1ae281` and `c281953`; this record exists because the reasoning lived only in code
comments and a CI job, where a future reader looking for *why* would not find it.

## Context

`celine-utils` required dbt, Meltano, Prefect and Keycloak in order to parse a YAML
file. Governance parsing was one module among an orchestration stack, and the stack
came with it.

No API service could take that dependency. So four repositories each wrote their own
`governance.yaml` parser — `celine-utils`, `dataset-api`, `ds` and `celine-superset` —
and the four disagreed about what the same file meant. The disagreements were not
cosmetic: the merge semantics differed, so a governance file resolved to different
rules depending on which service read it.

The copies were **dependency avoidance, not carelessness**. Asking people to stop
copying would not have worked, because copying was the cheaper correct response to
the constraint they faced.

A further constraint: `celine-superset` ships inside `apache/superset:6.0.0`, which
is Python 3.10, while `celine-utils` declared `>=3.12` — a floor it did not need,
inherited entirely through one extras-only dependency.

## Decision

Extract governance parsing into `celine.governance`, whose runtime dependencies are
`pydantic`, `pyyaml` and `jsonschema` — **nothing else, ever**.

- `celine.governance` must not import `celine.utils`, SQLAlchemy, OpenLineage, or
  anything from an optional extra. Where a governance module needs something from the
  fat side, the dependency goes the other way round.
- The orchestration stack moves behind optional extras: `pipelines`, `admin`,
  `openlineage`, and `all` for existing consumers.
- The import path is `celine.governance`, not `celine.utils.governance`, so the
  expected split into its own distribution is a directory rename with no consumer
  editing an import.
- The Python floor drops to 3.10, and the CI matrix is generated from the
  `pyproject.toml` classifiers so the supported range is proven rather than declared.
- **A CI job asserts the boundary** (`.github/workflows/governance-thin.yaml`): it
  installs with no extras, asserts the heavy packages are absent, and exercises the
  package. `tests/test_governance_contract.py` covers it in the ordinary suite.

## Consequences

The four parsers can be replaced by one import, which is what removes the
disagreement. `dataset-api`, `ds` and `celine-superset` can each depend on this
package without inheriting an orchestration stack.

**The cost is that the boundary must be defended continuously.** Adding
`import sqlalchemy` to a governance module is one line, and nothing in a diff makes
that line look expensive; a developer environment has every extra installed, so it
works locally and fails at a consumer's install. That is why the boundary is a CI
job and not a review convention.

Consumers that used to get everything from one install now choose extras. Existing
ones move to `celine-utils[all]`.

**What will tempt someone to undo it:** a convenience import. Governance code
wanting a helper that lives in `celine.utils`, or a "small" dependency that makes
one function neater. Each is individually reasonable and collectively reconstructs
the original problem — at which point the consumers fork again, and the four parsers
come back.

Also tempting: "tidying" `celine.governance` to `celine.utils.governance` for
consistency with the rest of the tree. That would make the eventual distribution
split a breaking change for every downstream repository.
