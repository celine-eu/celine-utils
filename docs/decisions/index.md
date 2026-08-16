# Decisions

Architecture decision records: **why a technical choice was made here**, when the reason
is not derivable from the code and would otherwise be re-litigated.

One file per decision, named `ADR-####-short-slug.md`, with this shape:

```markdown
# ADR-0001 — <the decision, as a statement>

**Date:** <ISO-8601>
**Status:** accepted | superseded by ADR-####

## Context
<what forced a choice. The constraint, and what had already been tried.>

## Decision
<what was decided, in the imperative.>

## Consequences
<what this costs, what it forecloses, and what will tempt someone to undo it.>
```

## What is not an ADR

- **A requirement.** What the product must do belongs with the requirements, where it can
  be traced to a test. An ADR is measured by nothing.
- **A rule with a referent that something already measures.** If a statement could carry
  an identifier and a test that names it, put it where that measurement happens. Deciding
  it here hides it from the report.
- **A procedure.** That is a playbook, and playbooks live in the companion.
- **A fact about the code.** That is knowledge, and knowledge lives in the companion.

An ADR is immutable once accepted. It is superseded by a later ADR that names it, never
edited to say something else.

## The records

| ADR | Decision |
|---|---|
| [ADR-0001](ADR-0001-governance-is-a-thin-core.md) | `celine.governance` is a separate package with three dependencies |
| [ADR-0002](ADR-0002-schemas-stay-at-the-repository-root.md) | The JSON Schemas stay at `schema/` and reach the package by symlink |
| [ADR-0003](ADR-0003-two-exposure-gates.md) | Catalogue exposure and dataspace exposure are two gates, ANDed |
| [ADR-0004](ADR-0004-the-cli-ships-governance-and-pipelines-only.md) | The CLI ships `governance` and `pipeline` only |

The first three are recorded after the fact. Each documents a decision already
implemented whose reasoning existed only in code comments, where a reader looking for
*why* would not have found it.
