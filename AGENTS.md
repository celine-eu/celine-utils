# celine-utils — Agent Guide

Component guide. Load this, plus `.agents/README.md`, plus the workspace guide in
`celine-dev` if you are working across repositories.

This file is **navigation and constraints**: where things are and what you may not do.
It does not explain how the code works or why a decision was taken — those have homes
of their own, listed below.

## What this repository is

Shared utilities and CLIs forming the backbone of the data platform: reusable building blocks for pipelines, governance, lineage, metadata and platform integrations.

This repository **owns the governance schemas** that other repositories validate against. A schema change here must be released before any downstream repository can consume it.

## Where things are

| Looking for | Go to |
|---|---|
| what this component does | `README.md` |
| architecture, rationale, features | `docs/` |
| why a technical choice was made | `docs/decisions/` |
| a repeatable procedure | `.agents/playbooks/` |
| a trap that is true of the code and not obvious from it | `.agents/knowledge/` |
| what is being worked on | `.agents/plans/`, `.agents/work/` |
| what is broken | the issue tracker — `gh issue list`. Not a file here |
| how to run it locally | `taskfile.yaml` |

## Behavioural settings

The switches, not the rules. What each one serves is stated in `.agents/README.md`.

- **Ask rather than decide** when a request needs a requirement that does not exist
  yet. Ask directly, and do not proceed on an inferred requirement.
- **Write the plan first** for anything non-trivial, and create its work directory
  before the first change of any phase.
- **Report faithfully.** Name what ran, what did not, and what was skipped.
- **Establish the baseline before changing anything**, so a pre-existing failure is
  never attributed to your change.

## Crossing a seam

This repository is one component of a platform assembled from separate repositories.
Before changing anything exposed to another one, check whether it moves an API
contract, a data schema, governance metadata, an ontology mapping, or identity and
policy behaviour. Those five are the seams, and a change that crosses one is not local
however local it compiles.

## Structure

```text
src/celine/governance/       the package
src/celine/utils/            the package
tests/                       the suite
docs/                        human documentation, published by the docs site
.agents/                     knowledge contract, plans, playbooks, knowledge.
                             Committed, except .agents/work/
```

## Testing

The suite lives in `tests/`. Run it before you change anything and again after,
and record the invocation in `.agents/playbooks/testing.md` the first time you
work out what it is — that file ships as a shape to fill, not as an answer.

## Maintaining this file

It gets an agent to the right file quickly and stops it making a wrong edit. It is not
a design document and not a place for rationale.

- One fact, one home. Link rather than restate — a duplicated fact becomes two
  contradicting facts.
- Do not restate the workspace guide.
- Delete on sight. A stale instruction is worse than a missing one, because it is
  followed.
- Update it in the same commit as the change that dates it.
