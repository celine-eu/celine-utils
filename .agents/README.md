# The knowledge contract

`.agents/` holds what an agent needs to work here and what it produces while working:
durable knowledge, repeatable procedures, plans and execution state.

`AGENTS.md` routes and states constraints. **This document is the rulebook** —
everything normative about how work is recorded. Where the two disagree, this one wins.
A component `AGENTS.md` states only its deltas; anything it restates from the root is a
defect rather than an override (REQ-0403).

`.agents/` is committed, except `.agents/work/` (REQ-0107).

---

## What lives where

| Artefact | Home | Committed |
|---|---|---|
| durable knowledge | `.agents/knowledge/` | yes |
| repeatable procedures | `.agents/playbooks/` | yes |
| intended changes and the decisions taken | `.agents/plans/<slug>.md` | yes |
| progress, blockers, what was verified | `.agents/work/<slug>/` | **no** |
| requirement-to-verification mapping | the trace directory, or the tool named in `.agents/harness.toml` | — |
| **defects** | the issue tracker, never this directory | — |
| why a technical choice was made | `docs/decisions/` | — |

**A directory not in that list is not created here.** The set is closed on purpose
(REQ-0003): a new one is a change to this document, which makes it deliberate and
reviewable. Where a repository genuinely needs another, it declares it under `[agents]
extra_dirs` in `.agents/harness.toml`.

---

## knowledge/

Durable facts that are **true of the code and not obvious from reading it**: invariants,
hidden assumptions, the reason the obvious change is the wrong one.

Each entry names the trap, not the feature. Knowledge must stay useful after the current
work has finished; if a fact stops being true, delete it in the same change. A stale fact
here is worse than a missing one, because it is written as settled.

What does *not* go here: behaviour the published documentation already describes,
anything a test already asserts, and anything that will be untrue next week.

---

## playbooks/

Repeatable procedures — how work of a given kind is performed here.

A playbook exists the first time a procedure is performed twice. It states the steps, the
commands, what to check afterwards, and the traps. **It links to the published
documentation rather than restating it**; two descriptions of one procedure is the
failure this rule prevents.

---

## plans/

Intended changes and the decisions taken while making them. Plans are committed.

A plan is required before any non-trivial implementation. Every plan carries YAML front
matter (REQ-0104):

```yaml
---
slug: auth-refactor          # equals the filename without .md (REQ-0105)
created: 2026-01-31          # ISO-8601
status: proposed             # proposed | in-progress | complete | superseded
requirements: REQ-0007       # optional: the requirements it serves (REQ-0501)
requires-new-spec: false     # optional: if true, the plan may not execute (REQ-0502)
---
```

A plan that needs a requirement nobody has written **stays `proposed`** until that
conversation has happened. Open questions are addressed in the plan definition phase, by
asking directly.

Plans record **decisions and deviations, and why**. They do not record progress
(REQ-0106).

---

## work/

Active execution state, one directory per plan:

```text
work/
    auth-refactor/
        status.md      the current position — required (REQ-0103)
        notes.md       discoveries still being worked out
```

`work/` is **not committed**. It holds progress, checklists, what was verified and how,
blockers, and owed work.

---

## Plans and work are one unit

A plan and its work directory share a slug and are created together:

```text
plans/auth-refactor.md   <->   work/auth-refactor/{status.md,notes.md}
```

**Create `work/<slug>/status.md` before the first change of a plan phase** (REQ-0102),
not after it. A plan being executed with no work directory is the error to catch: its
status has nothing to derive from, and the execution record is being written somewhere it
does not belong — usually into the plan itself.

The split is *survives* versus *does not*:

| Goes in `work/<slug>/` | Goes in the plan |
|---|---|
| progress, checklists, what was verified and how | decisions taken, and why |
| blockers, owed work, open costs | deviations from the plan, and why |
| counts and measurements taken during execution | anything a future reader must not lose |
| the current position | the phase status, derived from `status.md` |

- Update `status.md` **in the same change as the code**, not at the end of the phase.
- When a discovery in `notes.md` proves durable, promote it to `knowledge/` and delete it
  from `notes.md` — do not leave both.
- **A measurement pasted into a plan is stale the moment it is written.** State the exit
  criterion as the command that produces the number, and keep the number in `status.md`.

---

## Defects are not kept here

**A defect is an issue, in the project's issue tracker** (REQ-0006). A defect carries a
lifecycle, a priority and an owner, which is a tracker's job and none of the directories
above. An observation is also not a unit of work: most defects are never worked, and a
plan per defect means writing plans for things you then decline.

- **Filing and reading one:** use the forge's own tooling — `gh issue create`,
  `gh issue view 123`, `gh issue list` — or the equivalent for whatever forge is in use.
- **Where that tooling is unavailable** — no CLI, no credentials, no network — **write a
  plain reference**: state the defect in prose where it matters and say that it is
  unfiled. Never invent an identifier for it, and never start a ledger file. A named
  thing that cannot be looked up is worse than an unnamed one.
- **A code comment carries the reason, not the number.** Issues are not in the clone, so
  the sentence must survive `#123` being unresolvable.

A plan cites the issues it closes. `work/<slug>/status.md` names the ones a phase
actually closed. Neither restates the issue.

---

## Traceability

Every requirement is verified by something, and the mapping is **generated, not
authored** — a hand-maintained matrix is stale within a week.

- Where this checker owns it: requirements carry identifiers, a test declares what it
  verifies with a `@verifies <identifier>` tag, and the trace matrix is the
  projection of the two.
- **Where this repository already has a tool that answers it**, that tool keeps the job.
  Declare it under `[traceability]` in `.agents/harness.toml` and the checker reports
  those requirements as `DELEGATED`, naming it. Do not run two traceability stacks: a
  second identifier namespace and a second evidence syntax measure nothing new.

---

## Testing

Every unit of work is validated before it is reported as done.

- **Run the tests that exist**, and establish the baseline *before* changing anything, so
  a pre-existing failure is never attributed to the change.
- **Create the tests that do not.** A change with no test covering it is not finished.
  Missing coverage is work, not a caveat.
- **Report faithfully.** If a level could not be run, say which one and why. Silence must
  never read as a pass, and a change is not "verified" on the strength of the levels that
  were convenient.
- Procedures for each level belong in `.agents/playbooks/testing.md`.

---

## Choosing where information belongs

**Will this still be true and useful after this work is finished?** → `knowledge/`

**Is this a repeatable way of working?** → `playbooks/`

**Is this a proposal, or a decision taken while implementing one?** → `plans/`

**Is this only useful while executing current work?** → `work/`

**Is this something that is broken?** → an issue. Not a file here.

**Is this why a technical choice was made?** → `docs/decisions/`

**Is this the record of a plan phase that is now finished?** → split it. The decisions go
in the plan, the progress stays in `work/`.

---

## Principles

- One fact, one home. Prefer updating an existing document over creating a new one.
- Prefer a few well-maintained documents over many fragmented ones.
- Outdated knowledge is worse than missing knowledge, because it is trusted.
- If documentation and code disagree, identify the inconsistency rather than guessing.
- A number a command can produce is never written down by hand.
- A change is done when its tests pass, its documentation matches it, and what was
  skipped is stated.
