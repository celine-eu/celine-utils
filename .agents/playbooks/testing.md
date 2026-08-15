# Playbook — testing a change

<TODO: this file is a shape to fill in. Delete every instruction that does not survive
contact with this repository, and keep the ones that do.>

## Before touching anything

Run the suite that covers what you are about to change, and record the result:

```bash
<TODO: the command>
```

A suite that was already red stays attributable to whoever made it red. Skipping this is
how a pre-existing failure becomes "the change broke it".

## The layers

| Layer | Command | Proves |
|---|---|---|
| <TODO> | <TODO> | <TODO — what this layer proves that no other one does> |

A layer whose command nobody can find is a layer nobody runs. If a level exists only in
somebody's shell history, that is the first thing to fix here.

## Declaring what a test verifies

<TODO: if requirements are traced in this repository, state the marker syntax. The
default is a `@verifies <identifier>` tag on its own line directly above the test; if a
tool of this repository's own answers traceability, name its marker instead and keep
`.agents/harness.toml` in agreement.>

## Reporting

Name the layers that ran, the layers that did not, and why. A layer skipped because it
needs infrastructure you did not start is a fact about the evidence, not an admission.

**A green run is only evidence about the thing that actually ran.** When a layer reports
success, ask what it exercised; if the output does not say, make that visible in the
product rather than in the test.
