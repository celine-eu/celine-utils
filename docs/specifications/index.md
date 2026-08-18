# Requirements

What this repository **must** do, as opposed to what it currently does. Each
requirement carries an identifier, and a test declares what it verifies with a
`@verifies REQ-####` tag on its own line directly above the test. The trace matrix is
the projection of the two — generated, never hand-maintained.

Scope is deliberately the **shared surface**: the properties other repositories
depend on, where a regression here breaks something there. Pipeline execution is out
of scope for now — it has integration coverage but no unit evidence, so requirements
there would need evidence written alongside them. Platform administration is out of
scope permanently: it was removed in 2.3.0
([ADR-0004](../decisions/ADR-0004-the-cli-ships-governance-and-pipelines-only.md)).

A requirement here states a property and its consequence. It is not an ADR — the
reasoning for a decision lives in [decisions](../decisions/index.md) — and it is not
documentation of behaviour, which lives in the pages these link to.

## The register

| ID | Requirement | Verified by |
|---|---|---|
| REQ-0001 | `celine.governance` imports on core dependencies alone | `tests/test_governance_contract.py`, `.github/workflows/governance-thin.yaml` |
| REQ-0002 | Catalogue and dataspace exposure are separate gates, ANDed | `tests/test_governance_exposure.py` |
| REQ-0003 | The package works on every Python version it claims | `tests/test_supported_python_range.py`, `.github/workflows/test.yaml` |
| REQ-0004 | The published facet schema URL keeps resolving | `tests/test_governance_contract.py` |
| REQ-0005 | An unknown key never silently changes a dataset's governance | `tests/test_governance_contract.py` |
| REQ-0006 | A declared dependency resolves to a producer, or is reported | `tests/test_governance_graph.py` |
| REQ-0007 | A schedule contradicting the graph is reported, and an uncertain one is not asserted | `tests/test_governance_graph.py` |

---

## REQ-0001 — `celine.governance` imports on core dependencies alone

`celine.governance` and every module beneath it MUST import successfully with only
`pydantic`, `pyyaml` and `jsonschema` installed.

It MUST NOT import `celine.utils`, SQLAlchemy, OpenLineage, Keycloak, pandas, dbt,
Meltano or Prefect, at module scope or inside a function. Where a governance module
needs something from the fat side, the dependency inverts: `celine.utils` may import
`celine.governance`, never the reverse.

**Consequence if violated:** `dataset-api`, `ds` and `celine-superset` cannot import
this package without inheriting an orchestration stack, which is the condition that
produced four disagreeing parsers. The violation appears at a consumer's install,
not here — a developer environment has every extra installed.

Rationale: [ADR-0001](../decisions/ADR-0001-governance-is-a-thin-core.md).
Behaviour: [the governance library](../governance-library.md#the-dependency-contract).

## REQ-0002 — Catalogue and dataspace exposure are separate gates, ANDed

`expose` MUST gate catalogue listing and API serving. `dataspace.expose` MUST gate
the dataspace offer. Dataspace access requires both.

- `expose` MUST be tri-state. Unset MUST fall back to `dataspace.expose`, so a file
  written before the split keeps its catalogue behaviour unchanged.
- `dataspace.expose` MUST NOT fall back in either direction.
- `expose: false` with `dataspace.expose: true` MUST be reported as a conflict rather
  than resolved. It MUST be reported rather than raised, so a caller can collect
  every conflict in one run.
- An override that states `expose: false` MUST withdraw a dataset the baseline
  exposed.

**Consequence if violated:** either data is published that the catalogue never
advertised, or an offer someone deliberately made is silently dropped. Both are
security-relevant, which is why neither direction may be chosen implicitly.

Rationale: [ADR-0003](../decisions/ADR-0003-two-exposure-gates.md).
Behaviour: [the two gates](../governance.md#exposure--two-gates-not-one).

## REQ-0003 — The package works on every Python version it claims

The `Programming Language :: Python :: 3.x` classifiers in `pyproject.toml` are the
single declaration of the supported range. `requires-python` MUST agree with them,
and CI MUST build its matrix from them rather than from a second list.

Every submodule of `celine.governance` and `celine.utils` MUST import on every
version in that range, with the extras installed.

**Consequence if violated:** the supported range becomes aspirational. `celine-sdk`
lost two of 553 submodules to a version-gated import while its own suite passed, and
this package once declared a 3.12 floor it did not need — inherited through an
extras-only dependency — which locked `celine-superset` out entirely, since it ships
inside `apache/superset:6.0.0` on Python 3.10.

## REQ-0004 — The published facet schema URL keeps resolving

`celine.governance.facet.SCHEMA_URL` MUST remain
`https://celine-eu.github.io/schema/GovernanceDatasetFacet.schema.json`, and that URL
MUST keep serving a schema the emitted facets validate against.

The schema files MUST stay at `schema/` in the repository root, and MUST remain
readable from the installed package through `importlib.resources` — not by walking
from `__file__`, which fails inside a wheel or a container.

**Consequence if violated:** no build breaks. Every OpenLineage event already emitted
and sitting in Marquez carries this URL, so changing or moving it silently
invalidates historical lineage, and nothing reports it.

Rationale: [ADR-0002](../decisions/ADR-0002-schemas-stay-at-the-repository-root.md).

## REQ-0005 — An unknown key never silently changes a dataset's governance

A key the grammar does not define MUST be preserved in `rule.extra` rather than
dropped, and MUST be reported by `validate` — as a warning by default, and as an
error under `strict=True`.

`KNOWN_KEYS` MUST list every field of `GovernanceRule` that a file may declare. A
field present in the model and absent from `KNOWN_KEYS` parses into `extra` and reads
as permanently absent.

Parsing MUST go through `model_validate` on the keys a file actually declared, so
`model_fields_set` distinguishes *unset* from *set to a falsy value*.

**Consequence if violated:** a misspelled key — `access_levl: open` — validates
against the schema, is discarded by the model, and the dataset silently takes the
default. Under kwargs construction the merge degrades to "override always wins" and
`expose: false` becomes inexpressible, which is how a withdrawal that validated clean
left a dataset in the catalogue.

Behaviour: [unknown keys](../governance.md#unknown-keys),
the companion's knowledge.

## REQ-0006 — A declared dependency resolves to a producer, or is reported

`depends_on` MUST name **datasets**, never pipelines, and the producing pipeline MUST
be resolved by matching each entry against the `sources` keys of every governance file
in the scanned set.

- Matching MUST be case-sensitive `fnmatch` applied in **both** directions, so either
  the dependency or the `sources` key may be the glob.
- An entry resolving to no producer MUST be reported, unless it declares
  `external: true`. An entry declaring `external: true` that *does* resolve MUST be
  reported as informational rather than as a fault.
- A dataset declared by two governance files MUST be reported.
- A pipeline that cannot be placed in a run order MUST be reported and MUST NOT be
  emitted in a tier.
- Absent `depends_on` MUST remain distinct from `depends_on: []`: the first states that
  a pipeline has not declared its inputs, the second that it has none.
- An unknown key at the **root** of a governance document MUST be reported by
  `validate`, on the same terms as an unknown key inside a block.
- The graph MUST be computable within the dependencies of REQ-0001.
- `active: false` MUST mark a pipeline as not meant to run, and an active pipeline
  reading from an inactive one MUST be reported.

**Consequence if violated:** each app is its own dbt project, so `ref()` never crosses
an app boundary and nothing else in the platform can see the ordering across pipelines
— it lives in cron offsets and hand-maintained tier tables. A dependency that resolves
to nothing and is not reported produces a graph that looks authoritative and is
silently incomplete, which is worse than no graph: an ordering is read as an
instruction. The both-directions rule is what keeps this from happening per
deployment: `ds_dev_*` is the default everywhere, but a consumer resolves the schema
through `CELINE_SILVER_SCHEMA` / `CELINE_GOLD_SCHEMA`, which a deployment may point
anywhere.

Behaviour: [`depends_on`](../governance.md#depends_on--what-a-pipeline-consumes),
[the CLI](../cli.md#governance-graph).

## REQ-0007 — A schedule contradicting the graph is reported, and an uncertain one is not asserted

Given a deployment's scheduled flows, the graph MUST report a producer/consumer pair
whose crons contradict the declared edge:

- **Collision** — the two can start in the same minute, so which run the consumer sees
  depends on which container starts first.
- **Inversion** — both fire hourly, never in the same minute, and the consumer is always
  earlier in the hour, so it always reads the previous run's output.

Two things it MUST NOT do:

- **Assert an ordering it cannot know.** Governance is per app; a deployment schedules
  *flows*, and one app can deploy several on independent crons. Where either side
  deploys more than one, the pairing may not be the one that moves the data, and the
  finding MUST be reported as advisory and excluded from `--strict`.
- **Compare minutes across periods.** Pairs on different periods MUST be reported only
  on an identical cron expression; reasoning about a real offset between an hourly and a
  daily flow needs the run duration, which is in no file.

Schedules MUST NOT be read from `governance.yaml`. A cron is a deployment fact — one app
runs several flows, and the same pipeline runs on different schedules in different
deployments — while a governance file is one per app and is shared across every
deployment that installs it.

**Consequence if violated:** the failure this catches is silent by construction. A
consumer that runs before its producer succeeds every time, on stale data, and no test
or alert fires — the ordering was only ever expressed as a cron offset, which is a
convention nothing enforces. The advisory split matters just as much: a check that
reports a pairing it cannot verify as though it were certain gets ignored, and then so
does the one that was right.

Behaviour: [`governance graph`](../cli.md#governance-graph).
