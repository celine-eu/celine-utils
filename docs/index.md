# celine-utils documentation

Two audiences use this repository, and they share almost nothing. Start with the
track that matches what you are doing.

---

## Governance as a library

You are building a service that reads `governance.yaml` — a catalogue API, a policy
exporter, an analytics integration — and you want the format parsed the same way
every other CELINE component parses it, without inheriting an orchestration stack.

| Page | Covers |
|---|---|
| [The governance library](governance-library.md) | `celine.governance`: installation, the three-dependency contract, resolving, merging, validation, exposure gates, facet building |
| [`governance.yaml` format](governance.md) | the grammar — every field, resolution and merge semantics, the two exposure gates |
| [`owners.yaml` registry](governance-owners.md) | turning owner aliases into canonical identities |
| [JSON Schemas](schemas.md) | the published contracts and how to validate against them |

The short version:

```bash
uv add celine-utils        # three runtime dependencies, no extras needed
```

```python
from pathlib import Path
from celine.governance import GovernanceResolver, validate_file, effective_expose

validate_file(Path("governance.yaml"))
rule = GovernanceResolver.from_file(Path("governance.yaml")).resolve("db.schema.table")
effective_expose(rule)
```

---

## Building and running pipelines

You are writing or operating a CELINE pipeline application — Meltano ingestion, dbt
transformation, Prefect flows, with lineage and governance emitted along the way.

| Page | Covers |
|---|---|
| [Pipeline tutorial](pipeline-tutorial.md) | end to end: scaffold, configure, run, deploy |
| [CLI reference](cli.md) | every command — `governance` and `pipeline` |
| [Environment reference](environment.md) | every variable, its default, and the traps |
| [`governance.yaml` format](governance.md) | what to declare for the datasets your pipeline produces |

The short version:

```bash
uv add "celine-utils[pipelines]"
celine-utils pipeline init app my_app
cd my_app
celine-utils pipeline run meltano
celine-utils pipeline run dbt gold
```

---

## Contracts and choices

[Requirements](specifications/index.md) states what this package must do on the
surface other repositories depend on, and names the test that verifies each one.

[Decisions](decisions/index.md) records the architectural choices that would
otherwise be re-litigated — the thin governance core, the schema location, the split
exposure gates.
