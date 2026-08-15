# CELINE Utils

A collection of shared utilities, libraries, and command-line tools that form the technical backbone of the CELINE data platform. Provides reusable building blocks for data pipelines, governance, lineage, metadata management, and platform integrations.

Not an end-user application — a platform utility layer embedded into CELINE applications and executed within orchestrated environments using Meltano, dbt, Prefect, and OpenLineage.


---

## Scope and goals

- Centralise cross-cutting platform logic used by multiple CELINE projects
- Provide opinionated but extensible tooling for data pipelines
- Enforce consistent governance and lineage semantics
- Reduce duplication across pipeline applications
- Act as a stable foundation for CELINE-compatible services and workflows

---

## Key capabilities

### Governance framework

A declarative `governance.yaml` specification defines the metadata, access control, and dataspace exposure rules for each dataset.

The `GovernanceRule` model covers:

- Dataset ownership (`ownership`, `attribution`), resolved through an [owner registry](docs/governance-owners.md)
- License and access level (`open`, `internal`, `restricted` — `secret` is accepted for compatibility and normalised to `restricted`)
- Data classification (`pii`, `green`, `yellow`, `red`) and retention
- Tags, documentation links, and source system
- `row_filters` — list of filter specs (`[{handler, args}]`) for per-subject consent-based row filtering
- **Two exposure gates, ANDed**: `expose` controls whether the dataset is listed in the DCAT catalogue and served by the API, while `dataspace.expose` controls whether it is offered into the dataspace as an EDC asset. `expose` is tri-state — unset falls back to `dataspace.expose` so pre-split files keep their behaviour

Extended blocks for DCAT-AP 3.0 and dataspace integration:

`dcat:` block — propagated to the DCAT-AP catalogue by dataset-api:
- `publisher_uri` — overrides the API-level fallback publisher
- `themes` — EU Publications Office data-theme URIs
- `language_uris` — dct:language URIs
- `spatial_uris` — dct:spatial URIs
- `accrual_periodicity` — dct:accrualPeriodicity URI
- `conforms_to` — dct:conformsTo URI
- `temporal.start` / `temporal.end` — dct:temporal coverage

`dataspace:` block — consumed when registering datasets in EDC:
- `expose` — offer the dataset into the dataspace
- `contract_required` — enables `ds:contractRequired` ODRL constraint
- `consent_required` — enables `ds:consentStatus` ODRL constraint and consent-based row filtering
- `odrl_action` — default ODRL action (default `use`)
- `purpose` — ODRL purpose values
- `medallion` — data quality level (gold / silver / bronze)

`ontology:` block — which mapping spec says what the columns mean: `spec` (a shared mapping published in `celine-ontologies`) or `spec_file` (a path relative to the governance file). Exactly one, enforced by the schema.

Governance rules are resolved with pattern matching via `GovernanceResolver`: exact key first, then the longest matching glob, then `defaults` alone. The chosen rule is overlaid on the defaults using the fields the file **explicitly set** — not truthiness — so `expose: false` withdraws a dataset instead of being silently dropped. `tags` and `dataspace.purpose` union, `consent_required` and `contract_required` OR, `ownership` / `row_filters` / `ontology` replace wholesale. See [the format reference](https://celine-eu.github.io/projects/celine-utils/docs/governance) for the full table.

`celine.governance` is the single parser: `dataset-api`, `ds` and `celine-superset` import it rather than reimplementing it. EDC-specific sub-objects in the `dataspace:` block belong to `ds` and are ignored here via `model_config = ConfigDict(extra="ignore")` rather than rejected.

### Pipeline orchestration

Structured execution layer for:

- Meltano ingestion pipelines
- dbt transformations, tests, and seeds (`dbt_seed` wrapper)
- Prefect-based Python flows (Prefect 3.x)

The `PipelineRunner` coordinates execution, logging, error handling, and lineage emission consistently across tools. Pipeline run environment variables can be injected via `pipeline_run_envs` for local runtime configuration.

See the [pipeline tutorial](https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial).

### OpenLineage integration

- Automatic emission of START, COMPLETE, FAIL, and ABORT events
- Dataset-level schema facets
- Data quality assertions from dbt tests
- Custom CELINE governance facets (including `row_filters`, `medallion`, `classification`)

### Dataset tooling

The `DatasetClient` enables:

- Schema and table introspection
- Column metadata inspection
- Safe query construction
- Export to Pandas

### Platform integrations

- MQTT pipeline run events, published through `celine-sdk`
- Keycloak client-credentials tokens, for authenticating to a protected Marquez

Keycloak and Superset **administration** was removed in 3.0.0 along with the
`celine-utils admin` command tree — it was a replicable-setup tool with no remaining
callers. Provisioning lives in `celine-policies`.

---

## CLI

```bash
celine-utils governance generate marquez --app <app>   # scaffold governance.yaml from Marquez
celine-utils pipeline init app <name>                  # scaffold a new pipeline app
celine-utils pipeline run (meltano | dbt | prefect)    # run a pipeline stage
```

Full reference: [CLI](https://celine-eu.github.io/projects/celine-utils/docs/cli).

---

## Repository structure

```
src/celine/
  governance/        the thin core — three dependencies, imported by other repositories
  utils/
    cli/
    common/
    datasets/
    pipelines/
schema/              JSON Schemas — published, and symlinked into the package
docs/
tests/
integration-tests/
```

---

## Configuration

Environment-driven via `pydantic-settings`:

- Environment variables first
- Optional `.env` files
- Typed validation with container-friendly defaults

---

## Documentation

Two tracks — see the [documentation index](https://celine-eu.github.io/projects/celine-utils/docs/).

**Governance as a library**

- [The governance library](https://celine-eu.github.io/projects/celine-utils/docs/governance-library) — `celine.governance` API, the three-dependency contract, merging, validation
- [`governance.yaml` format](https://celine-eu.github.io/projects/celine-utils/docs/governance) — the grammar, resolution, the two exposure gates
- [`owners.yaml` registry](https://celine-eu.github.io/projects/celine-utils/docs/governance-owners) — owner aliases to canonical identities
- [Schemas](https://celine-eu.github.io/projects/celine-utils/docs/schemas) — the three published JSON Schemas

**Building and running pipelines**

- [Pipeline Tutorial](https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial) — end-to-end pipeline setup guide
- [CLI](https://celine-eu.github.io/projects/celine-utils/docs/cli) — full CLI reference
- [Environment](https://celine-eu.github.io/projects/celine-utils/docs/environment) — every variable and its default

**Why choices were made** — [Decisions](https://celine-eu.github.io/projects/celine-utils/docs/decisions/).

---

## Installation

```bash
uv add celine-utils                    # governance only — three dependencies
uv add "celine-utils[pipelines]"       # dbt / Meltano / Prefect / lineage
uv add "celine-utils[all]"             # pipelines + the typed OpenLineage facet
```

Parsing `governance.yaml` needs no extras. That is deliberate and enforced by CI —
see [ADR-0001](https://celine-eu.github.io/projects/celine-utils/docs/decisions/ADR-0001-governance-is-a-thin-core).

---

## Intended audience

- Data engineers
- Platform engineers
- CELINE application developers

---

## License

Copyright © 2025 Spindox Labs

Licensed under the Apache License, Version 2.0.
