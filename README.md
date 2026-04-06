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

- Dataset ownership (`owner`, `attribution`)
- License and access level (`open`, `internal`, `restricted`, `secret`)
- Data classification (`pii`, `green`, `yellow`, `red`) and retention
- Tags, documentation links, and source system
- `user_filter_column` — the column used for per-subject consent-based row filtering
- `expose: true` — controls whether the dataset appears in the DCAT catalogue and is registered as an EDC asset

Extended blocks for DCAT-AP 3.0 and dataspace integration:

`dcat:` block — propagated to the DCAT-AP catalogue by dataset-api:
- `publisher_uri` — overrides the API-level fallback publisher
- `themes` — EU Publications Office data-theme URIs
- `language_uris` — dct:language URIs
- `spatial_uris` — dct:spatial URIs
- `accrual_periodicity` — dct:accrualPeriodicity URI
- `conforms_to` — dct:conformsTo URI
- `temporal.start` / `temporal.end` — dct:temporal coverage

`dataspace:` block — consumed by `export_governance.py` when registering datasets in EDC:
- `contract_required` — enables `ds:contractRequired` ODRL constraint
- `consent_required` — enables `ds:consentStatus` ODRL constraint and consent-based row filtering
- `odrl_action` — default ODRL action (default `use`)
- `purpose` — ODRL purpose values
- `medallion` — data quality level (gold / silver / bronze)

Governance rules are resolved with pattern matching via `GovernanceResolver` — defaults cascade from the `defaults:` block into each source entry, with per-source values taking precedence. The `expose` and `dcat`/`dataspace` fields use an OR-merge for booleans and override-merge for objects.

Both `celine-utils` (pipeline side) and `dataset-api/cli/export_governance.py` (catalogue side) parse the same `governance.yaml` format. EDC-specific sub-objects in the `dataspace:` block are silently ignored by `celine-utils` via `model_config = ConfigDict(extra="ignore")`.

### Pipeline orchestration

Structured execution layer for:

- Meltano ingestion pipelines
- dbt transformations and tests
- Prefect-based Python flows

The `PipelineRunner` coordinates execution, logging, error handling, and lineage emission consistently across tools.

See the [pipeline tutorial](https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial).

### OpenLineage integration

- Automatic emission of START, COMPLETE, FAIL, and ABORT events
- Dataset-level schema facets
- Data quality assertions from dbt tests
- Custom CELINE governance facets (including `userFilterColumn`, `medallion`, `classification`)

### Dataset tooling

The `DatasetClient` enables:

- Schema and table introspection
- Column metadata inspection
- Safe query construction
- Export to Pandas

### Platform integrations

- Keycloak for identity and access management
- Apache Superset for analytics platform integration
- MQTT for lightweight messaging

---

## CLI

```bash
celine-utils governance generate   # generate governance.yaml template
celine-utils pipeline init         # scaffold a new pipeline
celine-utils pipeline run          # run a pipeline
```

---

## Repository structure

```
celine/
  admin/
  cli/
  common/
  datasets/
  pipelines/
schemas/
tests/
```

---

## Configuration

Environment-driven via `pydantic-settings`:

- Environment variables first
- Optional `.env` files
- Typed validation with container-friendly defaults

---

## Documentation

- [Pipeline Tutorial](https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial) — end-to-end pipeline setup guide
- [Governance](https://celine-eu.github.io/projects/celine-utils/docs/governance) — governance.yaml format, access levels, pattern matching, dcat/dataspace blocks
- [Schemas](https://celine-eu.github.io/projects/celine-utils/docs/schemas) — JSON Schema definitions including `governance.schema.json`
- [CLI](https://celine-eu.github.io/projects/celine-utils/docs/cli) — full CLI reference

---

## Installation

```bash
pip install celine-utils
```

---

## Intended audience

- Data engineers
- Platform engineers
- CELINE application developers

---

## License

Copyright © 2025 Spindox Labs

Licensed under the Apache License, Version 2.0.
