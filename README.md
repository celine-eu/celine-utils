# CELINE Utils

**CELINE Utils** is a collection of shared utilities, libraries, and command-line tools that form the technical backbone of the **CELINE data platform**.

It provides reusable building blocks for data pipelines, governance, lineage, metadata management, and platform integrations. The repository is designed to be embedded into CELINE applications and executed within orchestrated environments using Meltano, dbt, Prefect, and OpenLineage.

---

## Scope and Goals

The goals of this repository are to:

- Centralize **cross-cutting platform logic** used by multiple CELINE projects
- Provide **opinionated but extensible** tooling for data pipelines
- Enforce **consistent governance and lineage semantics**
- Reduce duplication across pipeline applications
- Act as a stable foundation for CELINE-compatible services and workflows

This is not an end-user application; it is a **platform utility layer**.

---

## Key Capabilities

### Command Line Interface (CLI)

A unified CLI built with Typer exposes administrative, governance, and pipeline utilities:

```text
celine-utils
 ├── governance
 │    └── generate
 └── pipeline
      ├── init
      └── run
```

---

### Pipeline Orchestration

CELINE Utils provides a structured execution layer for:

- **Meltano** ingestion pipelines
- **dbt** transformations and tests
- **Prefect**-based Python flows

The `PipelineRunner` coordinates execution, logging, error handling, and lineage emission in a consistent way across tools.

See the [pipeline tutorial](docs/pipeline-tutorial.md) to discover how to setup and deploy a new pipeline.

---

### OpenLineage Integration

First-class OpenLineage support includes:

- Automatic emission of START, COMPLETE, FAIL, and ABORT events
- Dataset-level schema facets
- Data quality assertions from dbt tests
- Custom CELINE governance facets

---

### Governance Framework

A declarative `governance.yaml` specification allows you to define:

- Dataset ownership
- License and access level
- Classification and retention
- Tags and documentation links

Governance rules are resolved using pattern matching and injected into lineage events.

---

### Dataset Tooling

The `DatasetClient` enables:

- Schema and table introspection
- Column metadata inspection
- Safe query construction
- Export to Pandas

---

### Platform Integrations

Built-in integrations include:

- **Keycloak** for identity and access management
- **Apache Superset** for analytics platform integration
- **MQTT** for lightweight messaging

---

## Repository Structure

```text
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

Configuration is environment-driven using `pydantic-settings`:

- Environment variables first
- Optional `.env` files
- Typed validation
- Container-friendly defaults

---

## Installation

```bash
pip install celine-utils
```

---

## Intended Audience

CELINE Utils is intended for:

- Data engineers
- Platform engineers
- CELINE application developers

It is not a general-purpose data tooling library.

---

## License

Copyright © 2025  
Spindox Labs

Licensed under the Apache License, Version 2.0.
