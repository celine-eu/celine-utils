# celine-tools

`celine-tools` is a collection of utilities used across the CELINE data platform.  
It provides command-line tooling, runtime helpers, data governance resolution, dataset introspection, and pipeline lineage instrumentation for `dbt`, `Meltano`, and `OpenLineage`.

This repository is intended to be embedded within applications deployed inside CELINE pipelines and orchestrated via Prefect, Meltano, and dbt.

---

# 🚀 Features Overview

## 1. Command Line Interface (CLI)

A unified CLI implemented with Typer:

```
celine
 ├── admin
 │    ├── keycloak
 │    └── setup
 ├── governance
 │    └── generate
 └── pipeline
      ├── run
      └── init
```

Key features include:

- Administrative helpers (Keycloak, Superset)
- Governance spec generation (docs/governance.md)
- Dataset metadata and querying tools
- Pipeline execution utilities for Meltano/dbt
- Pipeline application scaffolding (`celine pipeline init app`)

---

## 2. Keycloak Integration Layer

Typed wrappers for `KeycloakAdmin` and `KeycloakOpenID`, providing:

- realm and user administration  
- client secret resolution  
- automatic token handling  

---

## 3. Superset Integration

`SupersetClient` provides authenticated access to:

- list and create connections  
- manage datasets  
- integrate metadata into lineage and governance flows  

---

## 4. Dataset Introspection and Querying

`DatasetClient` supports:

- schema introspection  
- querying datasets  
- injection‑safe filtering  
- export to pandas  

---

## 5. Governance Framework

Pattern-based `governance.yaml` specification and resolver:
- governance rules via pattern matching  
- OpenLineage facet enrichment  
- dbt assertion propagation  

---

## 6. Lineage Extraction (dbt & Meltano)

Automatic lineage extraction enriched with:
- schema metadata  
- governance patterns  
- dbt test results  

---

## 7. Pipeline Runner

The `PipelineRunner` orchestrates:

- `meltano run`
- `dbt run`, `dbt test`, `dbt run-operation`
- streaming logs  
- OpenLineage emission  
- governance enforcement  

---

## 8. Pipeline Application Scaffolding

`celine pipeline init app <name>` generates a fully structured pipeline application:

```
<app_name>/
  meltano/
    meltano.yml        # env-based config (${POSTGRES_HOST}, etc.)
  dbt/
    dbt_project.yml
    profiles.yml       # uses env_var('VAR') for dynamic config
    models/
    tests/
    macros/
  flows/
    pipeline.py
  .env
  README.md
```

A non-interactive, templated setup for:
- Meltano  
- dbt  
- Python orchestration flows  

---

## 9. Configuration System

Based on `pydantic-settings`:

- environment variable driven  
- `.env` / `.env.local` resolution  
- container-friendly defaults  

---

## 10. MQTT Utility

Simple MQTT client wrapper:
- safe reconnects  
- pub/sub helpers  
- structured logging  

---

# 📁 Folder Structure

```
celine/
  admin/
  cli/
    commands/
  common/
  datasets/
  pipelines/
  docs/
```

---

# 📦 Installation

```
pip install celine-utils
```

---

# 📝 License

Copyright >=2025 Spindox Labs

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
