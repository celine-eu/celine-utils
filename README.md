# celine-tools

celine-tools is a collection of utilities used across the CELINE data platform.
It provides command-line tools, runtime helpers, and integration layers for Keycloak, Superset, data governance, dataset introspection, and pipeline lineage (dbt, Meltano, OpenLineage).

This repository is intended to be reused across applications deployed within CELINE pipelines and orchestrated by Prefect, Meltano, and dbt.

## Features

### 1. Command Line Interface

A unified CLI implemented with Typer:

```
celine
 ├── admin
 │    ├── keycloak
 │    └── setup
 └── governance
      └── generate
```

Includes admin and [governance](docs/governance.md) commands.  

### 2. Keycloak Integration Layer

Typed wrappers for KeycloakAdmin and KeycloakOpenID providing realm, client, group, and user management.

### 3. Superset Integration

SupersetClient handles authentication, listing databases, and registering connections.

### 4. Dataset Introspection and Querying

DatasetClient allows discovering schemas, fetching structures, querying tables, and exporting results to pandas.

### 5. Governance Framework

Pattern-based governance.yaml specification and resolver.  
See docs/governance.md.

### 6. Lineage Extraction (dbt & Meltano)

Automatic lineage extraction enriched with schema and governance facets and dbt test assertions.

### 7. Pipeline Runner

Orchestrates Meltano and dbt execution, emits OpenLineage events, and integrates governance rules.

### 8. Configuration System

Environment-based typed settings via pydantic-settings.

### 9. MQTT Utility

Reusable MQTT client wrapper with reconnects and simple publish/subscribe API.

## Folder Structure

```
celine/
  admin/
  cli/
  common/
  datasets/
  pipelines/
  tests/
  docs/
```

## Installation

`pip install celine-utils`

## License

Copyright >=2025 Spindox Labs

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
