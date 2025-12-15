# CELINE Pipelines Tutorial

This document provides a **complete, end-to-end guide** to setting up, running, and deploying a **CELINE pipeline application**.
It consolidates all prior sections into a **single reference file**.


## Overview

A CELINE pipeline is a **self-contained application** that combines:

- Meltano (ingestion / bronze)
- dbt (staging, silver, gold)
- Optional Python / Prefect flows
- Governance configuration
- OpenLineage emission (optional)
- Container-first execution

Pipelines are designed to run **identically** in:
- Local development
- CI
- Kubernetes


## Creating a New Pipeline Application

CELINE provides a CLI command to **scaffold a complete pipeline application** with sane defaults.

From the root of your pipelines repository:

```bash
celine-cli pipeline init app demo_app
```

This command creates a fully functional example pipeline with:

- Meltano project
- dbt project structure
- Example Prefect-compatible flow
- Governance configuration
- `.env` file
- README

You can safely use the generated structure as-is and evolve it incrementally.


## Canonical Repository Layout

After initialization, your repository will look like:

```
pipelines/
└── apps/
    └── demo_app/
        ├── meltano/
        ├── dbt/
        ├── flows/
        ├── governance.yaml
        ├── .env
        └── README.md
```

Each folder under `apps/` represents a **deployable pipeline application**.


## Baseline Docker Image (Required)

All CELINE pipelines **must** be built on top of the official pipeline image:

```
ghcr.io/celine-eu/pipeline
```

This image already contains:
- Python + uv
- `celine-cli`
- Meltano
- dbt
- Prefect
- OpenLineage client


## Canonical Dockerfile (Per App)

Example Dockerfile for the `demo_app` pipeline:

```dockerfile
ARG BASE_TAG=latest
FROM ghcr.io/celine-eu/pipeline:${BASE_TAG}

ARG APP_NAME

ENV APP_NAME=${APP_NAME}

# Enable / disable OpenLineage support
ENV OPENLINEAGE_ENABLED=false
ENV OPENLINEAGE_URL=http://marquez-api:5001
ENV OPENLINEAGE_NAMESPACE=${APP_NAME}

ENV PIPELINES_ROOT=${PIPELINES_ROOT:-/pipelines}
ENV BASE_DIR="${PIPELINES_ROOT}/apps"
ENV APP_PATH="${BASE_DIR}/${APP_NAME}"

ENV MELTANO_PROJECT_ROOT="${APP_PATH}/meltano"
ENV DBT_PROJECT_DIR="${APP_PATH}/dbt"
ENV DBT_PROFILES_DIR="${DBT_PROJECT_DIR}"

WORKDIR /pipelines

COPY ./apps/${APP_NAME} /pipelines/apps/${APP_NAME}

RUN uv sync

RUN if [ -f "${APP_PATH}/requirements.txt" ]; then       uv add --requirements "${APP_PATH}/requirements.txt";     fi

RUN if [ -d "${MELTANO_PROJECT_ROOT}" ]; then       cd "${MELTANO_PROJECT_ROOT}" &&       rm -rf .meltano &&       MELTANO_PROJECT_ROOT=$(pwd) meltano install ;     fi

RUN if [ -d "${DBT_PROJECT_DIR}" ]; then       cd "${DBT_PROJECT_DIR}" &&       rm -rf target dbt_packages .dbt &&       DBT_PROFILES_DIR=$(pwd) dbt deps ;     fi

WORKDIR ${APP_PATH}
```

### Build Example

```bash
docker build   --build-arg APP_NAME=demo_app   -t demo-app-pipeline:latest   .
```


## Environment Configuration

CELINE uses **environment-driven configuration**.

Minimal `.env`:

```env
APP_NAME=demo_app

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=datasets
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

OPENLINEAGE_URL=http://marquez-api:5000
```

Resolution order:
- Environment variables
- `.env` files
- Defaults


## Meltano (Ingestion / Bronze)

Configure ingestion in `meltano/meltano.yml`.

Example job:

```yaml
jobs:
  - name: import
    tasks:
      - tap-my-source target-postgres
```

Run manually:

```bash
celine-cli pipeline run meltano "run import"
```

CELINE automatically:
- Executes Meltano
- Discovers datasets
- Emits lineage events (if enabled)
- Applies governance metadata


## dbt (Staging / Silver / Gold)

CELINE follows a medallion-style convention:

| Layer   | Command |
|-------|---------|
| Staging | `celine-cli pipeline run dbt staging` |
| Silver  | `celine-cli pipeline run dbt silver` |
| Gold    | `celine-cli pipeline run dbt gold` |
| Tests   | `celine-cli pipeline run dbt test` |

During execution, CELINE:

- Collects schema metadata
- Captures dbt test results
- Emits dataset-level lineage


## Governance Configuration

Each pipeline includes a `governance.yaml`.

Example:

```yaml
defaults:
  access_level: internal
  classification: green
  retention_days: 365

sources:
  datasets.ds.gold_*:
    license: CC0-1.0
    ownership:
      - name: data-team
        type: DATA_OWNER
    tags: [gold]
```

Governance is:

- Pattern-based
- Automatically resolved
- Emitted as a custom OpenLineage facet

Generate interactively:

```bash
celine-cli governance generate marquez --app demo_app
```

**Note** Since governance are openlineage facets, disabling openlineage using `OPENLINEAGE_ENABLED=false` will exclude the governance tracking capabilities.

## Python / Prefect Flows

Example `flows/pipeline.py`:

```python
from prefect import flow
from celine.pipelines.pipeline import (
    meltano_run_import,
    dbt_run_staging,
    dbt_run_silver,
    dbt_run_gold,
)

@flow
def medallion_flow():
    meltano_run_import()
    dbt_run_staging()
    dbt_run_silver()
    dbt_run_gold()
```


## Local Prefect Setup

Start a local Prefect server:

```bash
prefect server start
```

Set API URL:

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
```


## Example `prefect.yaml` (Local)

```yaml
name: demo-app
deployments:
  - name: demo-app-flow
    description: "Demo app medallion pipeline"
    entrypoint: flows/pipeline.py:medallion_flow

    schedule:
      cron: "0 * * * *"

    work_pool:
      name: local-process
      job_variables:
        env:
          APP_NAME: demo_app
          PIPELINES_ROOT: /pipelines
          POSTGRES_HOST: localhost
          POSTGRES_DB: datasets
```

Register the deployment:

```bash
prefect deploy --prefect-file prefect.yaml
```

Run it:

```bash
prefect deployment run demo-app/demo-app-flow
```


## Kubernetes & Production Deployment

In Kubernetes, Prefect deployments are **registered once** and executed later by workers.

CELINE provides a reference infrastructure repository at https://github.com/celine-eu/infra 

It includes:

- Prefect Server
- Prefect Workers
- Marquez / OpenLineage
- PostgreSQL
- Keycloak
- Helm charts
- Minikube-based local setup
- Other CELINE specific services, that can be omitted customizing the helmfiles

### Local Kubernetes (Minikube)

```bash
minikube start
kubectl create namespace celine
```

Follow the infra repository README to deploy the full stack.


## Recommended Workflow

| Stage | Tool |
|-----|------|
| Local development | Prefect local server |
| Image build | `ghcr.io/celine-eu/pipeline` |
| Flow authoring | `flows/*.py` |
| Deployment config | `prefect.yaml` |
| Local execution | `local-process` work pool |
| Production | CELINE infra Helm charts |


## Key Takeaways

- Use `celine-cli pipeline init` to bootstrap pipelines
- One Docker image per pipeline app
- Always use `ghcr.io/celine-eu/pipeline`
- Governance is declarative and automatic
- Prefect deployments are configuration, not code
- Use CELINE infra for production


## Need help?

Open an issue or submit a pull request to propose improvements. Commercial support is also available.
