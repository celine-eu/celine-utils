# CLI Reference

`celine-utils` provides a unified CLI for governance and pipeline management.

```
celine-utils
 ├── governance
 │    └── generate marquez
 └── pipeline
      ├── init app
      └── run (meltano | dbt | prefect)
```

---

## governance generate marquez

Generate a `governance.yaml` scaffold from datasets discovered in Marquez (OpenLineage backend).

```bash
celine-utils governance generate marquez --app <app-name> [options]
```

**Options:**

| Option | Description | Default |
|---|---|---|
| `--app` | CELINE app name (required) | — |
| `--output`, `-o` | Output path for `governance.yaml` | `$PIPELINES_ROOT/apps/<app>/governance.yaml` |
| `--marquez` | Marquez base URL | `$OPENLINEAGE_URL` or `http://localhost:5000` |
| `--namespace` | OpenLineage namespace | `$OPENLINEAGE_NAMESPACE` or auto-derived |
| `--yes`, `-y` | Non-interactive mode — use defaults for all datasets | `false` |

**Behavior:**

1. Fetches the list of datasets from Marquez in the specified namespace.
2. In interactive mode: prompts for license, access level, classification, ownership, and tags for each dataset. Suggests pattern scopes (exact, schema wildcard, prefix wildcard).
3. In `--yes` mode: writes a skeleton `governance.yaml` with empty rules for each discovered dataset.
4. Writes the output file to the resolved path.

**Usage examples:**

```bash
# Interactive mode — configure each dataset
celine-utils governance generate marquez --app om

# Non-interactive — scaffold only
celine-utils governance generate marquez --app om --yes

# Custom Marquez URL and output path
celine-utils governance generate marquez \
  --app om \
  --marquez http://marquez.internal:5000 \
  --output ./governance.yaml

# Custom namespace
celine-utils governance generate marquez \
  --app om \
  --namespace ds_prod_silver
```

**Environment variables:**

| Variable | Description |
|---|---|
| `OPENLINEAGE_URL` | Marquez base URL |
| `OPENLINEAGE_NAMESPACE` | Default namespace |
| `PIPELINES_ROOT` | Root of the pipelines monorepo (used for output path resolution) |
| `KEYCLOAK_*` | Keycloak client credentials for authenticated Marquez requests |

---

## pipeline init app

Scaffold a new CELINE pipeline application directory.

```bash
celine-utils pipeline init app <app-name> [options]
```

**Arguments:**

| Argument | Description |
|---|---|
| `app-name` | Name of the new pipeline application |

**Options:**

| Option | Description | Default |
|---|---|---|
| `--force`, `-f` | Overwrite if the folder already looks like a pipeline app | `false` |

**Resulting structure:**

```
<app-name>/
  meltano/
    meltano.yml
  dbt/
    dbt_project.yml
    profiles.yml
    models/
    tests/
    macros/
    seeds/
    snapshots/
    analyses/
  flows/
    pipeline.py
  .env
  README.md
```

Templates are rendered with `{{ app_name }}` substituted. Database connection values in `.env` are populated from environment variables (`POSTGRES_HOST`, `POSTGRES_USER`, etc.) if present.

**Usage examples:**

```bash
# Create a new pipeline app named "owm"
celine-utils pipeline init app owm

# Overwrite an existing scaffold
celine-utils pipeline init app owm --force
```

---

## pipeline run

Execute pipeline components: Meltano ingestion, dbt transformations, or Prefect flows.

### pipeline run meltano

```bash
celine-utils pipeline run meltano [command]
```

Runs a Meltano command inside the app's `meltano/` directory.

| Argument | Description | Default |
|---|---|---|
| `command` | Meltano command string | `run import` |

```bash
# Default: run the import tap
celine-utils pipeline run meltano

# Run a specific command
celine-utils pipeline run meltano "run import --select my_stream"
```

### pipeline run dbt

```bash
celine-utils pipeline run dbt <tag>
```

Runs `dbt run --select tag:<tag>` and `dbt test --select tag:<tag>` in the app's `dbt/` directory.

| Argument | Description |
|---|---|
| `tag` | dbt selector (e.g. `staging`, `silver`, `gold`, `test`) |

```bash
celine-utils pipeline run dbt staging
celine-utils pipeline run dbt gold
```

### pipeline run prefect

```bash
celine-utils pipeline run prefect [options]
```

Loads and executes a Prefect `@flow`-decorated function from the app's `flows/` directory.

| Option | Description | Default |
|---|---|---|
| `--flow`, `-f` | Name of `flows/<flow>.py` (without extension) | auto-detected |
| `--function`, `-x` | Function name inside the module | auto-detected via `@flow` decorator |

```bash
# Auto-detect flow file and function
celine-utils pipeline run prefect

# Specify explicitly
celine-utils pipeline run prefect --flow pipeline --function om_flow
```

**Environment variables used by `pipeline run`:**

| Variable | Description |
|---|---|
| `APP_NAME` | Override the inferred app name |
| `PIPELINES_ROOT` | Root of the pipelines monorepo |
| `MELTANO_PROJECT_ROOT` | Override Meltano project directory |
| `DBT_PROJECT_DIR` | Override dbt project directory |
| `DBT_PROFILES_DIR` | Override dbt profiles directory |

The runner auto-discovers the app root by walking upward from the current directory, looking for `meltano/`, `dbt/`, or `flows/` subdirectories.
