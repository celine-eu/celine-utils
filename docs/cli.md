# CLI reference

```text
celine-utils
├── governance
│   └── generate marquez
└── pipeline
    ├── init app
    └── run  (envs | meltano | dbt | prefect)
```

Installed as the `celine-utils` entry point. From a checkout of this repository,
`task cli -- <args>` runs the same CLI from the working tree.

**This page describes what each command is for and the traps in using it. For the
exact current options, ask the CLI:**

```bash
celine-utils <command> --help
```

An option table copied into a document is stale the next time an option is added,
with nothing to detect it. The tables below are orientation, not the source of truth.

---

## governance

### `governance generate marquez`

Generate a `governance.yaml` scaffold from the datasets Marquez already knows about,
so a governance file starts from what the pipeline actually produces rather than from
a blank page.

```bash
celine-utils governance generate marquez --app <app-name> [options]
```

| Option | Meaning |
|---|---|
| `--app` | CELINE app name. **Required** |
| `--output`, `-o` | Output path. Defaults to `PIPELINES_ROOT/apps/<app>/governance.yaml` |
| `--marquez` | Marquez base URL, overriding `OPENLINEAGE_URL` |
| `--namespace` | OpenLineage namespace, overriding `OPENLINEAGE_NAMESPACE` |
| `--yes`, `-y` | Non-interactive — write a skeleton using defaults |

Interactively it prompts per dataset for licence, access level, classification,
ownership and tags, and offers pattern scopes (exact name, schema wildcard, prefix
wildcard) so one answer can cover many datasets. With `--yes` it writes a skeleton
with an empty rule per discovered dataset.

```bash
# Interactive, against the configured Marquez
celine-utils governance generate marquez --app om

# Scaffold only, no prompts
celine-utils governance generate marquez --app om --yes

# Explicit endpoint, namespace and destination
celine-utils governance generate marquez \
  --app om \
  --marquez http://marquez.internal:5000 \
  --namespace ds_prod_silver \
  --output ./governance.yaml
```

The generated file is a starting point, not a finished one: it cannot know licence
terms or who owns the data. Review it, then [validate](governance-library.md#validation)
it.

Reads `OPENLINEAGE_URL`, `OPENLINEAGE_NAMESPACE`, `PIPELINES_ROOT`, and the
`KEYCLOAK_*` variables when Marquez requires authentication — see
[environment](environment.md).

---

## pipeline

### `pipeline init app`

Scaffold a new pipeline application.

```bash
celine-utils pipeline init app <app-name> [--force]
```

Produces:

```text
<app-name>/
├── meltano/
├── dbt/
├── flows/pipeline.py
├── .env
└── README.md
```

Templates are rendered with the app name substituted, and database values in `.env`
are populated from `POSTGRES_*` in the environment when present. The command aborts
if the target already looks like a CELINE pipeline app; `--force` overwrites it.

See [the pipeline tutorial](pipeline-tutorial.md) for what to do with the result.

### `pipeline run`

Executes one stage of a pipeline. Every subcommand discovers the app root by walking
upward from the current directory looking for `meltano/`, `dbt/` or `flows/`, so run
these from inside the app.

| Command | Does |
|---|---|
| `pipeline run envs` | Print the pipeline run environment as `export` lines |
| `pipeline run meltano [command]` | Run a Meltano command in the app's `meltano/`. Default: `run import` |
| `pipeline run dbt <tag>` | `dbt run` then `dbt test`, both `--select tag:<tag>`, in the app's `dbt/` |
| `pipeline run prefect` | Load and execute a `@flow` function from `flows/` |

```bash
# Make the pipeline's own environment available to your shell
source <(celine-utils pipeline run envs)

celine-utils pipeline run meltano
celine-utils pipeline run meltano "run import --select my_stream"

celine-utils pipeline run dbt staging
celine-utils pipeline run dbt gold

celine-utils pipeline run prefect
celine-utils pipeline run prefect --flow pipeline --function om_flow
```

`pipeline run prefect` auto-detects both the flow module and the decorated function
when `--flow` / `--function` are omitted. Pass them explicitly when a module holds
more than one flow.

Each run emits OpenLineage events and resolves governance for the datasets it
touches; see [governance](governance.md#how-governance-reaches-lineage).

---

## Configuration

Every command is configured by environment variables, loaded from the process
environment and from `.env`, `.env.dev`, `.env.prod` where present. The full list is
in [the environment reference](environment.md).
