# CLI reference

```text
celine-utils
├── governance
│   ├── generate marquez
│   └── graph
└── pipeline
    ├── init app
    └── run  (envs | meltano | seed | dbt | build | prefect)
```

Installed as the `celine-utils` entry point (it needs the `pipelines` extra). The
governance checks have their own, thinner entry point — see
[`celine-governance validate`](#celine-governance-validate--the-gate-before-an-import). From a checkout of this repository,
`task cli -- <args>` runs the same CLI from the working tree.

**This page describes what each command is for and the traps in using it. For the
exact current options, ask the CLI:**

```bash
celine-utils <command> --help
```

An option table copied into a document is stale the next time an option is added,
with nothing to detect it. The tables below are orientation, not the source of truth.

---

## `celine-governance validate` — the gate before an import

A second entry point, `celine-governance`, ships beside `celine-utils` and runs on the
**thin core**: `pydantic`, `pyyaml` and `jsonschema`, no `pipelines` extra. It is the
command a repository, a CI job or a deployment task runs before anything imports a
catalogue.

```bash
celine-governance validate apps/                       # every governance.yaml under apps/
celine-governance validate apps/ ../deployment/apps/ \
    --overlay-dir ../deployment/governance \
    --owners ../deployment/owners.yaml --strict         # as a deployment will serve it
celine-governance validate apps/ --handlers my_plugin --format json
```

Each `governance.yaml` is **materialized** before it is checked: merged with its
deployer overlay `governance.<app>.yaml`, found beside it or in an `--overlay-dir`, by
the same resolver the exporter uses. An overlay alone is never the unit — it has no
`sources` of its own — so it is checked for types and unknown keys and then as part of
its base. One overlay per base: the same name in two places is an error.

| Rule | Finding |
|---|---|
| `schema` | each file against `governance.schema.json`; unknown keys warn |
| `row-policy` | an exposed, non-`secret`, non-`open` dataset that does not declare both layers, `person` and `organization`. `open` datasets relying on being open are listed as notes |
| `handler` | a handler that is neither built in nor named with `--handlers`; a built-in in the wrong layer |
| `layer-verdict` | `allow_all` and `deny_all` in one layer |
| `consent-coherence` | on dataspace offers: a person filter without `consent_required` or `pii`, and personal data without a person filter |
| `owners` | an owners file that is not strictly valid; an ownership name that resolves to no owner; with `--dataspace`, an owner of an organization-bound offer without a DID |
| `input` | a path matching no `governance.yaml`, a missing overlay directory, an overlay that applies to no checked base (a warning) |

Every finding is collected — never only the first. Exit **1** if any error remains
(`--strict`: or any warning), **0** otherwise, **2** on a usage error. Several
`--owners` files merge with later ones shadowing earlier by `id`, each validated
before it is merged, exactly as `celine-policies keycloak sync-orgs` loads them: a
partial per-environment owners file is refused — pass the materialized registry.

The same checks are a library call, `celine.governance.check.check_governance`, for a
loader that wants to refuse before it writes anything.

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

### `governance graph`

Show which pipelines feed which, resolved from the `depends_on` and `sources` blocks of
every `governance.yaml` matched.

```bash
celine-utils governance graph [PATHS...] [--format tree|order|json|mermaid]
                              [--schedules FILE] [--strict]
```

`PATHS` are shell globs naming pipelines or governance files. A directory contributes
the `governance.yaml` inside it, so `apps/*` and `apps/*/governance.yaml` are
equivalent; `governance.<name>.yaml` is a deployer overlay and is skipped with a note.
The default is `apps/*`.

```bash
# one repository
celine-utils governance graph 'apps/*'

# a deployment and the open-source pipelines together
celine-utils governance graph 'apps/*' '/path/to/deployment/pipelines/apps/*'

# the run order, flattened — what to run, in sequence
celine-utils governance graph 'apps/*' --format order

# a diagram, or something to feed a scheduler
celine-utils governance graph 'apps/*' --format mermaid
celine-utils governance graph 'apps/*' --format json
```

The tree output is a topological ordering: everything in tier *N* may run in parallel
once tier *N-1* is done. Findings and the summary go to **stderr**, so the graph itself
stays pipeable.

**Which trees you glob is a judgement, not a detail.** A deployment repository may hold
unmaintained copies of open-source apps, and including them reports every dataset those
copies declare as having two producers.

#### Findings

| Finding | Meaning |
|---|---|
| `unresolved` | No scanned pipeline produces the dataset and it is not marked `external: true` — a typo, or a tree you did not glob |
| `multiple-producers` | Two governance files declare the same dataset: two answers to who owns it, and an ambiguous producer for anything depending on it |
| `cycle` | No run order exists for the pipelines named |
| `self-dependency` | A pipeline depends on a dataset it also declares as its own output |
| `inactive-producer` | An active pipeline reads from one marked `active: false` — whatever it reads is as old as the last time that pipeline ran |
| `external-satisfied` | An entry marked `external: true` that a producer in this scan satisfies. Informational — it means a wider scan closed the graph |

`--strict` exits 1 when anything other than `external-satisfied` or
`schedule-unverified` is reported, which is the form for CI.

#### Checking the deployed schedules

Pass a deployment's scheduled flows and the crons are checked against the graph:

```bash
celine-utils governance graph 'apps/*' --schedules staging-flows.yaml
```

```yaml
flows:
  - name: weather
    path: /pipelines/apps/weather/flows/pipeline.py   # or: app: weather
    schedule:
      cron: "15 * * * *"
```

`app` may be given directly or derived from `path`, which is the shape a Prefect
deployment manifest already has. An entry with no cron is skipped — a flow triggered by
hand has no ordering to check.

| Finding | Meaning |
|---|---|
| `schedule-inversion` | Both fire hourly, never in the same minute, and the consumer is always earlier in the hour. It succeeds every time, on the previous run's output |
| `schedule-collision` | Producer and consumer can start in the same minute; which run the consumer sees depends on which starts first |
| `schedule-unverified` | The same two problems, on a pair where an app deploys several flows. Governance is per app and cannot say which flow produces which dataset, so the pairing may not be the one that moves the data. Advisory, and excluded from `--strict` |
| `not-deployed` | A pipeline marked active that no scheduled flow in this deployment runs |

**Schedules are not read from `governance.yaml`, deliberately.** A cron is a deployment
fact: one app runs several flows on independent schedules, and the same pipeline runs on
different schedules in different deployments. A governance file is one per app and is
shared by every deployment that installs it, so it is the wrong place for either.

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
| `pipeline run seed` | `dbt seed` in the app's `dbt/` |
| `pipeline run dbt <spec>` | One dbt stage in the app's `dbt/` — see the spec grammar below |
| `pipeline run build [select]` | `dbt build` — every model followed immediately by its own tests |
| `pipeline run prefect` | Load and execute a `@flow` function from `flows/` |

```bash
# Make the pipeline's own environment available to your shell
source <(celine-utils pipeline run envs)

celine-utils pipeline run meltano
celine-utils pipeline run meltano "run import --select my_stream"

celine-utils pipeline run seed
celine-utils pipeline run dbt staging
celine-utils pipeline run dbt "test -s tag:meters"
celine-utils pipeline run build silver

celine-utils pipeline run prefect
celine-utils pipeline run prefect --flow pipeline --function om_flow
```

#### The dbt spec grammar

`pipeline run dbt` takes **one string**, not a bare tag, and the same string that a
flow passes to `dbt_run()`. It may open with a dbt subcommand — `run`, `build`,
`test`, `seed`, `snapshot` — and defaults to `run` when it does not:

| Spec | Runs |
|---|---|
| `silver` | `dbt run --select silver` |
| `staging --exclude tag:meters` | `dbt run --select staging --exclude tag:meters` |
| `-s gold,tag:wind` | `dbt run -s gold,tag:wind` |
| `build -s silver` | `dbt build -s silver` |
| `test` | `dbt test` |
| `test -s tag:meters` | `dbt test -s tag:meters` |

`--select` is injected only when the spec names nodes without a selection flag of
its own. A consequence of the leading-subcommand rule: a model actually *named*
`run`, `build`, `test`, `seed` or `snapshot` is shadowed, and has to be named
through an explicit `-s`.

**Which verb.** `build` is the one to use when the point is to populate the
database: it interleaves each model's tests with the model, so a layer that
populated badly fails where it broke rather than several layers downstream. `run`
is for iterating on a single model, where the tests are noise until the model is
right.

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
