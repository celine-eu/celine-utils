# Environment reference

Everything in `celine-utils` is configured by environment variable. Values are read
from the process environment and, where present, from `.env`, `.env.dev` and
`.env.prod` in the working directory — in that order, with the process environment
winning.

Unknown variables are ignored rather than rejected, so a typo in a variable name is
silent: the setting simply keeps its default. Check the values you expect with
`celine-utils pipeline run envs` before assuming they took effect.

> **The defaults are development defaults.** `securepassword123`,
> `host.docker.internal` — the credentials below have working defaults so a local
> stack starts without configuration. None of them is safe in a deployed
> environment, and nothing warns you when one is still in place.

---

## `POSTGRES_*` — one set of names, two sets of defaults

The same five variables are read by two different configuration classes with
**different defaults**:

| Variable | `PipelineConfig` (pipeline execution) | `PostgresConfig` (`DatasetClient`) |
|---|---|---|
| `POSTGRES_HOST` | `host.docker.internal` | `datasets-db` |
| `POSTGRES_PORT` | `15432` | `5432` |
| `POSTGRES_DB` | `datasets` | `datasets` |
| `POSTGRES_USER` | `postgres` | `postgres` |
| `POSTGRES_PASSWORD` | `securepassword123` | *(unset)* |

So code that reaches the database through `DatasetClient` and code that runs a
pipeline stage disagree about where the database is whenever the variables are
unset — the first assumes a container on a compose network, the second a host port
forwarded to a developer's machine. **Set all five explicitly.** Relying on either
default set means the behaviour changes with which class happens to read it.

---

## Pipeline execution

| Variable | Default | Meaning |
|---|---|---|
| `APP_NAME` | *(inferred)* | Overrides the app name derived from the directory |
| `PIPELINES_ROOT` | `./` | Root of the pipelines monorepo; governance discovery and app paths resolve from it |
| `RAISE_ON_FAILURE` | `true` | Raise on a failed task, failing the whole pipeline |
| `MELTANO_PROJECT_ROOT` | *(discovered)* | Overrides the Meltano project directory |
| `MELTANO_DATABASE_URI` | a local Postgres URI | Meltano's own state backend |
| `DBT_PROJECT_DIR` | *(discovered)* | Overrides the dbt project directory |
| `DBT_PROFILES_DIR` | *(discovered)* | Overrides the dbt profiles directory |
| `DBT_SCHEMA` | `public` | Schema written into a scaffolded app's dbt profile |
| `PREFECT_MODE` | `dev` | Anything other than `dev` disables development behaviour |

When `MELTANO_PROJECT_ROOT`, `DBT_PROJECT_DIR` and `DBT_PROFILES_DIR` are unset, the
runner discovers them by walking upward from the working directory looking for
`meltano/`, `dbt/` or `flows/`. Running from the wrong directory therefore does not
fail — it finds a different app, or none.

```bash
# Print the resolved configuration as export lines
celine-utils pipeline run envs

# Load it into the current shell
source <(celine-utils pipeline run envs)
```

---

## Governance

| Variable | Default | Meaning |
|---|---|---|
| `GOVERNANCE_CONFIG_PATH` | *(unset)* | Absolute path to `governance.yaml`; wins over every other discovery step |
| `GOVERNANCE_OVERLAY_NAME` | *(unset)* | Name of the deployer overlay — loads `governance.<name>.yaml` beside the base file |

A `GOVERNANCE_CONFIG_PATH` pointing at a nonexistent file logs a warning and falls
through to the remaining discovery steps rather than failing. If no file is found
anywhere, resolution returns empty rules and the pipeline runs with no governance
metadata — successfully. See
[discovery order](governance.md#where-the-file-lives).

---

## OpenLineage

| Variable | Default | Meaning |
|---|---|---|
| `OPENLINEAGE_ENABLED` | `true` | Emit lineage events |
| `OPENLINEAGE_URL` | `http://host.docker.internal:5003` | Marquez / OpenLineage endpoint |
| `OPENLINEAGE_API_KEY` | *(unset)* | Bearer token for that endpoint |
| `OPENLINEAGE_NAMESPACE` | *(unset)* | Namespace used by `governance generate marquez` |
| `BASE_NAMESPACE` | `""` | Prefix applied to emitted dataset namespaces |

---

## Keycloak

Used by `governance generate marquez` to obtain a client-credentials token when
Marquez requires authentication. If `KEYCLOAK_CLIENT_ID` or `KEYCLOAK_CLIENT_SECRET`
is empty, the request is made unauthenticated.

| Variable | Default | Meaning |
|---|---|---|
| `KEYCLOAK_URL` | `http://keycloak:8080` | Keycloak base URL |
| `KEYCLOAK_REALM` | `celine` | Realm holding the client |
| `KEYCLOAK_CLIENT_ID` | `""` | Service client id |
| `KEYCLOAK_CLIENT_SECRET` | `""` | Service client secret |
| `KEYCLOAK_VERIFY` | `true` | Verify TLS certificates |

Some Keycloak versions need an `/auth/` suffix on `KEYCLOAK_URL`.

The token endpoint is derived as
`{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token`. A rejected
request raises rather than falling back to an unauthenticated call — a 401 here means
misconfigured credentials, and continuing would surface it much later as a confusing
403 from Marquez.

Platform administration (`KEYCLOAK_ADMIN_*`, `SUPERSET_*`) was removed in 3.0.0
together with the `celine-utils admin` command tree. Keycloak provisioning lives in
`celine-policies`.

---

## MQTT — pipeline run events

Published through `celine-sdk`, and configured with its `CELINE_*` names:

| Variable | Meaning |
|---|---|
| `MQTT_EVENTS_ENABLED` | Master switch for pipeline event publishing (default `true`) |
| `CELINE_MQTT_HOST`, `CELINE_MQTT_PORT`, `CELINE_MQTT_USE_TLS`, … | Broker connection, read by `celine-sdk` |
| `CELINE_OIDC_BASE_URL`, `CELINE_OIDC_CLIENT_ID`, `CELINE_OIDC_CLIENT_SECRET` | OIDC client credentials for authenticating to the broker |

Events are published to `celine/pipelines/runs/{namespace}`. Every failure in this
path is logged and swallowed — a broker that is down or misconfigured never fails a
pipeline, and never announces itself either.

There used to be a second, unrelated MQTT surface here reading `MQTT_HOST`,
`MQTT_USER` and friends. It was removed in 3.0.0: nothing imported it, and its
variables looked like they configured the events above while configuring nothing.

---

## Logging

| Variable | Default | Applies to |
|---|---|---|
| `LOG_LEVEL` | `INFO` | the root logger, and every logger not named `celine.*` |
| `CELINE_LOG_LEVEL` | `DEBUG` | loggers named `celine.*` |

`CELINE_LOG_LEVEL` defaults to `DEBUG` while `LOG_LEVEL` defaults to `INFO`, so
CELINE components are more verbose than their surroundings unless told otherwise.

An unrecognised level name is **not** an error: it falls back to `INFO`. So
`LOG_LEVEL=debug` works (the value is upper-cased) but `LOG_LEVEL=verbose` silently
gives you `INFO`.

Output is colourised only when stdout is a TTY; redirected to a file or a log
collector it switches to a timestamped format carrying the logger name.

---

## Development

| Variable | Default | Meaning |
|---|---|---|
| `DEBUGGER` | *(unset)* | Truthy starts a `debugpy` listener before the CLI runs |
| `DEBUGGER_WAIT` | *(unset)* | Truthy blocks until a debugger attaches |
| `DEBUGGER_HOST` | `127.0.0.1` | Listener address |
| `DEBUGGER_PORT` | `5678` | Listener port |

`debugpy` ships in the `dev` dependency group only. When `DEBUGGER` is set and
`debugpy` is not installed, the CLI reports it on stderr and runs anyway.

The listener binds to loopback by default, deliberately: it accepts arbitrary code
execution from whoever connects. Setting `DEBUGGER_HOST=0.0.0.0` — needed to debug
inside a container — exposes that to the network.

---

## A starting point

`.env.example` in the repository root holds a working local configuration. Copy it to
`.env` and change the credentials.
