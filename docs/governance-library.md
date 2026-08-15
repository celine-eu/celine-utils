# `celine.governance` — the library

`celine.governance` is the one implementation of `governance.yaml` parsing, merging
and validation. Every CELINE component that reads a governance file imports it:
`dataset-api`, `ds`, `celine-superset` and the pipeline runner in this repository.

This page is for **consumers importing the package**. For the file format itself see
[the format reference](governance.md); for the CLI see [CLI](cli.md).

---

## The dependency contract

> Runtime dependencies are `pydantic`, `pyyaml` and `jsonschema`. Nothing else, ever.

This is the feature, not a style preference. The package exists because
`celine-utils` once required dbt, Meltano, Prefect and Keycloak in order to parse a
YAML file — so no API service could import it, four teams wrote their own parser
instead, and the four then disagreed about what the same file meant.

`celine.governance` must not import `celine.utils`, SQLAlchemy, OpenLineage, or
anything from an optional extra. If a governance module needs something from the fat
side, the dependency goes the other way round: `celine.utils` may import
`celine.governance`, never the reverse.

A CI job asserts this rather than trusting review — adding one import line is easy
and nothing in a diff makes it look expensive. See
[ADR-0001](decisions/ADR-0001-governance-is-a-thin-core.md).

### Installing just the core

```bash
uv add celine-utils          # core only — three dependencies
```

The extras are opt-in and none of them are needed to read governance:

| Extra | Brings | For |
|---|---|---|
| *(none)* | pydantic, pyyaml, jsonschema | parsing governance |
| `openlineage` | `openlineage-python` | the typed `GovernanceDatasetFacet` class |
| `pipelines` | dbt, Meltano, Prefect, pandas, SQLAlchemy, … | running pipelines |
| `admin` | python-keycloak, bcrypt, … | Keycloak / Superset administration |
| `all` | everything above | the pre-2.0 dependency set |

`build_facet` returns a plain `dict` and needs none of them, including
`openlineage`.

### Import path

The package ships from the `celine-utils` wheel today and is expected to become its
own distribution. The import path `celine.governance` was chosen so that move is a
directory rename with no consumer editing an import. Do not refer to it as
`celine.utils.governance` — that path does not exist, and creating it would make the
eventual split a breaking change for every downstream repository.

---

## Reading a file

```python
from pathlib import Path
from celine.governance import GovernanceResolver

resolver = GovernanceResolver.from_file(Path("governance.yaml"))
rule = resolver.resolve("datasets.ds_prod_gold.weather_hourly")

rule.access_level      # 'restricted'
rule.tags              # ['gold', 'weather']
```

A missing file is **not** an error: `from_file` logs a warning and returns a resolver
over an empty configuration, so every dataset resolves to empty defaults. Check the
path yourself if absence should fail.

### Constructors

| Call | Use |
|---|---|
| `GovernanceResolver.from_file(path)` | a known path |
| `GovernanceResolver.from_dict(raw)` | a document already parsed, e.g. from an API response |
| `GovernanceResolver.from_file_with_override(base, overlay_name=None, *, infer_from_dir=False)` | base plus a deployer overlay beside it |
| `GovernanceResolver.auto_discover(app_name=None, project_dir=None)` | convention-based lookup — see [discovery order](governance.md#where-the-file-lives) |

`infer_from_dir` is opt-in rather than default because the two callers this
consolidated genuinely disagreed: one inferred the overlay name from the parent
directory, the other returned the base unchanged. Neither default would have been
correct for both, so each passes what it means.

### `parse_rule`

`parse_rule(block)` builds a single `GovernanceRule` from one raw block, accepting
either a bare block or one nested under a `governance:` key.

It goes through `model_validate` on a dict containing only the keys the block
actually declared. **That is load-bearing.** Pydantic records those keys in
`model_fields_set`, and the whole merge layer reads it to tell *unset* from *set to a
falsy value*. Constructing a `GovernanceRule` with keyword arguments marks every
field as set, which silently degrades merging to "override always wins" and makes
`expose: false` inexpressible. Build rules with `parse_rule` or `model_validate`,
never with kwargs, if the result will be merged.

---

## Merging

```python
from celine.governance import merge_rules, merge_configs

resolved = merge_rules(base_rule, override_rule)
config   = merge_configs(base_config, overlay_config)
```

| Function | Combines |
|---|---|
| `merge_rules(base, override)` | two `GovernanceRule`s, with the per-field rules below |
| `merge_configs(base, override)` | two whole documents — defaults with defaults, sources rule-wise, overlay-only sources added as-is |
| `merge_dataspace(base, override)` | two `DataspaceConfig`s |
| `merge_models(base, override, cls)` | the generic `exclude_unset` overlay |

The overlay uses `exclude_unset` — not `exclude_defaults`, and never truthiness.
All three drop a field the source never mentioned; they differ on a field the source
*did* mention whose value equals the default. `exclude_defaults` drops it, so it
cannot tell *silent* from *said no*. Truthiness cannot express *off* at all once the
base says *on*.

The full per-field table is in
[the format reference](governance.md#how-a-field-combines-with-the-baseline).

---

## Validation

```python
from pathlib import Path
from celine.governance import validate_file, validate, GovernanceValidationError

unknown = validate_file(Path("governance.yaml"))              # warns
unknown = validate_file(Path("governance.yaml"), strict=True) # raises
```

| Function | Behaviour |
|---|---|
| `validate(data, *, source, strict=False)` | schema violations **raise**; unknown keys **warn**, or raise under `strict`. Returns the unknown keys |
| `validate_file(path, *, strict=False)` | the same, loading YAML from a path |
| `schema_errors(data, schema_name)` | every violation as a list of strings, sorted by document position. Raises nothing |
| `unknown_keys(data)` | keys the grammar does not define, per block |
| `load_schema(name)` | a packaged JSON Schema as a dict |
| `validate_owners(data)` / `validate_owners_file(path)` | strict always — see [owners](governance-owners.md) |

`schema_errors` reports **every** error rather than the first: a governance file is
edited by hand, and one problem per run turns a five-minute fix into five round
trips.

**Why unknown keys warn by default.** Seventeen governance files had never been
schema-checked when validation arrived. Making unknown keys fatal on day one would
have turned adopting this package into a seventeen-file cleanup discovered at import
time — in an exporter, in CI, at the worst possible moment. Warn, fix, then flip the
default.

Schemas are read from the installed package via `importlib.resources`, never by
walking from `__file__`, so validation works from a wheel and inside a container.

---

## Exposure gates

```python
from celine.governance import effective_expose, dataspace_expose, exposure_conflict

effective_expose(rule)    # listed in the catalogue and served by the API?
dataspace_expose(rule)    # offered into the dataspace?

if (why := exposure_conflict(rule)) is not None:
    log.error("%s: %s", name, why)
```

`effective_expose` falls back to `dataspace.expose` when `expose` is unstated;
`dataspace_expose` has no fallback in either direction. `exposure_conflict` returns a
sentence or `None` — it is **reported rather than raised** so a caller can collect
every conflict in a run instead of failing on the first.

Never read `rule.expose` directly to decide catalogue visibility: it is tri-state,
and `None` does not mean `False`. See
[the two gates](governance.md#exposure--two-gates-not-one).

---

## Building the OpenLineage facet

```python
from celine.governance import build_facet, is_empty, SCHEMA_URL

if not is_empty(rule):
    facet = build_facet(rule, producer="https://github.com/celine-eu/celine-utils")
```

Returns a plain `dict` with camelCase keys and imports nothing from OpenLineage, so a
consumer gets the projection without the dependency. Absent values are omitted rather
than emitted as `null`.

`include_dataspace` defaults to `True`, which matches the catalogue — the consumer
that reads those fields back out. The lineage extractors historically did **not**
project the dataspace block, so events already in Marquez carry no dataspace fields;
an extractor adopting this function should pass `include_dataspace=False` to keep
emitting what it emits today. Widening the payload is a deliberate change, not a side
effect of deduplication.

`SCHEMA_URL` is a **contract, not a configuration**. It is embedded in every
OpenLineage event already sitting in Marquez, so changing it does not break a build —
it silently invalidates historical lineage.

---

## Levels

```python
from celine.governance import normalize_access_level, normalize_classification

normalize_access_level("SECRET")   # -> 'restricted'
normalize_classification("PII")    # -> 'pii'
```

Both accept `None` and return `None`. Both raise `ValueError` on a value outside the
enum. `secret` is folded to `restricted` for compatibility with deployed files; the
enums are `GovernanceAccessLevel` (open / internal / restricted), `DataClassification`
(green / yellow / red / pii) and `AccessRequirement` (all / partner / contract).

---

## Models

`GovernanceRule` is the resolved block; `GovernanceConfig` holds `defaults` and
`sources`. Sub-models: `DcatConfig`, `OntologyConfig`, `DataspaceConfig`,
`TemporalCoverage`, `GovernanceOwner`.

Every model sets `extra="ignore"`, so a file carrying fields this package does not
know — the EDC sub-objects `ds` adds, a new field from a newer release — parses
rather than failing. Unknown keys at block level are preserved in `rule.extra`.

`KNOWN_KEYS` is the frozenset deciding which keys reach the model and which land in
`extra`. **Adding a field to `GovernanceRule` without adding it to `KNOWN_KEYS`
makes that field read as permanently absent** — the key parses into `extra`, the
schema still validates the file, and nothing reports it. That is exactly how the
`ontology` block failed on introduction.

---

## Extending the models

`ds` extends `GovernanceRule` with ODRL policy and a richer dataspace spec. Subclass
rather than adding fields here: what belongs in this package is the surface *every*
consumer shares. A field only one component reads is a field the other three carry
without meaning.
