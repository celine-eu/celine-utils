# `governance.yaml` — format reference

`governance.yaml` declares, per dataset or dataset pattern, who owns the data, how
sensitive it is, under what terms it may be used, and through which channels it is
exposed.

This page is the **grammar**: every field, what it means, and how two blocks combine.
For the Python API that reads it, see [the governance library](governance-library.md).
For the owner aliases the `ownership` field refers to, see
[the owner registry](governance-owners.md).

One parser reads this format — `celine.governance` — and every CELINE component
imports it. Before it there were four independently written parsers that disagreed
about what the same file meant; see
[ADR-0001](decisions/ADR-0001-governance-is-a-thin-core.md).

---

## Where the file lives

For a pipeline application named `<app_name>`:

```text
PIPELINES_ROOT/
└── apps/
    └── <app_name>/
        └── governance.yaml
```

Discovery order, as implemented by `GovernanceResolver.auto_discover`:

1. `GOVERNANCE_CONFIG_PATH` — absolute path, wins over everything.
2. `PIPELINES_ROOT/apps/<app_name>/governance.yaml`
3. `<project_dir>/../governance.yaml` — for a dbt or Meltano project directory.
4. Nothing found → an empty configuration, and every dataset resolves to empty
   defaults. This is not an error and produces no failure; it produces datasets
   with no governance metadata.

---

## Shape

Two top-level sections, both optional:

```yaml
defaults:            # baseline applied to every dataset
  <field>: <value>

sources:             # per-dataset or per-pattern overrides
  <dataset-name-or-glob>:
    <field>: <value>
```

A `sources` entry may also nest its fields under a `governance:` key; the parser
accepts both spellings and treats them identically.

---

## Resolution

`resolve("db.schema.table")` picks exactly one `sources` entry, then overlays it on
`defaults`:

1. **Exact key match** in `sources`.
2. Otherwise **glob match**, and where several patterns match, **the longest
   pattern wins** — a more specific glob is a more deliberate statement than a
   broad one. Matching is case-sensitive on every host (`fnmatchcase`), so a
   pattern does not change meaning between Linux and macOS.
3. Otherwise `defaults` alone.

Only one source entry ever applies. Patterns do not accumulate: given
`datasets.*` and `datasets.gold.*`, a gold dataset takes the second and inherits
nothing from the first except through `defaults`.

### How a field combines with the baseline

The overlay is driven by **what the file actually said**, not by what the value is.
A field the override never mentions leaves the baseline intact; a field it mentions
takes effect even when the value is `false`, `null` or empty.

This matters most for withdrawal. `expose: false` in an override means *withdraw
this dataset*, and it works — under an earlier truthiness-based merge it dumped to
nothing and the baseline's `expose: true` survived, leaving datasets published that
a file had explicitly retracted.

Fields whose combination is **not** "override wins":

| Field | Rule |
|---|---|
| `tags` | **union** — an overlay adds keywords, it does not retract them |
| `ownership` | whole replacement when non-empty; a partial owner list is not a meaningful statement |
| `row_filters` | whole replacement when non-empty — filters are independent gates, and interleaving two lists by position would build a filter neither file declared |
| `ontology` | whole replacement — `spec` and `spec_file` are alternatives, and a field-wise overlay could produce a block declaring both |
| `dataspace.purpose` | **union** |
| `dataspace.consent_required`, `dataspace.contract_required` | **OR** — once required, a file layered on top may tighten but never loosen |
| `dataspace.expose` | ordinary override rule, deliberately **not** OR — OR would mean *once offered, always offered* |
| `dcat` | field-wise, recursively |
| `extra` | dict merge, override wins per key |

### Deployer overlays

A second file beside the first states what differs in one environment:

```text
apps/<app>/governance.yaml          # the base
apps/<app>/governance.prod.yaml     # the overlay
```

Loaded with `GovernanceResolver.from_file_with_override`. The overlay name comes
from the caller's argument, else `GOVERNANCE_OVERLAY_NAME`, else — only when the
caller passes `infer_from_dir=True` — the name of the directory holding the file.
Overlay blocks combine by the same rules as above, so an overlay can withdraw as
well as add.

---

## Exposure — two gates, not one

`expose` and `dataspace.expose` answer different questions, and they are **AND**:

| Field | Question | Gates |
|---|---|---|
| `expose` | Is the dataset listed in the catalogue and served by the API? | `/catalogue*`, `/query` |
| `dataspace.expose` | Is it *offered into the dataspace*? | requests arriving with EDR context — a negotiated contract, a third party |

Until these were split there was only `dataspace.expose`, and the exporter copied it
onto the catalogue flag. One boolean answered both questions, so a dataset that
merely had to appear in a dashboard was thereby offered into the dataspace.
See [ADR-0003](decisions/ADR-0003-two-exposure-gates.md).

**`expose` is tri-state.** Unset is not `false`:

```yaml
expose: true      # listed in the catalogue
expose: false     # withheld from the catalogue
# unset          -> falls back to dataspace.expose
```

The fallback is what makes every file written against the old grammar keep its
current behaviour exactly. Once a file states `expose`, the fallback stops applying
to it.

**Offered but unlisted is a contradiction, not a narrow grant.** A consumer reaches
a dataspace asset through the catalogue entry describing it, so:

```yaml
expose: false
dataspace:
  expose: true    # <- reported as a conflict
```

is reported by `exposure_conflict` rather than resolved silently. Resolving it
either way would be a security-relevant surprise: granting publishes data the
catalogue never advertised, withholding drops an offer someone deliberately made.

---

## Fields

### Identity and description

| Field | Type | Meaning |
|---|---|---|
| `title` | string | Human-readable dataset title, surfaced in the catalogue |
| `description` | string | Human-readable description |
| `source_system` | string | Origin system or domain — `openweathermap`, `copernicus`, `dwd` |
| `documentation_url` | string | Link to human documentation |
| `tags` | string[] | Free-form labels for discovery and grouping |

### Terms of use

| Field | Type | Meaning |
|---|---|---|
| `license` | string | Licence identifier — `CC-BY-NC-4.0`, `ODbL-1.0`, `proprietary` |
| `attribution` | string | Attribution text the licence requires. Surface it wherever the dataset is exposed |
| `ownership` | list | Owners — see below |
| `retention_days` | integer | Retention period in days |

`ownership` accepts either a full form or bare names:

```yaml
ownership:
  - name: spxl
    type: DATA_OWNER
  - rec            # equivalent to {name: rec, type: OWNER}
```

`name` is an alias resolved through [the owner registry](governance-owners.md).
`type` is semantic and not constrained by the schema.

### Access and sensitivity

| Field | Values | Meaning |
|---|---|---|
| `access_level` | `open`, `internal`, `restricted` | **Intended** exposure. Expresses intent, not enforcement |
| `access_requirements` | `all`, `partner`, `contract` | Precondition before access can be granted |
| `classification` | `green`, `yellow`, `red`, `pii` | **Intrinsic** sensitivity. Informs handling; grants and denies nothing |

`access_level: secret` is still accepted by the schema for compatibility with
deployed files and is **normalised to `restricted`** by `normalize_access_level`.
There are three levels, not four; do not write `secret` in a new file.

Classification and access level are independent axes. A `green` dataset can be
`restricted`, and a `pii` dataset that is `internal` is a statement about who may
see it, not about how careful the handling must be.

### Row filtering

```yaml
row_filters:
  - handler: user_column
    args:
      column: user_id
```

A list of independent gates. Each names a `handler` — the filter strategy — and
`args` for it. Used for per-subject and consent-based filtering.

`user_filter_column` is the **legacy** single-column form, carried only for
backward compatibility with deployed `ds` files. `row_filters` supersedes it: a bare
column name cannot say how a subject maps to values in it. Where both appear,
`row_filters` wins. No CELINE governance file uses the legacy field.

### `dcat:` — catalogue metadata

Propagated into the DCAT-AP 3.0 catalogue by `dataset-api`. Unknown keys are
**rejected** by the schema for this block.

| Field | Meaning |
|---|---|
| `publisher_uri` | Overrides the API-level fallback publisher |
| `themes` | EU Publications Office data-theme URIs |
| `language_uris` | `dct:language` URIs |
| `spatial_uris` | `dct:spatial` URIs |
| `accrual_periodicity` | `dct:accrualPeriodicity` URI from the EU Authority Table |
| `conforms_to` | URI of a standard the payload conforms to |
| `temporal.start` / `temporal.end` | `dct:temporal` coverage |

### `ontology:` — semantic model binding

Which mapping spec says what the columns *mean*. Exactly one of the two, enforced
by the schema — two bindings for one dataset is two answers to the same question:

```yaml
ontology:
  spec: obs_rec_energy          # a shared mapping published in celine-ontologies
# or
ontology:
  spec_file: ./mapping.yaml     # a path relative to this governance.yaml
```

Use `spec` for a mapping that recurs across producers (meter readings, forecasts) —
restating it per dataset makes one fact many. Use `spec_file` for a dataset whose
shape is its own: a spec names source *columns*, and a spec living in another
repository goes stale on a rename with nothing to detect it.

Distinct from `dcat.conforms_to`, deliberately. `conforms_to` names the *model*, an
IRI a consumer can compare across datasets; `ontology` names the *mapping onto these
columns*. Several datasets can conform to one model through different mappings.

Resolution happens in the consumer (`dataset-api`), not in this package.

### `dataspace:` — exposure and ODRL policy

| Field | Type | Meaning |
|---|---|---|
| `expose` | bool | Offer this dataset into the dataspace. See the two gates above |
| `medallion` | `bronze`, `silver`, `gold` | Data quality level |
| `contract_required` | bool | Emit the `ds:contractRequired` ODRL constraint |
| `consent_required` | bool | Emit the `ds:consentStatus` ODRL constraint and enable consent-based row filtering |
| `odrl_action` | string | Default ODRL action, default `use` |
| `purpose` | string[] | ODRL purpose values |

This block permits additional keys. The EDC-specific sub-objects (`asset`,
`data_address`, `contract`) belong to `ds` and are carried in its own subclass;
`celine.governance` ignores them rather than rejecting the file.

---

## Unknown keys

The grammar defines a fixed key set. A key outside it is **not** an error: it is
collected into `extra`, so a consumer can still see what a file said, and reported
as a warning by `validate`.

This is the trap the format cannot protect you from by itself. `access_levl: open`
passes schema validation — `governanceBlock` permits additional properties — and the
model discards it, so the dataset silently takes the default. Run validation and
read the warnings:

```python
from pathlib import Path
from celine.governance import validate_file

validate_file(Path("governance.yaml"))                 # warns on unknown keys
validate_file(Path("governance.yaml"), strict=True)    # raises on them
```

See [validation](governance-library.md#validation) for why the default is lenient.

---

## Worked example

```yaml
defaults:
  access_level: internal
  access_requirements: partner
  classification: green
  retention_days: 365
  source_system: openweathermap
  documentation_url: https://example.org/datasets/docs

sources:
  # Everything gold is catalogued.
  datasets.ds_prod_gold.*:
    expose: true

  # One dataset is also offered into the dataspace, under contract.
  datasets.ds_prod_gold.weather_hourly:
    title: Hourly weather observations
    license: CC-BY-NC-4.0
    attribution: >
      Weather data derived from OpenWeatherMap One Call API 3.0 © OpenWeather Ltd.
    ownership:
      - name: spxl
        type: DATA_OWNER
    access_level: restricted
    access_requirements: contract
    tags: [gold, weather]
    expose: true
    dcat:
      themes:
        - http://publications.europa.eu/resource/authority/data-theme/ENVI
      accrual_periodicity: http://publications.europa.eu/resource/authority/frequency/HOURLY
    ontology:
      spec: obs_rec_energy
    dataspace:
      expose: true
      medallion: gold
      contract_required: true
      purpose: [research]

  # Raw data is catalogued for internal use and never offered outward.
  datasets.ds_prod_raw.*:
    classification: red
    access_level: restricted
    expose: false
```

Resolving `datasets.ds_prod_gold.weather_hourly` matches the exact key rather than
the `*` pattern, then overlays it on `defaults` — so `retention_days: 365` and
`source_system: openweathermap` are inherited while `access_level` is overridden.

---

## How governance reaches lineage

During pipeline execution the runner resolves each dataset name against the file and
emits the result as a custom OpenLineage dataset facet — for inputs, outputs and dbt
test datasets alike.

Absent values are **omitted** from the facet, never emitted as `null`: a facet saying
`"license": null` claims the dataset has no licence, while omitting the key says
nothing about it, and only the second is what silence means.

The facet's field names are camelCase and its schema is published; see
[schemas](schemas.md).

---

## Practice

- Put the repetition in `defaults` and the exceptions in `sources`.
- Prefer a schema-level glob to a per-dataset entry where the statement is
  genuinely about the schema.
- State `expose` explicitly in new files rather than relying on the fallback.
- Validate in CI. Seventeen files went unchecked before validation existed, and the
  failure mode is silent.
- Version the file with the code that produces the datasets.
