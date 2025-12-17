# Dataset Governance

This document describes how **dataset governance** is defined, configured, and applied in CELINE pipelines using the `governance.yaml` file and the CELINE CLI.

Governance metadata is used to enrich **OpenLineage events**, provide a single source of truth for licensing, attribution, ownership, access intent, and data sensitivity, and to enable future integration with **dataspace and contract-based access models**.

---

## What Is `governance.yaml`

`governance.yaml` is a **declarative configuration file** that defines governance rules for datasets produced or consumed by a pipeline.

It allows you to specify, per dataset or dataset pattern:

- License
- Attribution (mandatory where required by license)
- Ownership
- Access level (exposure intent)
- Access requirements (preconditions such as contracts or partnerships)
- Classification / sensitivity
- Tags
- Retention policy
- Documentation and source system

These rules are resolved at runtime and injected into lineage events as **custom OpenLineage dataset facets**.

---

## Where the File Lives

For a pipeline application named `<app_name>`, the expected location is:

```
PIPELINES_ROOT/
└── apps/
    └── <app_name>/
        └── governance.yaml
```

The file is automatically discovered by CELINE tooling at runtime.

You can override discovery using:

```
GOVERNANCE_CONFIG_PATH=/absolute/path/to/governance.yaml
```

---

## File Structure

A `governance.yaml` file has two top-level sections:

- `defaults`: applied to all datasets unless overridden
- `sources`: dataset-specific or pattern-based rules

---

## Example `governance.yaml`

```yaml
defaults:
  license: null
  attribution: null
  ownership: []
  access_level: internal
  access_requirements: partner
  classification: green
  tags: []
  retention_days: 365
  documentation_url: https://example.com/datasets/docs
  source_system: "integration-tests"

sources:
  datasets.ds.gold.weather_hourly:
    license: CC-BY-NC-4.0
    attribution: >
      Weather data derived from OpenWeatherMap One Call API 3.0 © OpenWeather Ltd.
    ownership:
      - name: Weather Team
        type: DATA_OWNER
    access_level: restricted
    access_requirements: contract
    classification: green
    tags: [gold, weather]

  datasets.ds.raw.weather_events:
    license: proprietary
    ownership:
      - name: Internal Platform
        type: DATA_OWNER
    access_level: restricted
    classification: pii
    tags: [raw, sensitive]
```

---

## Defaults Section

The `defaults` block defines baseline governance applied to **all datasets** unless overridden.

Typical use cases:
- Global access level
- Default access requirements
- Retention policy
- Shared documentation URL
- Default classification

Fields set to `null` are omitted unless overridden.

---

## Sources Section

The `sources` section defines governance rules for specific datasets or **patterns**.

### Dataset Keys

Keys correspond to **OpenLineage dataset names**, for example:

- `database.schema.table`
- `datasets.ds.gold.weather_hourly`
- `singer.tap-openweathermap.forecast_stream`

### Pattern Matching

Wildcard rules are supported using glob semantics:

```yaml
sources:
  datasets.ds.*:
    access_level: internal

  datasets.raw.*:
    classification: red
```

Resolution precedence:
1. Exact match
2. Longest matching wildcard
3. Defaults

---

## Governance Fields

### `license`
License identifier for the dataset (e.g. `CC-BY-NC-4.0`, `ODbL-1.0`, `proprietary`).

### `attribution`
Mandatory attribution text required by the dataset license.  
This text should be surfaced in catalogs, APIs, or documentation when datasets are exposed.

### `ownership`
List of owners responsible for the dataset.

```yaml
ownership:
  - name: Data Platform Team
    type: DATA_OWNER
```

### `access_level`
Defines the **intended exposure level** of the dataset.

Allowed values:
- `open` — publicly shareable
- `internal` — organization-wide access
- `restricted` — limited, explicitly authorized access

> Access level expresses **intent**, not enforcement.

### `access_requirements`
Defines **preconditions** that must be satisfied before access can be granted.

Allowed values:
- `all` — no precondition
- `partner` — ecosystem or organizational partner
- `contract` — explicit legal or data-sharing agreement

This field is designed to integrate with **dataspace and contract-based models** without binding to IAM or policy engines.

### `classification`
Describes the **intrinsic sensitivity** of the data.

Allowed values:
- `green` — non-sensitive
- `yellow` — potentially sensitive
- `red` — sensitive or regulated
- `pii` — personal data

Classification does **not** grant or deny access; it informs compliance and handling requirements.

### `tags`
Free-form labels used for discovery, grouping, or filtering.

### `retention_days`
Retention period in days.

### `documentation_url`
Link to human-readable documentation for the dataset.

### `source_system`
Origin system or domain (e.g. `openweathermap`, `copernicus`, `dwd`).

---

## How Governance Is Applied

During pipeline execution:

1. Dataset lineage is collected
2. Dataset names are resolved against `governance.yaml`
3. Defaults and overrides are merged
4. Governance metadata is emitted as a **custom OpenLineage dataset facet**

This applies to:
- Inputs
- Outputs
- dbt test datasets

---

## OpenLineage Integration

Governance metadata is published as a custom **GovernanceDatasetFacet**, including:

- License
- Attribution
- Access level
- Access requirements
- Classification
- Retention
- Source system

This allows downstream systems (catalogs, dataspaces, policy engines) to reason about datasets consistently.

---

## Interactive CLI Usage

CELINE provides an interactive CLI to generate governance files.

### Command

```bash
celine-utils governance generate marquez --app <app_name>
```

The CLI will:
1. Discover datasets from Marquez
2. Prompt for governance metadata per dataset
3. Allow pattern-based scoping
4. Write `governance.yaml` to the pipeline folder

### Non-Interactive Mode

```bash
celine-utils governance generate marquez --app <app_name> --yes
```

Generates a skeleton file using defaults.

---

## Best Practices

- Use defaults to minimize repetition
- Prefer wildcard rules for schema-level governance
- Keep dataset names stable
- Version governance files with code
- Treat governance as **declarative metadata**, not enforcement logic

---

## Summary

`governance.yaml` provides a single, declarative mechanism for defining dataset governance in CELINE pipelines.

It is:
- Pattern-based
- License- and attribution-aware
- Dataspace-ready
- Integrated with lineage
- CLI-assisted
