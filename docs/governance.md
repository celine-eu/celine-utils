# Governance Configuration

This document describes how **dataset governance** is defined, configured, and applied in CELINE pipelines using the `governance.yaml` file and the CELINE CLI.

Governance metadata is used to enrich **OpenLineage events**, enforce consistency, and provide a single source of truth for ownership, licensing, access control, and classification.

---

## What Is `governance.yaml`

`governance.yaml` is a **declarative configuration file** that defines governance rules for datasets produced or consumed by a pipeline.

It allows you to specify, per dataset or dataset pattern:

- License
- Ownership
- Access level
- Classification / sensitivity
- Tags
- Retention policy
- Documentation and source system

These rules are resolved at runtime and injected into lineage events as **custom OpenLineage facets**.

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
  ownership: []
  access_level: internal
  classification: green
  tags: []
  retention_days: 365
  documentation_url: https://example.com/dataset-test/docs
  source_system: "integration-tests"

sources:
  datasets.ds.gold_color_metrics:
    license: CC0-1.0
    ownership:
      - name: owner1
        type: DATA_OWNER
    access_level: internal
    classification: green
    tags:
      - gold
      - test

  datasets.ds.silver_normalized:
    license: ODbL-1.0
    ownership:
      - name: owner1
        type: DATA_OWNER
    access_level: internal
    classification: yellow
    tags:
      - silver
      - test

  datasets.ds.stg_raw:
    license: proprietary
    ownership:
      - name: company ltd
        type: DATA_OWNER
    access_level: restricted
    classification: red
    tags:
      - raw
      - test
      - secret_sauce

  datasets.raw.test:
    license: proprietary
    ownership:
      - name: company foo
        type: DATA_OWNER
    access_level: secret
    classification: red
    tags:
      - foo
      - test
      - raw

  singer.tap-test.test:
    license: proprietary
    ownership: []
    access_level: secret
    classification: red
    tags: []
```

---

## Defaults Section

The `defaults` block defines baseline governance applied to **all datasets** unless overridden.

Typical use cases:
- Global access level
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
- `datasets.ds.gold_color_metrics`
- `singer.tap-test.test`

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

| Field | Description |
|------|-------------|
| `license` | Dataset license identifier |
| `ownership` | List of owners (`name`, `type`) |
| `access_level` | `open`, `internal`, `restricted`, `secret` |
| `classification` | Sensitivity class (e.g. `green`, `yellow`, `red`) |
| `tags` | Free-form tags |
| `retention_days` | Retention period in days |
| `documentation_url` | Link to documentation |
| `source_system` | Origin system or domain |

---

## How Governance Is Applied

During pipeline execution:

- Dataset lineage is collected
- Dataset names are resolved against `governance.yaml`
- Defaults and overrides are merged
- Governance is emitted as a custom OpenLineage dataset facet

This applies to:
- Inputs
- Outputs
- dbt test datasets

---

## Interactive CLI Usage

CELINE provides an interactive CLI to generate governance files.

### Command

```bash
celine-cli governance generate marquez --app <app_name>
```

The CLI will:
1. Discover datasets from Marquez
2. Prompt for governance metadata per dataset
3. Allow pattern-based scoping
4. Write `governance.yaml` to the pipeline folder

### Non-Interactive Mode

For automation:

```bash
celine-cli governance generate marquez --app <app_name> --yes
```

This generates a skeleton file using defaults.

---

## Best Practices

- Use defaults to minimize repetition
- Prefer wildcard rules for schema-level governance
- Keep dataset names stable
- Version governance files with code
- Treat governance as configuration, not logic

---

## Summary

`governance.yaml` provides a single, declarative mechanism for defining dataset governance in CELINE pipelines.

It is:
- Pattern-based
- Automatically applied
- Integrated with lineage
- CLI-assisted
