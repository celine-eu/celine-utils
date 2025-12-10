# CELINE Governance Metadata — Specification and Workflow

## Overview

The governance layer in CELINE enriches lineage and datasets with formal metadata describing:

- Ownership
- Licensing
- Access level and sensitivity
- Data classification
- Tags and retention policies
- Documentation URLs
- Source system and provenance

This metadata:

1. Lives in version-controlled YAML alongside the pipelines.
2. Is injected into OpenLineage events generated during Meltano and dbt pipelines via celine-utils.
3. Is exported and consumed by the Dataset API.
4. Is visible to end users and curators in Dataset API views.

Governance is intended to be declarative, fully reproducible, pipeline-driven, and integrated with lineage.

---

## 1. Governance in Pipelines

Governance is defined per application in:

```
PIPELINES_ROOT/
  apps/
    <app-name>/
      governance.yaml
```

`celine-utils` loads this file at runtime inside:

- Meltano orchestrations
- dbt transformations
- Prefect tasks that emit OpenLineage

### Processing Steps

1. The governance resolver locates `governance.yaml`.
2. It loads global defaults and pattern-based dataset rules.
3. For each dataset produced or consumed during the pipeline, the resolver selects:
   - exact match
   - schema wildcard
   - namespace or prefix wildcard
   - or defaults if nothing matches
4. Governance metadata is attached to each OpenLineage dataset event as a custom dataset facet.

### OpenLineage Facet Example

```
{
  "governance": {
    "license": "ODbL-1.0",
    "ownership": [{"name": "GIS Team", "type": "DATA_OWNER"}],
    "access_level": "internal",
    "classification": "green",
    "tags": ["osm", "geodata"],
    "retention_days": 365,
    "documentation_url": "https://docs.example",
    "source_system": "openstreetmap"
  }
}
```

This uses a custom OpenLineage Dataset Facet, which is valid under OpenLineage specifications.

---

## 2. Governance CLI Generator

The CLI tool generates or bootstraps `governance.yaml` for a given application based on datasets registered in Marquez.

Command:

```
celine governance generate marquez --app <app>
```

Environment variables:

| Variable | Description |
|---------|-------------|
| OPENLINEAGE_URL | Marquez API base URL |
| OPENLINEAGE_NAMESPACE | Lineage namespace |
| PIPELINES_ROOT | Determines default output path |

### Non-interactive Mode

```
celine governance generate marquez --app osm --yes
```

Generates:

- Defaults section populated
- One empty `sources:` entry per dataset

Example output:

```
defaults:
  license: null
  ownership: []
  access_level: internal
  classification: green
  tags: []
  retention_days: 365
  documentation_url: null
  source_system: null

sources:
  datasets.ds_dev_raw.openstreetmap_fi_lapperanta: {}
  datasets.ds_dev_silver.openstreetmap_it_alpecimbra_base: {}
```

### Interactive Mode

```
celine governance generate marquez --app osm
```

For each dataset the wizard prompts for:

- Matching rule (exact, schema wildcard, prefix wildcard)
- License (common presets or custom)
- Access level
- Classification
- Owner
- Tags

Example generated entry:

```
sources:
  datasets.ds_dev_raw.*:
    license: ODbL-1.0
    ownership:
      - name: GIS Team
        type: DATA_OWNER
    access_level: internal
    classification: green
    tags: ["osm", "raw"]
```

---

## 3. governance.yaml Format and Schema

Each file contains two top-level sections:

```
defaults:
sources:
```

### Defaults Section

Applies to any dataset without a matching rule.

```
defaults:
  license: null
  ownership: []
  access_level: internal
  classification: green
  tags: []
  retention_days: 365
  documentation_url: null
  source_system: null
```

### Sources Section

Pattern-to-metadata mapping.

```
sources:
  <pattern>:
    license: <string>
    ownership:
      - name: <string>
        type: DATA_OWNER | STEWARD | PRODUCER
    access_level: internal | public | restricted | secret
    classification: green | yellow | red
    tags:
      - <string>
    retention_days: <int>
    documentation_url: <URL or null>
    source_system: <string>
```

Supported patterns:

| Type | Example | Meaning |
|------|----------|---------|
| exact | datasets.ds_dev_raw.table1 | Matches only this table |
| schema wildcard | datasets.ds_dev_raw.* | All tables in schema |
| prefix wildcard | datasets.* | All datasets under prefix |

Matching priority:

1. Exact
2. Schema wildcard
3. Prefix wildcard
4. Defaults

---

## 4. Dataset API Consumption

The Dataset API:

1. Exports lineage and datasets from Marquez.
2. Loads governance metadata.
3. Stores enriched metadata in catalog tables.
4. Exposes governance in dataset detail and list views.

Used for:

- Documentation
- Compliance and classification
- Access control logic (future)
- Ownership visibility
- User-interface labels and metadata badges

Governance travels end-to-end:

```
governance.yaml
→ celine-utils OpenLineage emission
→ Marquez lineage storage
→ dataset-export CLI
→ Dataset API catalog
→ UI and consumer APIs
```

---

## 5. Schemas and Validation

Governance fields follow this structure:

```
license: string|null
ownership: list[{name: string, type: string}]
access_level: string|null
classification: string|null
tags: list[string]
retention_days: int|null
documentation_url: string|null
source_system: string|null
```

Values may be arbitrary unless restricted by internal policies.

---

## 6. Test Coverage

Tests validate:

- URL and namespace resolution
- Non-interactive generation
- Output path override
- Marquez API failure behavior
- Empty dataset namespace handling

---

This document defines how governance metadata flows through CELINE pipelines, lineage, and API layers, and how governance.yaml is created and interpreted.
