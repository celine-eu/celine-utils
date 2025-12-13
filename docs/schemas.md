# Governance Schemas

This folder contains **JSON Schema definitions** used by CELINE to formally describe
custom metadata structures exchanged across the platform.

The schemas are primarily intended for **OpenLineage custom facets**, validation,
and interoperability between pipeline components, tooling, and external consumers.

---

## Purpose

The schemas in this folder serve as:

- **Authoritative contracts** for custom OpenLineage facets
- **Validation targets** for emitted lineage events
- **Documentation artifacts** describing governance and metadata semantics
- **Stable references** via `_schemaURL` fields in OpenLineage facets

They ensure that lineage, governance, and metadata extensions remain consistent,
machine-readable, and evolvable over time.

---

## Typical Contents

You will usually find schemas defining:

- Dataset-level custom facets (e.g. governance, ownership, classification)
- Field-level or schema-level metadata extensions
- CELINE-specific extensions that are not part of the OpenLineage core specification

Each schema is written in **JSON Schema** format and is designed to be compatible
with OpenLineage custom facet requirements.

---

## Usage

Schemas in this folder are typically referenced by:

- Custom OpenLineage facets via the `_schemaURL` attribute
- Validation tooling during pipeline execution or CI
- Documentation and downstream metadata consumers

Example reference from a custom facet:

```json
{
  "_schemaURL": "https://raw.githubusercontent.com/celine-eu/celine-utils/main/schemas/GovernanceDatasetFacet.json"
}
```

---

## Design Principles

- **Backward compatible** whenever possible
- **Explicit and typed** fields
- **Stable URLs** once published
- **No runtime logic** — schemas are declarative only

Breaking changes should result in a **new schema file**, not a modification of an
existing one.

---

## Relationship to the Platform

These schemas are part of the broader CELINE tooling ecosystem and are consumed by
pipeline runners, lineage emitters, governance resolvers, and external cataloging
systems.

---

## Notes

- Do not embed secrets or environment-specific values in schemas
- Keep descriptions concise but precise
- Prefer clarity over cleverness

For implementation details, refer to the corresponding facet or resolver code that
uses each schema.
