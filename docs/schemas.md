# JSON Schemas

Three JSON Schemas are published from this repository. They are the machine-readable
contracts for the governance grammar, the owner registry, and the custom OpenLineage
facet that carries governance into lineage.

| Schema | Describes | Validated by |
|---|---|---|
| [`governance.schema.json`](https://celine-eu.github.io/schema/governance.schema.json) | a `governance.yaml` document | `celine.governance.validate` / `validate_file` |
| [`owners.schema.json`](https://celine-eu.github.io/schema/owners.schema.json) | an `owners.yaml` registry | `celine.governance.validate_owners` / `validate_owners_file` |
| [`GovernanceDatasetFacet.schema.json`](https://celine-eu.github.io/schema/GovernanceDatasetFacet.schema.json) | the custom OpenLineage dataset facet | referenced by `_schemaURL` in every emitted event |

They are not documentation of the models — they are executed. Until validation
existed, `governance.schema.json` was published and read by nobody, which let the
schema and the models drift apart while both failed open.

---

## Where they live, and why that is fixed

The files are tracked at `schema/` in the repository root and **must stay there**.
The documentation site serves that directory directly, which makes
`https://celine-eu.github.io/schema/GovernanceDatasetFacet.schema.json` the live
location of the `_schemaURL` already embedded in every OpenLineage event sitting in
Marquez.

They are also runtime artifacts, because `celine.governance.validation` executes
them, and `importlib.resources` can only read files inside a package. So
`src/celine/governance/schema` is a **symlink** to `../../../schema`: one
authoritative copy, reachable both ways.

Do not replace the symlink with a real directory — that recreates the duplicate it
exists to avoid, and the copies then drift. See
[ADR-0002](decisions/ADR-0002-schemas-stay-at-the-repository-root.md).

---

## `governance.schema.json`

Root: `defaults` and `sources`, both optional, both holding *governance blocks*.
Field-by-field meaning is in [the format reference](governance.md).

**`governanceBlock` permits additional properties, deliberately.** Seventeen
governance files had never been schema-checked when validation arrived, and setting
`additionalProperties: false` would have made adopting the package a
seventeen-file cleanup discovered at import time. The consequence is the trap worth
knowing:

> A misspelled key — `access_levl: open` — **passes schema validation**, is dropped
> by the model, and the dataset silently takes the default.

`celine.governance.unknown_keys` catches what the schema cannot, and `validate`
reports it as a warning. Pass `strict=True` to make it fatal.

The sub-objects are stricter. `dcatConfig` and `ontologyConfig` set
`additionalProperties: false`, and `ontologyConfig` carries a `oneOf` enforcing
`spec` **xor** `spec_file`. `dataspaceConfig` permits extra keys on purpose, because
`ds` carries its EDC-specific sub-objects there.

`access_level` still accepts `secret` for compatibility with deployed files, though
the Python enum has three values and normalises `secret` to `restricted`.

## `owners.schema.json`

`additionalProperties: false` at every level, `type` constrained to an enum of
Schema.org CURIEs, and `id`, `type`, `name` all required. Validation is strict with
no lenient mode — see [the owner registry](governance-owners.md#validation-is-strict-here).

The schema cannot express alias uniqueness across entries; the registry resolves a
contested alias to its first claimant and logs a warning naming both.

## `GovernanceDatasetFacet.schema.json`

The projection of a resolved governance rule onto an OpenLineage dataset facet,
produced by `celine.governance.build_facet`. Keys are camelCase — `accessLevel`,
`retentionDays`, `documentationUrl`, `rowFilters` — and every field except
`_producer` and `_schemaURL` is optional.

Absent values are **omitted rather than emitted as `null`**: a facet saying
`"license": null` claims the dataset has no licence, while omitting the key says
nothing about it.

---

## Validating against them

```python
from pathlib import Path
from celine.governance import validate_file, validate_owners_file, load_schema

validate_file(Path("governance.yaml"))            # warns on unknown keys
validate_file(Path("governance.yaml"), strict=True)
validate_owners_file(Path("owners.yaml"))         # always strict

load_schema("governance.schema.json")             # the schema itself, as a dict
```

Schemas are read from the installed package, so this works from a wheel and inside a
container. Nothing walks up from `__file__`.

---

## Changing a schema

- **The `_schemaURL` is a contract, not a configuration.** Changing it does not break
  a build; it silently invalidates every historical lineage event that carries the
  old value.
- **Prefer a new file to a breaking edit.** Consumers validate emitted events against
  the published URL, including events emitted by versions you no longer control.
- **This repository owns these schemas**, and other repositories validate against
  them. A schema change must be **released** before any downstream repository can
  consume it.
- Adding a field means adding it in three places — the JSON Schema, the pydantic
  model, and `KNOWN_KEYS`. Omitting the third makes the field parse into `extra` and
  read as permanently absent, with the schema still validating the file and nothing
  reporting the problem.
