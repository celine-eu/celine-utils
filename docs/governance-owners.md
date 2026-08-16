# `owners.yaml` — the owner registry

A `governance.yaml` `ownership` block names owners by **short alias**:

```yaml
ownership:
  - name: spxl
    type: DATA_OWNER
```

`owners.yaml` is what turns `spxl` into a canonical, machine-readable identity — a
DID or a URL — plus the metadata the catalogue and Keycloak need. The indirection is
the point: an open-source pipeline can say `dso`, and each deployment's registry
decides who that is.

---

## Format

```yaml
owners:
  - id: spxl
    type: schema:Corporation
    name: Spindox Labs S.r.l.
    role: publisher
    did: did:web:spindoxlabs.example
    url: https://spindoxlabs.example
    aliases: [spindox, labs]
    organization:
      create: true
      role: technology-provider
      attributes:
        country: IT

  - id: rec
    type: schema:Project
    name: Renewable Energy Community
    url: https://rec.example
    aliases: [dso, community]
```

### Fields

| Field | Required | Meaning |
|---|---|---|
| `id` | yes | The lookup key — what a governance file's `ownership.name` says |
| `type` | yes | Schema.org type CURIE, emitted as the JSON-LD `@type` |
| `name` | yes | Display name, emitted as `foaf:name` in DCAT-AP output |
| `role` | no | The party's role **with respect to the data** — `publisher`, `controller`, … Declared but not yet consumed |
| `did` | no | `did:web:` URI, for owners operating a dataspace connector |
| `url` | no | Canonical homepage, emitted as `foaf:homepage`, and used as publisher URI when no DID is set |
| `aliases` | no | Alternative lookup keys resolving to this owner |
| `organization` | no | Keycloak provisioning block |

`type` is a closed enum: `schema:Organization` (generic fallback),
`schema:Corporation` (for-profit company), `schema:GovernmentOrganization` (public
authority), `schema:ResearchOrganization`, `schema:EducationalOrganization`,
`schema:NGO`, `schema:Project` (a consortium without a separate legal entity).

Schema.org is used because it is the most broadly understood vocabulary and aligns
with DCAT-AP's use of `foaf:Agent` for publishers; the types are emitted alongside
`foaf:Organization` in JSON-LD for full compatibility.

### `organization:` — Keycloak provisioning

Read by `celine-policies keycloak sync-orgs`. Entries with `create: true` are
provisioned as Keycloak organizations, `role` becomes the KC `attributes.type`, and
`attributes` are set alongside it.

```yaml
organization:
  create: true
  role: technology-provider
  attributes:
    country: IT
```

The key is also accepted as `organization_config`, which is the column name in the
identity-registry database and therefore the key in its API responses, while YAML
seed files say `organization`. One model reads both.

### Two fields named `role`, and they are not the same thing

- `role` at entry level — the party's role with respect to the **data**.
- `organization.role` — the **Keycloak** organization role.

Setting one does not set the other.

---

## Reading it

```python
from pathlib import Path
from celine.governance import load_owners_yaml

registry = load_owners_yaml(Path("owners.yaml"), validate=True)

registry.by_id("spindox")          # -> OwnerEntry(id='spxl', ...) via alias
registry.canonical_uri("spxl")     # -> 'did:web:spindoxlabs.example'
registry.by_uri("https://rec.example")
len(registry)                      # owners, not lookup keys
```

| Call | Returns |
|---|---|
| `by_id(key)` | the entry for an id, falling back to aliases |
| `by_uri(uri)` | the entry indexed by DID or URL — used by the DCAT formatter |
| `canonical_uri(key)` | DID if present, else URL, else `None` |
| `all()` | every owner once |
| `aliases()` | `{alias: owner_id}`, for diagnostics |
| `len(registry)` | the number of **owners** |
| `key in registry` | whether an id or alias resolves |

**DID takes priority over URL** as the published identifier.

### Loader options

```python
load_owners_yaml(path, missing_ok=False, validate=False)
```

- `missing_ok` — return an empty registry instead of raising when the file is
  absent. The two implementations this consolidated disagreed (one raised, one
  returned empty), so the choice is the caller's rather than a silent behaviour
  change for one of them.
- `validate` — check against `owners.schema.json` before parsing. Off by default so
  adopting this loader cannot turn an existing warning into a crash. **Callers that
  provision real identities should turn it on.**

---

## Precedence, and why counting is separate from lookup

Ids and aliases are held in separate maps, and an **id always wins over an alias**.
A deployment therefore cannot lose an owner because another owner claimed its name
as an alias — the alias is ignored and a warning is logged.

Aliases are registered only after every id is known, so precedence does not depend on
the order entries appear in the file.

`len(registry)` counts owners, not lookup keys. An earlier version merged the two
maps, and the export CLI reported "Loaded 16 owner(s)" for a fourteen-owner file.

Two owners claiming the same alias is a conflict the JSON Schema cannot express;
the first claimant keeps it and a warning names both. Duplicate ids are also warned,
and the later entry wins.

---

## Validation is strict here

Unlike `governance.yaml`, an owners file has no lenient mode:

```python
from celine.governance import validate_owners_file
validate_owners_file(Path("owners.yaml"))     # raises on any violation
```

`owners.schema.json` sets `additionalProperties: false` and constrains `type` to an
enum, so it can say precisely what is wrong — and an owners file is short enough that
fixing it is not a migration.

The strictness earns its keep downstream. An entry missing `id` is skipped without
comment by `celine-policies`' loader, so a typo does not fail: it quietly produces
one fewer Keycloak organization, and the missing org surfaces much later as an
authorization failure with no obvious cause.

Note the schema requires `id`, `type` **and** `name`, while the Python model defaults
`type` and leaves `name` optional. A file that parses is not necessarily a file that
validates — run validation.
