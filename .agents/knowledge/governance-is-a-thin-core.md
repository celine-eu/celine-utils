# `celine.governance` has three dependencies, and that is the whole point

Verified against the code and CI on 2026-08-14.

## Why the package exists

There were **four** implementations of `governance.yaml` parsing — here, in the dataset
API, in the dataspace platform, and in the Superset tooling — and they disagreed about what
the same file meant.

They were not carelessness. `celine-utils` pulled in dbt, Meltano, Prefect and Keycloak in
order to parse a YAML file, and no API service could take that dependency. **The copies
were dependency avoidance**, and the fix was to remove the reason for them rather than to
ask people to stop copying.

## The constraint

> Runtime dependencies are `pydantic`, `pyyaml` and `jsonschema`. Nothing else, ever.

`celine.governance` must not import `celine.utils`, sqlalchemy, OpenLineage, or anything
from an optional extra. That constraint is what makes the package shareable, so it is not a
style preference — it is the feature.

**A CI job asserts it rather than trusting review** (`.github/workflows/governance-thin.yaml`,
with a contract test in `tests/test_governance_contract.py`). The workflow's own comment
states why: adding `import sqlalchemy` to a governance module is one line, and nothing in a
diff makes that line look expensive.

If a governance module needs something from the fat side, the dependency goes the other
way round — `celine.utils` may import `celine.governance`, never the reverse.

## The import path is chosen for a move that has not happened yet

The package ships from the `celine-utils` wheel today and is expected to become its own
distribution. The import path `celine.governance` was chosen so that move is a directory
rename with **no consumer editing an import**.

Do not "tidy" it to `celine.utils.governance`. That would make the eventual split a
breaking change for every downstream repository.

## Three model decisions that look like sloppiness and are not

**`extra="ignore"` on the owners model is deliberate.** Strictness lives in
`validate_owners`, which checks against a schema declaring `additionalProperties: false`.
Enforcing it in the model too would make a live identity-registry response carrying a new
field fail to parse in a consumer that only wanted the URI.

The same model accepts two spellings of one concept, because the identity registry's
database and API say one thing while the YAML seed files say another. A model that knew
only the API spelling silently discarded the block when loading a YAML — accepting both is
what lets one model read either.

**Overlay inference is opt-in rather than default**, because the two callers being
consolidated genuinely disagreed: one inferred the overlay name from the parent directory,
the other did not and returned the base unchanged. Inferring for everyone would make the
second start honouring overlays it deliberately ignores; defaulting to off would make the
first silently stop applying them. Both keep their behaviour by passing what they mean, and
neither default would have been correct.

## Release coupling

This repository **owns the governance schemas** that other repositories validate against, so
a schema change here must be released before any downstream repository can consume it. A
downstream floor is raised when the old version resolves to a package that breaks a shipped
path, not merely one that lacks a feature.
