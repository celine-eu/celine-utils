"""Contracts that hold the consolidation together.

These do not test behaviour so much as pin the properties that make
`celine.governance` safe to share and cheap to move.
"""

from __future__ import annotations

import json
import pkgutil
from pathlib import Path

import pytest

import celine.governance as gov
from celine.governance import (
    KNOWN_KEYS,
    GovernanceResolver,
    GovernanceRule,
    GovernanceValidationError,
    build_facet,
    load_schema,
    parse_rule,
    validate,
)

SRC = Path(__file__).resolve().parents[1] / "src"


# ---------------------------------------------------------------------------
# The boundary — this is what makes a single repo as safe as a split one
# ---------------------------------------------------------------------------


# @verifies REQ-0001
def test_governance_never_imports_celine_utils():
    """`celine.governance` must stand alone.

    It is shipped from the `celine-utils` wheel today and is expected to become
    its own distribution. One convenience import from `celine.utils` turns that
    move from a `git mv` back into a project — and, worse, reintroduces the
    dependency weight that made three consumers fork their own parser.

    A source scan rather than an import hook, so it fails on the offending line
    rather than on whichever module happens to be imported first.
    """
    offenders = []
    for path in (SRC / "celine" / "governance").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith(("import celine.utils", "from celine.utils")):
                offenders.append(f"{path.relative_to(SRC)}:{lineno}: {stripped}")
    assert not offenders, "celine.governance must not import celine.utils:\n" + "\n".join(
        offenders
    )


# @verifies REQ-0001
def test_governance_imports_no_heavy_dependency():
    """Guards the same boundary from the other direction.

    The extras exist so an API service can parse governance without dbt, Meltano,
    Prefect or sqlalchemy. If one of these appears here, `celine-utils` core has
    silently regained the weight that caused the duplication.
    """
    forbidden = {"sqlalchemy", "prefect", "meltano", "dbt", "openlineage", "keycloak", "pandas"}
    offenders = []
    for path in (SRC / "celine" / "governance").rglob("*.py"):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if not stripped.startswith(("import ", "from ")):
                continue
            root = stripped.split()[1].split(".")[0]
            if root in forbidden:
                offenders.append(f"{path.relative_to(SRC)}:{lineno}: {stripped}")
    assert not offenders, "heavy dependency in celine.governance:\n" + "\n".join(offenders)


# ---------------------------------------------------------------------------
# The trap that made `ontology` read as absent on introduction
# ---------------------------------------------------------------------------


# @verifies REQ-0005
def test_known_keys_matches_the_model_fields():
    """A field on the model but missing from KNOWN_KEYS parses into `extra` and
    reads as absent — silently, with the schema still validating the file.

    That is exactly how the `ontology` block failed when it was introduced. This
    test is the thing that would have caught it.
    """
    model_fields = set(GovernanceRule.model_fields) - {"extra"}
    assert model_fields == set(KNOWN_KEYS), (
        f"only in model: {sorted(model_fields - set(KNOWN_KEYS))}; "
        f"only in KNOWN_KEYS: {sorted(set(KNOWN_KEYS) - model_fields)}"
    )


# @verifies REQ-0005
def test_parsing_records_only_the_keys_the_file_declared():
    """`model_fields_set` is what every overlay reads to tell unset from false.

    Building with keyword arguments — the previous implementation — marks every
    field as set and degrades the merge to "override always wins".
    """
    rule = parse_rule({"access_level": "open"})
    assert rule.model_fields_set == {"access_level"}
    assert rule.dataspace is None


# @verifies REQ-0005
def test_unknown_keys_land_in_extra_rather_than_vanishing():
    rule = parse_rule({"access_level": "open", "access_levl": "typo"})
    assert rule.extra == {"access_levl": "typo"}


# ---------------------------------------------------------------------------
# Validation — the schema had never been executed before this package
# ---------------------------------------------------------------------------


# @verifies REQ-0005
def test_schema_violation_raises_with_every_error_at_once():
    """One problem per run turns a five-minute fix into five round trips."""
    bad = {
        "defaults": {"access_level": "nonsense"},
        "sources": {"ds.x": {"classification": "purple"}},
    }
    with pytest.raises(GovernanceValidationError) as exc:
        validate(bad, source="bad.yaml")
    assert len(exc.value.errors) == 2


# @verifies REQ-0005
def test_unknown_key_warns_by_default_and_raises_under_strict():
    doc = {"defaults": {}, "sources": {"ds.x": {"access_levl": "open"}}}
    assert validate(doc) == ["sources/ds.x: access_levl"]
    with pytest.raises(GovernanceValidationError):
        validate(doc, strict=True)


# @verifies REQ-0004
def test_schemas_are_readable_from_the_installed_package():
    """Read via importlib.resources, not a `__file__` walk — the latter breaks
    the moment this runs from a wheel or a container."""
    for name in ("governance.schema.json", "owners.schema.json",
                 "GovernanceDatasetFacet.schema.json"):
        assert load_schema(name)["$schema"]


# ---------------------------------------------------------------------------
# The facet — one projection, and a URL that may never move
# ---------------------------------------------------------------------------


# @verifies REQ-0004
def test_schema_url_is_pinned():
    """This string is embedded in every OpenLineage event already in Marquez.

    Changing it does not break a build — it silently invalidates historical
    lineage. The schema file may move between repositories; this may not.
    """
    assert gov.SCHEMA_URL == (
        "https://celine-eu.github.io/schema/GovernanceDatasetFacet.schema.json"
    )


# @verifies REQ-0004
def test_built_facet_validates_against_its_published_schema():
    import jsonschema

    rule = GovernanceResolver.from_dict(
        {
            "defaults": {"access_level": "internal", "tags": ["grid"]},
            "sources": {
                "ds.x": {
                    "title": "Substations",
                    "documentation_url": "https://example.org/docs",
                    "row_filters": [{"handler": "rec_registry", "args": {"column": "device_id"}}],
                    "dataspace": {"medallion": "gold", "purpose": ["GridMonitoring"]},
                }
            },
        }
    ).resolve("ds.x")

    facet = build_facet(rule, producer="tests")
    jsonschema.validate(facet, load_schema("GovernanceDatasetFacet.schema.json"))
    assert facet["_schemaURL"] == gov.SCHEMA_URL


def test_facet_omits_absent_fields_rather_than_nulling_them():
    """A facet saying `"license": null` claims the dataset has no license; one
    that omits the key says nothing about it. Only the second is what silence
    means."""
    facet = build_facet(parse_rule({"title": "t"}), producer="tests")
    assert "license" not in facet
    assert facet["title"] == "t"


def test_facet_dataspace_projection_is_opt_out():
    """The lineage extractors never emitted these; the catalogue exporter did.
    Widening lineage's payload must be a decision, not a side effect."""
    rule = parse_rule({"title": "t", "dataspace": {"medallion": "gold"}})
    assert build_facet(rule, producer="t")["medallion"] == "gold"
    assert "medallion" not in build_facet(rule, producer="t", include_dataspace=False)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def test_everything_exported_actually_exists():
    missing = [name for name in gov.__all__ if not hasattr(gov, name)]
    assert not missing


def test_deprecated_shim_still_resolves(recwarn):
    """A pipeline pinning an older import path must not break on upgrade."""
    import importlib

    mod = importlib.import_module("celine.utils.pipelines.governance")
    assert mod.GovernanceResolver is GovernanceResolver
