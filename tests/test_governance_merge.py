"""Overlay semantics — the rules that are not "override wins".

Every test here pins a distinction that a truthiness-based merge could not
express, which is what the previous implementation used.
"""

from __future__ import annotations

import pytest

from celine.governance import GovernanceResolver


def resolve(raw: dict, name: str):
    return GovernanceResolver.from_dict(raw).resolve(name)


# ---------------------------------------------------------------------------
# The defect this package was extracted to fix
# ---------------------------------------------------------------------------


# @verifies REQ-0002
def test_dataset_can_withdraw_an_exposure_the_defaults_granted():
    """`expose: false` over `defaults.expose: true` must take effect.

    Under the previous `base.expose or override.expose` this was inexpressible:
    the dataset stayed exposed and the overlay that withdrew it validated clean.
    The documented workaround was `access_level: secret`, which is a different
    statement about a different thing.
    """
    rule = resolve(
        {
            "defaults": {"dataspace": {"expose": True}},
            "sources": {"ds.gold.withdrawn": {"dataspace": {"expose": False}}},
        },
        "ds.gold.withdrawn",
    )
    assert rule.dataspace.expose is False


# @verifies REQ-0002
def test_unset_expose_still_inherits_the_default():
    """Silence must keep inheriting — the fix must not become "override always"."""
    rule = resolve(
        {
            "defaults": {"dataspace": {"expose": True}},
            "sources": {"ds.gold.quiet": {"dataspace": {"medallion": "gold"}}},
        },
        "ds.gold.quiet",
    )
    assert rule.dataspace.expose is True
    assert rule.dataspace.medallion == "gold"


def test_explicit_null_conforms_to_overrides_an_inherited_model():
    """`conforms_to: null` states *no payload model* — a claim, not a silence."""
    rule = resolve(
        {
            "defaults": {"dcat": {"conforms_to": "http://example.org/m", "themes": ["T"]}},
            "sources": {"ds.gold.unmodelled": {"dcat": {"conforms_to": None}}},
        },
        "ds.gold.unmodelled",
    )
    assert rule.dcat.conforms_to is None
    # ...and states nothing about themes, so they are still inherited.
    assert rule.dcat.themes == ["T"]


def test_partial_dcat_block_does_not_erase_the_defaults():
    """Whole-object replacement silently dropped every unrestated field."""
    rule = resolve(
        {
            "defaults": {
                "dcat": {
                    "themes": ["T"],
                    "language_uris": ["L"],
                    "accrual_periodicity": "DAILY",
                }
            },
            "sources": {"ds.gold.x": {"dcat": {"conforms_to": "http://www.w3.org/ns/sosa/"}}},
        },
        "ds.gold.x",
    )
    assert rule.dcat.conforms_to == "http://www.w3.org/ns/sosa/"
    assert rule.dcat.themes == ["T"]
    assert rule.dcat.language_uris == ["L"]
    assert rule.dcat.accrual_periodicity == "DAILY"


# ---------------------------------------------------------------------------
# Fields whose semantics are deliberately not "override wins"
# ---------------------------------------------------------------------------


def test_tags_are_a_union():
    rule = resolve(
        {"defaults": {"tags": ["a", "b"]}, "sources": {"ds.x": {"tags": ["c"]}}},
        "ds.x",
    )
    assert rule.tags == ["a", "b", "c"]


def test_purpose_is_a_union_not_a_replacement():
    """An overlay adds a reason for processing; it does not retract declared ones."""
    rule = resolve(
        {
            "defaults": {"dataspace": {"purpose": ["Billing"]}},
            "sources": {"ds.x": {"dataspace": {"purpose": ["GridMonitoring"]}}},
        },
        "ds.x",
    )
    assert rule.dataspace.purpose == ["Billing", "GridMonitoring"]


@pytest.mark.parametrize("field", ["consent_required", "contract_required"])
def test_requirements_tighten_but_never_loosen(field):
    """Once required, a layered file may not un-require it."""
    rule = resolve(
        {
            "defaults": {"dataspace": {field: True}},
            "sources": {"ds.x": {"dataspace": {field: False}}},
        },
        "ds.x",
    )
    assert getattr(rule.dataspace, field) is True


def test_ontology_is_replaced_whole_not_field_wise():
    """`spec` and `spec_file` are alternatives — a field-wise overlay could
    produce a rule declaring both, which the schema forbids."""
    rule = resolve(
        {
            "defaults": {"ontology": {"spec": "obs_energy_measurement"}},
            "sources": {"ds.x": {"ontology": {"spec_file": "./mappings/custom.yaml"}}},
        },
        "ds.x",
    )
    assert rule.ontology.spec_file == "./mappings/custom.yaml"
    assert rule.ontology.spec is None


def test_row_filters_are_replaced_whole():
    """Filters are independent gates; interleaving two lists by position would
    build a filter neither file declared."""
    rule = resolve(
        {
            "defaults": {"row_filters": [{"handler": "direct_user_match", "args": {}}]},
            "sources": {"ds.x": {"row_filters": [{"handler": "rec_registry", "args": {}}]}},
        },
        "ds.x",
    )
    assert [f["handler"] for f in rule.row_filters] == ["rec_registry"]


def test_ownership_is_replaced_when_stated_and_inherited_when_not():
    raw = {
        "defaults": {"ownership": [{"name": "dso", "type": "DATA_OWNER"}]},
        "sources": {"ds.a": {"ownership": ["rec"]}, "ds.b": {"tags": ["t"]}},
    }
    assert [o.name for o in resolve(raw, "ds.a").ownership] == ["rec"]
    assert [o.name for o in resolve(raw, "ds.b").ownership] == ["dso"]


# ---------------------------------------------------------------------------
# Resolution precedence
# ---------------------------------------------------------------------------


def test_exact_match_beats_glob_and_longest_glob_wins():
    raw = {
        "defaults": {"access_level": "internal"},
        "sources": {
            "ds.gold.*": {"access_level": "restricted"},
            "ds.gold.metrics_*": {"access_level": "open"},
            "ds.gold.metrics_exact": {"access_level": "secret"},
        },
    }
    assert resolve(raw, "ds.gold.metrics_exact").access_level == "secret"
    assert resolve(raw, "ds.gold.metrics_other").access_level == "open"
    assert resolve(raw, "ds.gold.anything").access_level == "restricted"
    assert resolve(raw, "ds.silver.x").access_level == "internal"
