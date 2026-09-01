"""A consumer may subclass the models and still use the module functions.

The models are ``extra="ignore"`` and their docstrings invite subclassing —
``DataspaceConfig``'s says the EDC sub-objects are carried in ``ds``'s own
``DataspaceSpec``, ``GovernanceRule``'s says ``ds`` extends it. Every module
function used to hardcode the class it validated into, so taking that invitation
returned a base-class instance with the subclass's fields dropped: not an error,
a quieter and smaller object. That is the defect this package exists to remove,
pointed at the package itself.

The fixtures below stand in for ``ds``'s real subclasses — its ODRL ``policy``
block and its richer dataspace spec — rather than importing them, because
``celine.governance`` must not know its consumers.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pytest
from pydantic import Field

from celine.governance import (
    KNOWN_KEYS,
    DataspaceConfig,
    GovernanceConfig,
    GovernanceRule,
    merge_configs,
    merge_dataspace,
    merge_rules,
    parse_rule,
)


class SpecDataspace(DataspaceConfig):
    """A dataspace block carrying the connector's own sub-objects."""

    asset: Dict[str, Any] = Field(default_factory=dict)


class SpecRule(GovernanceRule):
    """A rule with an ODRL policy block the shared grammar does not define."""

    policy: Optional[Dict[str, Any]] = None
    dataspace: Optional[SpecDataspace] = None


class SpecConfig(GovernanceConfig):
    connector_id: Optional[str] = None
    defaults: SpecRule = Field(default_factory=SpecRule)
    sources: Dict[str, SpecRule] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# parse_rule
# ---------------------------------------------------------------------------


# @verifies REQ-0005
def test_a_subclass_field_is_a_field_and_not_extra():
    """The split follows `model_cls`, not the base grammar's `KNOWN_KEYS`.

    Splitting a subclass's file against the base key set would put `policy` in
    `extra`, where the field reads as its default forever with nothing warning —
    the same silence as a field added to the model and forgotten in `KNOWN_KEYS`.
    """
    rule = parse_rule(
        {"title": "t", "policy": {"permitted_actions": ["use"]}, "mistyped": 1},
        SpecRule,
    )

    assert isinstance(rule, SpecRule)
    assert rule.policy == {"permitted_actions": ["use"]}
    assert "policy" not in rule.extra

    # A key no model declares still lands in `extra`, subclass or not.
    assert rule.extra == {"mistyped": 1}


# @verifies REQ-0005
def test_the_default_split_is_still_exactly_known_keys():
    """`KNOWN_KEYS` stays the published statement of the base grammar."""
    rule = parse_rule({"title": "t", "policy": {"a": 1}})

    assert type(rule) is GovernanceRule
    assert rule.extra == {"policy": {"a": 1}}
    assert frozenset(GovernanceRule.model_fields) - {"extra"} == KNOWN_KEYS


def test_parse_rule_keeps_model_fields_set_for_a_subclass():
    """The merge layer reads `model_fields_set`; a subclass must not lose it."""
    rule = parse_rule({"expose": False}, SpecRule)

    assert rule.model_fields_set == {"expose"}


# ---------------------------------------------------------------------------
# merge_rules
# ---------------------------------------------------------------------------


def test_merging_two_subclass_rules_keeps_the_subclass_and_its_fields():
    base = parse_rule(
        {
            "title": "base",
            "policy": {"permitted_actions": ["use"]},
            "dataspace": {"expose": True, "asset": {"id": "a"}},
        },
        SpecRule,
    )
    override = parse_rule({"title": "override"}, SpecRule)

    merged = merge_rules(base, override)

    assert isinstance(merged, SpecRule)
    assert merged.title == "override"
    # Unstated by the overlay, therefore inherited — the whole point of
    # `exclude_unset`, now reaching fields the base grammar never heard of.
    assert merged.policy == {"permitted_actions": ["use"]}
    assert isinstance(merged.dataspace, SpecDataspace)
    assert merged.dataspace.asset == {"id": "a"}


def test_an_overlay_can_withdraw_a_subclass_field():
    base = parse_rule({"policy": {"permitted_actions": ["use"]}}, SpecRule)
    override = parse_rule({"policy": None}, SpecRule)

    assert merge_rules(base, override).policy is None


def test_merging_different_classes_raises_rather_than_choosing():
    """Silently picking one is how this class of bug starts.

    Picking the more derived class invents fields the other operand never had;
    picking the base drops the ones it did. Both are a silent decision about
    which half of the input survives, which is what the raise refuses to make.
    """
    with pytest.raises(TypeError, match="different model classes"):
        merge_rules(parse_rule({"title": "a"}), parse_rule({"title": "b"}, SpecRule))


def test_model_cls_says_deliberately_what_the_operands_cannot():
    """The escape hatch, for a caller who means to narrow."""
    base = parse_rule({"title": "a"}, SpecRule)
    override = parse_rule({"policy": {"x": 1}}, SpecRule)

    merged = merge_rules(base, override, model_cls=GovernanceRule)

    assert type(merged) is GovernanceRule
    assert not hasattr(merged, "policy")


# ---------------------------------------------------------------------------
# merge_dataspace
# ---------------------------------------------------------------------------


def test_dataspace_subclass_survives_and_the_tightening_rules_still_apply():
    base = SpecDataspace.model_validate({"consent_required": True, "purpose": ["a"]})
    override = SpecDataspace.model_validate(
        {"purpose": ["b"], "asset": {"id": "x"}, "consent_required": False}
    )

    merged = merge_dataspace(base, override)

    assert isinstance(merged, SpecDataspace)
    assert merged.asset == {"id": "x"}
    # An overlay may tighten, never loosen.
    assert merged.consent_required is True
    assert merged.purpose == ["a", "b"]


def test_a_lone_operand_is_returned_as_it_is():
    only = SpecDataspace.model_validate({"asset": {"id": "x"}})

    assert merge_dataspace(None, only) is only
    assert merge_dataspace(only, None) is only
    assert merge_dataspace(None, None) is None


# ---------------------------------------------------------------------------
# merge_configs
# ---------------------------------------------------------------------------


def test_a_subclass_config_keeps_its_own_root_fields():
    base = SpecConfig.model_validate(
        {
            "connector_id": "provider-a",
            "defaults": {"expose": True},
            "sources": {"db.schema.t": {"policy": {"permitted_actions": ["use"]}}},
        }
    )
    override = SpecConfig.model_validate(
        {"sources": {"db.schema.t": {"title": "renamed"}}}
    )

    merged = merge_configs(base, override)

    assert isinstance(merged, SpecConfig)
    # Unstated by the overlay: inherited, rather than dropped by a constructor
    # that named three keys.
    assert merged.connector_id == "provider-a"

    rule = merged.sources["db.schema.t"]
    assert isinstance(rule, SpecRule)
    assert rule.title == "renamed"
    assert rule.policy == {"permitted_actions": ["use"]}


def test_config_overlay_rules_are_unchanged_for_a_subclass():
    """`depends_on` is whole replacement, and `[]` is a statement, not an absence."""
    base = SpecConfig.model_validate({"depends_on": [{"dataset": "a"}]})
    override = SpecConfig.model_validate({"depends_on": []})

    assert merge_configs(base, override).depends_on == []

    silent = SpecConfig.model_validate({"connector_id": "b"})
    assert [d.dataset for d in merge_configs(base, silent).depends_on or []] == ["a"]
