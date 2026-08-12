"""The grammar's coercions — what a rule accepts, and what it must not mangle."""
from __future__ import annotations

import pytest

from celine.governance import GovernanceOwner, GovernanceRule, parse_rule


# ---------------------------------------------------------------------------
# ownership normalisation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ownership",
    [
        pytest.param([{"name": "rec"}], id="dict-as-yaml-parses-it"),
        pytest.param(["rec"], id="bare-label-shorthand"),
        pytest.param([GovernanceOwner(name="rec")], id="already-a-model"),
    ],
)
def test_ownership_accepts_every_declared_form(ownership) -> None:
    """All three spellings mean the same owner.

    The third was silently wrong: the ``before`` validator sees whatever the
    caller passed, and a :class:`GovernanceOwner` is not a ``dict``, so it fell
    into the bare-label branch and became ``str(model)`` — a valid rule holding
    ``name="name='rec' type='OWNER'"``. Nothing caught it because the YAML path
    deals in dicts; it only surfaced when a consumer built a rule in Python and
    published ``urn:owner:name='rec' type='OWNER'``.
    """
    rule = GovernanceRule(ownership=ownership)
    assert [o.name for o in rule.ownership] == ["rec"]
    assert rule.ownership[0].type == "OWNER"


def test_ownership_model_keeps_its_own_type() -> None:
    """Pass-through must preserve the instance, not rebuild it from a name."""
    rule = GovernanceRule(ownership=[GovernanceOwner(name="rec", type="STEWARD")])
    assert rule.ownership[0].type == "STEWARD"


def test_ownership_forms_are_interchangeable_through_parse_rule() -> None:
    a = parse_rule({"ownership": ["rec"]})
    b = parse_rule({"ownership": [{"name": "rec"}]})
    assert a.ownership == b.ownership


def test_ownership_preserves_order_and_multiplicity() -> None:
    rule = parse_rule({"ownership": ["rec", {"name": "dso"}, "spxl"]})
    assert [o.name for o in rule.ownership] == ["rec", "dso", "spxl"]


def test_absent_ownership_is_an_empty_list_not_none() -> None:
    assert parse_rule({}).ownership == []


def test_ownership_rejects_a_mapping_without_a_name() -> None:
    """`name` is the lookup key against owners.yaml; a nameless owner is not one."""
    with pytest.raises(Exception):
        GovernanceRule.model_validate({"ownership": [{"type": "OWNER"}]})
