"""`row_filters[].binds` — a person or an organization, and nothing else (REQ-0010)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from celine.governance import (
    DEFAULT_ROW_FILTER_BINDS,
    ROW_FILTER_BINDS,
    GovernanceResolver,
    GovernanceRule,
    GovernanceValidationError,
    build_facet,
    row_filter_binds,
    validate,
)

PERSON = {"handler": "rec_registry", "args": {"column": "device_id"}}
ORGANIZATION = {
    "handler": "organization_match",
    "binds": "organization",
    "args": {"column": "rec_id"},
}


def _doc(*filters: dict) -> dict:
    return {"defaults": {}, "sources": {"ds.x": {"row_filters": list(filters)}}}


# @verifies REQ-0010
def test_an_undeclared_filter_binds_a_person():
    assert DEFAULT_ROW_FILTER_BINDS == "person"
    assert row_filter_binds(PERSON) == "person"
    assert row_filter_binds({**PERSON, "binds": None}) == "person"


# @verifies REQ-0010
@pytest.mark.parametrize("value", ROW_FILTER_BINDS)
def test_both_declared_values_are_read(value):
    assert row_filter_binds({**PERSON, "binds": value}) == value
    validate(_doc({**PERSON, "binds": value}))


# @verifies REQ-0010
def test_a_typed_filter_is_read_by_attribute():
    """`ds` types its filters; the helper reads them the same way."""

    class Typed:
        handler = "organization_match"
        binds = "organization"

    class Untyped:
        handler = "rec_registry"

    assert row_filter_binds(Typed()) == "organization"
    assert row_filter_binds(Untyped()) == "person"


# @verifies REQ-0010
@pytest.mark.parametrize("value", ["persn", "Organization", "community", "", 1, True])
def test_any_other_value_is_refused_by_the_schema(value):
    with pytest.raises(GovernanceValidationError):
        validate(_doc({**PERSON, "binds": value}))


# @verifies REQ-0010
@pytest.mark.parametrize("value", ["persn", "Organization", "community", ""])
def test_any_other_value_is_refused_by_the_model_naming_the_filter(value):
    with pytest.raises(ValidationError) as err:
        GovernanceRule.model_validate({"row_filters": [PERSON, {**ORGANIZATION, "binds": value}]})
    assert "row_filters[1] (organization_match)" in str(err.value)


# @verifies REQ-0010
def test_the_helper_refuses_a_value_built_around_the_model():
    with pytest.raises(ValueError):
        row_filter_binds({"handler": "x", "binds": "community"})


# @verifies REQ-0010
def test_binds_survives_a_merge_and_reaches_the_facet():
    rule = GovernanceResolver.from_dict(
        {
            "defaults": {"row_filters": [PERSON]},
            "sources": {"ds.x": {"row_filters": [ORGANIZATION]}},
        }
    ).resolve("ds.x")
    assert [row_filter_binds(f) for f in rule.row_filters] == ["organization"]

    facet = build_facet(rule, producer="test")
    assert facet["rowFilters"] == [ORGANIZATION]


# @verifies REQ-0010
def test_an_undeclared_binds_is_not_invented_in_the_facet():
    """Absent stays absent: the facet does not restate the default."""
    rule = GovernanceResolver.from_dict(_doc(PERSON)).resolve("ds.x")
    assert build_facet(rule, producer="test")["rowFilters"] == [PERSON]
