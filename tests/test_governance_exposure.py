"""The two exposure gates: catalogue vs dataspace.

The migration these support is a two-step one, and the tests are written around
that: the library must ship *first*, understand the new field, and leave every
unmigrated file behaving exactly as before — otherwise the day the governance
files change, 60 datasets silently leave the catalogue that the API is already
serving. The "legacy" cases below are that guarantee.
"""
from __future__ import annotations

import pytest

from celine.governance import (
    GovernanceResolver,
    dataspace_expose,
    effective_expose,
    exposure_conflict,
    merge_rules,
    parse_rule,
)


# ---------------------------------------------------------------------------
# legacy grammar — must not move
# ---------------------------------------------------------------------------


# @verifies REQ-0002
@pytest.mark.parametrize("offered", [True, False])
def test_legacy_file_keeps_todays_catalogue_behaviour(offered: bool) -> None:
    """No top-level `expose` — the catalogue gate falls back to dataspace.expose.

    This is the entire reason the field is tri-state rather than `bool = False`.
    With a plain default the fallback could not be expressed, and shipping the
    library would drop every dataset out of the catalogue before the files caught
    up.
    """
    rule = parse_rule({"dataspace": {"expose": offered}})
    assert effective_expose(rule) is offered
    assert dataspace_expose(rule) is offered


# @verifies REQ-0002
def test_a_file_with_no_dataspace_block_is_exposed_nowhere() -> None:
    rule = parse_rule({"title": "x"})
    assert effective_expose(rule) is False
    assert dataspace_expose(rule) is False


# ---------------------------------------------------------------------------
# migrated grammar — the point of the change
# ---------------------------------------------------------------------------


# @verifies REQ-0002
def test_catalogue_without_dataspace_is_now_expressible() -> None:
    """The case that motivated all of this: grid topology, visible but not offered."""
    rule = parse_rule({"expose": True, "dataspace": {"expose": False}})
    assert effective_expose(rule) is True
    assert dataspace_expose(rule) is False
    assert exposure_conflict(rule) is None


# @verifies REQ-0002
def test_both_channels() -> None:
    rule = parse_rule({"expose": True, "dataspace": {"expose": True}})
    assert effective_expose(rule) is True
    assert dataspace_expose(rule) is True


# @verifies REQ-0002
def test_expose_false_wins_over_an_inherited_true() -> None:
    """A stated `false` must beat the fallback, not be mistaken for "unstated"."""
    rule = parse_rule({"expose": False, "dataspace": {"expose": False}})
    assert effective_expose(rule) is False


# ---------------------------------------------------------------------------
# the contradiction
# ---------------------------------------------------------------------------


# @verifies REQ-0002
def test_offered_but_unlisted_is_reported() -> None:
    rule = parse_rule({"expose": False, "dataspace": {"expose": True}})
    conflict = exposure_conflict(rule)
    assert conflict is not None
    assert "cannot be discovered" in conflict


# @verifies REQ-0002
def test_conflict_is_only_that_one_combination() -> None:
    for block in (
        {},
        {"expose": True, "dataspace": {"expose": True}},
        {"expose": True, "dataspace": {"expose": False}},
        {"expose": False},
        {"dataspace": {"expose": True}},
    ):
        assert exposure_conflict(parse_rule(block)) is None, block


# ---------------------------------------------------------------------------
# inheritance and overlays
# ---------------------------------------------------------------------------


# @verifies REQ-0002
def test_a_dataset_can_withdraw_itself_from_a_file_default() -> None:
    """`expose` merges by `exclude_unset`, so a dataset can say no to defaults.

    Under the old truthiness merge (`base.expose or override.expose`) this was
    inexpressible — the exact defect that let `dataspace.expose: false` silently
    do nothing in production.
    """
    merged = merge_rules(parse_rule({"expose": True}), parse_rule({"expose": False}))
    assert effective_expose(merged) is False


# @verifies REQ-0002
def test_a_dataset_that_says_nothing_inherits_the_file_default() -> None:
    merged = merge_rules(parse_rule({"expose": True}), parse_rule({"title": "t"}))
    assert effective_expose(merged) is True


def test_a_deployer_overlay_can_withdraw_a_dataspace_offer(tmp_path) -> None:
    """Deny-by-default, expressed the way a deployment would express it.

    The base file offers the dataset; the overlay withdraws the offer while
    keeping it in the catalogue. This is what the four existing deployment
    overlays would carry.
    """
    (tmp_path / "governance.yaml").write_text(
        "defaults:\n  dataspace:\n    expose: true\n"
        "sources:\n  db.gold.substations: {}\n",
        encoding="utf-8",
    )
    (tmp_path / "governance.grid.yaml").write_text(
        "defaults:\n  expose: true\n  dataspace:\n    expose: false\n",
        encoding="utf-8",
    )
    res = GovernanceResolver.from_file_with_override(
        tmp_path / "governance.yaml", "grid"
    )
    rule = res.resolve("db.gold.substations")
    assert effective_expose(rule) is True
    assert dataspace_expose(rule) is False


# @verifies REQ-0002
def test_expose_reaches_the_rule_rather_than_extra() -> None:
    """`expose` must be in KNOWN_KEYS, or it parses into `extra` and reads as unset.

    That failure is silent and total: the field validates against the schema,
    lands in `extra`, and the gate falls back — so a migrated file would behave
    as if it had never been migrated.
    """
    rule = parse_rule({"expose": True})
    assert rule.expose is True
    assert "expose" not in rule.extra
