"""An ownership name leaves the library as an organisation id, never as a placeholder.

A public pipeline names its owner ``rec`` or ``dso`` so it ships without a
customer's identifier. Every consumer used the name as written — an asset owner
label, an ODRL assigner — so the placeholder reached the wire and matched
nothing. Resolution replaces each name with the id of the owner a deployment's
registry says it is, after any overlay has been merged.

Ordered unit → integration (the resolver loading files) → end to end (a public
governance file, a deployment's owners file and its overlay, down to the DID).
"""

from __future__ import annotations

import logging
import textwrap
from pathlib import Path
from typing import Any, Dict, Optional

import pytest
import yaml

from celine.governance import (
    GovernanceConfig,
    GovernanceResolver,
    GovernanceRule,
    OwnerEntry,
    OwnersRegistry,
    UnresolvedOwnerError,
    load_owners_yaml,
    merge_rules,
    parse_rule,
    resolve_config_ownership,
    resolve_ownership,
    unresolved_owners,
)


@pytest.fixture
def owners() -> OwnersRegistry:
    return OwnersRegistry(
        [
            OwnerEntry(id="example-rec", url="https://rec.example.org", aliases=["rec"]),
            OwnerEntry(
                id="example-dso",
                did="did:web:connector.dso.example.org",
                aliases=["dso"],
            ),
            OwnerEntry(id="open-data-provider", url="https://open-data.example.org"),
        ]
    )


def names(rule: GovernanceRule) -> list[str]:
    return [o.name for o in rule.ownership]


# ---------------------------------------------------------------------------
# unit — resolve_ownership
# ---------------------------------------------------------------------------


# @verifies REQ-0008
def test_a_placeholder_becomes_the_owner_id(owners: OwnersRegistry) -> None:
    rule = parse_rule({"ownership": [{"name": "rec", "type": "DATA_OWNER"}]})

    resolved = resolve_ownership(rule, owners, strict=True)

    assert names(resolved) == ["example-rec"]
    assert resolved.ownership[0].type == "DATA_OWNER"


# @verifies REQ-0008
def test_an_owner_id_resolves_to_itself_and_the_rule_is_returned_as_is(
    owners: OwnersRegistry,
) -> None:
    rule = parse_rule({"ownership": ["example-dso", "open-data-provider"]})

    assert resolve_ownership(rule, owners, strict=True) is rule


# @verifies REQ-0008
def test_the_input_rule_is_not_modified(owners: OwnersRegistry) -> None:
    rule = parse_rule({"ownership": ["dso"]})

    resolve_ownership(rule, owners, strict=True)

    assert names(rule) == ["dso"]


# @verifies REQ-0008
def test_a_rule_without_ownership_is_untouched(owners: OwnersRegistry) -> None:
    rule = parse_rule({"expose": True})

    resolved = resolve_ownership(rule, owners, strict=True)

    assert resolved is rule
    assert "ownership" not in resolved.model_fields_set


# @verifies REQ-0008
def test_strict_raises_on_an_unresolved_name(owners: OwnersRegistry) -> None:
    rule = parse_rule({"ownership": ["rec", "nobody", "ghost"]})

    with pytest.raises(UnresolvedOwnerError) as exc:
        resolve_ownership(rule, owners, strict=True)

    assert set(exc.value.names) == {"nobody", "ghost"}
    assert isinstance(exc.value, ValueError)


# @verifies REQ-0008
def test_lenient_keeps_an_unresolved_name_and_warns(
    owners: OwnersRegistry, caplog: pytest.LogCaptureFixture
) -> None:
    rule = parse_rule({"ownership": ["rec", "nobody"]})

    with caplog.at_level(logging.WARNING, logger="celine.governance.ownership"):
        resolved = resolve_ownership(rule, owners, strict=False)

    assert names(resolved) == ["example-rec", "nobody"]
    assert "'nobody'" in caplog.text


# @verifies REQ-0008
def test_a_placeholder_and_its_id_collapse_to_one_owner(owners: OwnersRegistry) -> None:
    rule = parse_rule({"ownership": ["rec", "example-rec", "dso"]})

    assert names(resolve_ownership(rule, owners, strict=True)) == [
        "example-rec",
        "example-dso",
    ]


# @verifies REQ-0008
def test_the_same_organisation_under_two_types_is_kept_twice(
    owners: OwnersRegistry,
) -> None:
    """Only an exact repeat is dropped; the file's two statements both survive."""
    rule = parse_rule(
        {
            "ownership": [
                {"name": "rec", "type": "DATA_OWNER"},
                {"name": "example-rec", "type": "consortium"},
            ]
        }
    )

    resolved = resolve_ownership(rule, owners, strict=True)

    assert [(o.name, o.type) for o in resolved.ownership] == [
        ("example-rec", "DATA_OWNER"),
        ("example-rec", "consortium"),
    ]


# @verifies REQ-0008
def test_a_resolved_rule_still_merges_as_declared(owners: OwnersRegistry) -> None:
    """Resolution must not mark fields set that the file never stated.

    If it did, the resolved rule would overwrite the baseline's ``expose`` with
    its own default — the kwargs-construction bug, reintroduced.
    """
    base = parse_rule({"expose": True, "ownership": ["open-data-provider"]})
    override = resolve_ownership(
        parse_rule({"ownership": ["rec"]}), owners, strict=True
    )

    merged = merge_rules(base, override)

    assert merged.expose is True
    assert names(merged) == ["example-rec"]


class PolicyRule(GovernanceRule):
    policy: Optional[Dict[str, Any]] = None


# @verifies REQ-0008
def test_a_consumer_subclass_survives_resolution(owners: OwnersRegistry) -> None:
    rule = parse_rule(
        {"ownership": ["dso"], "policy": {"permission": ["use"]}}, PolicyRule
    )

    resolved = resolve_ownership(rule, owners, strict=True)

    assert isinstance(resolved, PolicyRule)
    assert resolved.policy == {"permission": ["use"]}
    assert names(resolved) == ["example-dso"]


# @verifies REQ-0008
def test_any_lookup_with_by_id_is_accepted() -> None:
    """A consumer's own adapter over a live registry works like the YAML one."""

    class Adapter:
        def by_id(self, alias: str) -> Optional[OwnerEntry]:
            return OwnerEntry(id="example-rec") if alias in {"rec", "example-rec"} else None

    rule = parse_rule({"ownership": ["rec"]})

    assert names(resolve_ownership(rule, Adapter(), strict=True)) == ["example-rec"]


# @verifies REQ-0008
def test_a_placeholder_map_resolves_without_aliases() -> None:
    """The target shape: the owners file lists organisations, the map is local."""
    plain = OwnersRegistry([OwnerEntry(id="example-rec"), OwnerEntry(id="example-dso")])
    rule = parse_rule({"ownership": ["rec", "dso"]})

    resolved = resolve_ownership(
        rule, plain, strict=True, placeholders={"rec": "example-rec", "dso": "example-dso"}
    )

    assert names(resolved) == ["example-rec", "example-dso"]


# @verifies REQ-0008
def test_a_placeholder_pointing_at_no_owner_is_unresolved() -> None:
    plain = OwnersRegistry([OwnerEntry(id="example-rec")])
    rule = parse_rule({"ownership": ["dso"]})

    with pytest.raises(UnresolvedOwnerError) as exc:
        resolve_ownership(rule, plain, strict=True, placeholders={"dso": "example-dso"})

    assert set(exc.value.names) == {"dso"}


# @verifies REQ-0008
def test_an_owner_id_wins_over_a_placeholder_of_the_same_name(
    owners: OwnersRegistry, caplog: pytest.LogCaptureFixture
) -> None:
    rule = parse_rule({"ownership": ["example-rec"]})

    with caplog.at_level(logging.WARNING, logger="celine.governance.ownership"):
        resolved = resolve_ownership(
            rule, owners, strict=True, placeholders={"example-rec": "example-dso"}
        )

    assert names(resolved) == ["example-rec"]
    assert "owner id wins" in caplog.text


# @verifies REQ-0008
def test_a_placeholder_map_takes_precedence_over_a_registry_alias(
    owners: OwnersRegistry,
) -> None:
    """The deployment's map is the replacement for ``aliases:``; it is asked first."""
    rule = parse_rule({"ownership": ["rec"]})

    resolved = resolve_ownership(
        rule, owners, strict=True, placeholders={"rec": "open-data-provider"}
    )

    assert names(resolved) == ["open-data-provider"]


# ---------------------------------------------------------------------------
# unit — whole configs
# ---------------------------------------------------------------------------


def config(raw: Dict[str, Any]) -> GovernanceConfig:
    return GovernanceResolver.from_dict(raw).config


# @verifies REQ-0008
def test_defaults_and_every_source_are_resolved(owners: OwnersRegistry) -> None:
    cfg = config(
        {
            "defaults": {"ownership": ["rec"]},
            "sources": {"db.s.a": {"ownership": ["dso"]}, "db.s.b": {"tags": ["x"]}},
        }
    )

    resolved = resolve_config_ownership(cfg, owners, strict=True)

    assert names(resolved.defaults) == ["example-rec"]
    assert names(resolved.sources["db.s.a"]) == ["example-dso"]
    assert resolved.sources["db.s.b"] is cfg.sources["db.s.b"]


# @verifies REQ-0008
def test_a_config_with_nothing_to_resolve_is_returned_as_is(
    owners: OwnersRegistry,
) -> None:
    cfg = config({"defaults": {"ownership": ["example-rec"]}, "sources": {"db.s.a": {}}})

    assert resolve_config_ownership(cfg, owners, strict=True) is cfg


# @verifies REQ-0008
def test_strict_reports_every_unresolved_name_and_where(owners: OwnersRegistry) -> None:
    cfg = config(
        {
            "defaults": {"ownership": ["nobody"]},
            "sources": {
                "db.s.a": {"ownership": ["ghost"]},
                "db.s.b": {"ownership": ["nobody", "rec"]},
            },
        }
    )

    with pytest.raises(UnresolvedOwnerError) as exc:
        resolve_config_ownership(cfg, owners, strict=True)

    assert exc.value.names == {"nobody": ["defaults", "db.s.b"], "ghost": ["db.s.a"]}
    assert "db.s.a" in str(exc.value)


# @verifies REQ-0008
def test_unresolved_owners_reports_without_raising(owners: OwnersRegistry) -> None:
    cfg = config(
        {
            "defaults": {"ownership": ["rec"]},
            "sources": {"db.s.a": {"ownership": ["ghost", "ghost"]}},
        }
    )

    assert unresolved_owners(cfg, owners) == {"ghost": ["db.s.a"]}


# @verifies REQ-0008
def test_unresolved_owners_is_empty_when_everything_resolves(
    owners: OwnersRegistry,
) -> None:
    cfg = config({"defaults": {"ownership": ["rec", "dso", "open-data-provider"]}})

    assert unresolved_owners(cfg, owners) == {}


# @verifies REQ-0008
def test_unresolved_owners_consults_the_placeholder_map() -> None:
    plain = OwnersRegistry([OwnerEntry(id="example-rec")])
    cfg = config({"defaults": {"ownership": ["rec"]}, "sources": {"a": {"ownership": ["dso"]}}})

    assert unresolved_owners(cfg, plain, placeholders={"rec": "example-rec"}) == {
        "dso": ["a"]
    }


# ---------------------------------------------------------------------------
# integration — the resolver loads, merges, then resolves
# ---------------------------------------------------------------------------

BASE = """
defaults:
  ownership:
    - name: rec
      type: DATA_OWNER
sources:
  datasets.gold.meters:
    dataspace:
      expose: true
  datasets.gold.grid:
    ownership: [placeholder-nobody-maps]
"""

OVERLAY = """
sources:
  datasets.gold.grid:
    ownership: [dso]
"""


def write(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


# @verifies REQ-0008
def test_from_file_without_owners_keeps_names_as_written(tmp_path: Path) -> None:
    """The default is unchanged behaviour: nothing resolves unless asked."""
    path = write(tmp_path / "governance.yaml", BASE)

    rule = GovernanceResolver.from_file(path).resolve("datasets.gold.meters")

    assert names(rule) == ["rec"]


# @verifies REQ-0008
def test_from_file_resolves_with_owners(tmp_path: Path, owners: OwnersRegistry) -> None:
    path = write(tmp_path / "governance.yaml", "defaults:\n  ownership: [rec]\n")

    resolver = GovernanceResolver.from_file(path, owners=owners, strict_owners=True)

    assert names(resolver.resolve("anything")) == ["example-rec"]


# @verifies REQ-0008
def test_owners_without_a_strictness_is_refused(
    tmp_path: Path, owners: OwnersRegistry
) -> None:
    path = write(tmp_path / "governance.yaml", BASE)

    with pytest.raises(TypeError, match="strict_owners"):
        GovernanceResolver.from_file(path, owners=owners)


# @verifies REQ-0008
def test_strict_from_file_raises_on_the_unresolved_base(
    tmp_path: Path, owners: OwnersRegistry
) -> None:
    path = write(tmp_path / "governance.yaml", BASE)

    with pytest.raises(UnresolvedOwnerError) as exc:
        GovernanceResolver.from_file(path, owners=owners, strict_owners=True)

    assert exc.value.names == {"placeholder-nobody-maps": ["datasets.gold.grid"]}


# @verifies REQ-0008
def test_resolution_runs_after_the_overlay(tmp_path: Path, owners: OwnersRegistry) -> None:
    """The base alone does not resolve; the overlay replaces the name that fails.

    Resolving per file would raise on a placeholder the deployment has already
    withdrawn.
    """
    base = write(tmp_path / "governance.yaml", BASE)
    write(tmp_path / "governance.deploy.yaml", OVERLAY)

    resolver = GovernanceResolver.from_file_with_override(
        base, "deploy", owners=owners, strict_owners=True
    )

    assert names(resolver.resolve("datasets.gold.grid")) == ["example-dso"]
    assert names(resolver.resolve("datasets.gold.meters")) == ["example-rec"]


# @verifies REQ-0008
def test_a_missing_overlay_still_resolves(tmp_path: Path, owners: OwnersRegistry) -> None:
    base = write(tmp_path / "governance.yaml", "defaults:\n  ownership: [dso]\n")

    resolver = GovernanceResolver.from_file_with_override(
        base, "absent", owners=owners, strict_owners=True
    )

    assert names(resolver.resolve("x")) == ["example-dso"]


# @verifies REQ-0008
def test_no_overlay_name_still_resolves(
    tmp_path: Path, owners: OwnersRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GOVERNANCE_OVERLAY_NAME", raising=False)
    base = write(tmp_path / "governance.yaml", "defaults:\n  ownership: [dso]\n")

    resolver = GovernanceResolver.from_file_with_override(
        base, owners=owners, strict_owners=False
    )

    assert names(resolver.resolve("x")) == ["example-dso"]


# @verifies REQ-0008
def test_placeholders_without_owners_is_refused(tmp_path: Path) -> None:
    path = write(tmp_path / "governance.yaml", BASE)

    with pytest.raises(TypeError, match="placeholders"):
        GovernanceResolver.from_file(path, placeholders={"rec": "example-rec"})


# @verifies REQ-0008
def test_from_file_with_override_forwards_the_placeholder_map(tmp_path: Path) -> None:
    plain = OwnersRegistry([OwnerEntry(id="example-rec"), OwnerEntry(id="example-dso")])
    base = write(tmp_path / "governance.yaml", BASE)
    write(tmp_path / "governance.deploy.yaml", OVERLAY)

    resolver = GovernanceResolver.from_file_with_override(
        base,
        "deploy",
        owners=plain,
        strict_owners=True,
        placeholders={"rec": "example-rec", "dso": "example-dso"},
    )

    assert names(resolver.resolve("datasets.gold.grid")) == ["example-dso"]
    assert names(resolver.resolve("datasets.gold.meters")) == ["example-rec"]


# ---------------------------------------------------------------------------
# e2e — a public pipeline, a deployment's owners file and overlay, to the DID
# ---------------------------------------------------------------------------

PUBLIC_GOVERNANCE = """
defaults:
  license: ODbL-1.0
  ownership:
    - name: rec
      type: DATA_OWNER
    - name: open-data-provider
      type: consortium
sources:
  datasets.ds_dev_gold.rec_meter_readings:
    classification: pii
    dataspace:
      expose: true
  datasets.ds_dev_gold.grid_*:
    ownership:
      - name: dso
        type: DATA_OWNER
    dataspace:
      expose: true
"""

DEPLOYMENT_OVERLAY = """
sources:
  datasets.ds_dev_gold.rec_meter_readings:
    dataspace:
      expose: false
"""

DEPLOYMENT_OWNERS = """
owners:
  - id: example-rec
    type: schema:NGO
    name: Example REC
    url: https://rec.example.org
  - id: example-dso
    type: schema:Corporation
    name: Example DSO
    did: did:web:connector.dso.example.org
  - id: open-data-provider
    type: schema:Project
    name: Open Data Provider
    url: https://open-data.example.org
"""

# The deployment's own local configuration — not the owners file, not the public
# governance. Loaded however the deployment loads its settings.
DEPLOYMENT_PLACEHOLDERS = """
placeholders:
  rec: example-rec
  dso: example-dso
"""


# @verifies REQ-0008
def test_a_deployment_publishes_organisation_ids_and_their_dids(tmp_path: Path) -> None:
    """What a connector needs from a public pipeline in a real deployment.

    The owner label is the organisation id, and the assigner DID is found from
    it — with no placeholder left anywhere in the resolved rules. The owners
    file carries no ``aliases:``; the placeholder map is the deployment's own.
    """
    governance = write(tmp_path / "app" / "governance.yaml", PUBLIC_GOVERNANCE)
    write(tmp_path / "app" / "governance.example.yaml", DEPLOYMENT_OVERLAY)
    owners = load_owners_yaml(
        write(tmp_path / "env" / "owners.yaml", DEPLOYMENT_OWNERS), validate=True
    )

    local = yaml.safe_load(
        write(tmp_path / "env" / "local.yaml", DEPLOYMENT_PLACEHOLDERS).read_text()
    )
    placeholders = local["placeholders"]

    resolver = GovernanceResolver.from_file_with_override(
        governance,
        "example",
        owners=owners,
        strict_owners=True,
        placeholders=placeholders,
    )

    grid = resolver.resolve("datasets.ds_dev_gold.grid_substations")
    assert names(grid) == ["example-dso"]
    assert owners.canonical_uri(grid.ownership[0].name) == (
        "did:web:connector.dso.example.org"
    )
    assert grid.dataspace is not None and grid.dataspace.expose is True

    meters = resolver.resolve("datasets.ds_dev_gold.rec_meter_readings")
    assert names(meters) == ["example-rec", "open-data-provider"]
    assert meters.license == "ODbL-1.0"
    assert meters.dataspace is not None and meters.dataspace.expose is False

    assert unresolved_owners(resolver.config, owners) == {}
    assert unresolved_owners(
        GovernanceResolver.from_file(governance).config, owners, placeholders=placeholders
    ) == {}
    assert set(owners.aliases()) == set()
    every_name = {
        o.name
        for rule in [resolver.config.defaults, *resolver.config.sources.values()]
        for o in rule.ownership
    }
    assert every_name.isdisjoint({"rec", "dso"})
