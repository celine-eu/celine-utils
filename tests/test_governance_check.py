"""`celine-governance validate` — a tree checked as it will be served (REQ-0011–REQ-0014)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from celine.governance import GovernanceResolver, validate_owners
from celine.governance.check import ERROR, INFO, WARNING, check_governance
from celine.governance.cli import main

PERSON = {"handler": "rec_registry", "args": {"column": "device_id"}}
ORGANIZATION = {
    "handler": "organization_match",
    "binds": "organization",
    "args": {"column": "community_id"},
}
ALLOW_PERSON = {"handler": "allow_all", "binds": "person"}
ALLOW_ORGANIZATION = {"handler": "allow_all", "binds": "organization"}
DENY_ORGANIZATION = {"handler": "deny_all", "binds": "organization"}

OWNERS = {
    "owners": [
        {
            "id": "example-rec",
            "type": "schema:NGO",
            "name": "Example REC",
            "aliases": ["rec"],
            "organization": {"create": True, "role": "rec"},
        }
    ]
}


def _write(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


def _app(root: Path, app: str, sources: dict, defaults: dict | None = None) -> Path:
    return _write(
        root / "apps" / app / "governance.yaml",
        {"defaults": defaults or {}, "sources": sources},
    )


def _rule(access: str = "internal", *filters: dict, **extra) -> dict:
    return {"expose": True, "access_level": access, "row_filters": list(filters), **extra}


def _kinds(report, severity=None):
    return [
        (f.rule, f.dataset)
        for f in report.findings
        if severity is None or f.severity == severity
    ]


# --- REQ-0011: checked as served, every failure fails the run --------------------


# @verifies REQ-0011
def test_a_clean_tree_passes(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON, ORGANIZATION)})
    report = check_governance([tmp_path / "apps"])
    assert report.ok(strict=True), report.findings
    assert report.datasets == 1


# @verifies REQ-0011
def test_every_failure_is_collected_not_only_the_first(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("internal"), "ds.b": _rule("restricted")})
    _app(tmp_path, "b", {"ds.c": _rule("internal", PERSON)})
    report = check_governance([tmp_path / "apps"])
    assert {d for _, d in _kinds(report, ERROR)} == {"ds.a", "ds.b", "ds.c"}


# @verifies REQ-0011
def test_a_path_matching_nothing_is_an_error(tmp_path):
    report = check_governance([tmp_path / "missing", str(tmp_path / "nope/*/x.yaml")])
    assert [f.rule for f in report.failures()] == ["input", "input"]


# @verifies REQ-0011
def test_an_overlay_is_not_checked_alone(tmp_path):
    overlay = _write(tmp_path / "governance.a.yaml", {"defaults": {}})
    report = check_governance([overlay])
    assert not report.ok()
    assert "checked with its base" in report.failures()[0].detail


# @verifies REQ-0011
def test_a_file_that_does_not_load_fails_the_run(tmp_path):
    path = tmp_path / "apps" / "a" / "governance.yaml"
    path.parent.mkdir(parents=True)
    path.write_text("sources: [not, a, mapping\n")
    report = check_governance([path])
    assert [f.rule for f in report.failures()] == ["parse"]


# @verifies REQ-0011
def test_an_overlay_is_validated_as_part_of_the_materialized_result(tmp_path):
    """D13: an overlay has no `sources`, so alone it fails the schema; merged it passes."""
    _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON)})
    _write(
        tmp_path / "apps" / "a" / "governance.a.yaml",
        {"sources": {"ds.a": {"row_filters": [PERSON, ORGANIZATION]}}},
    )
    report = check_governance([tmp_path / "apps"])
    assert report.ok(strict=True), report.findings
    assert report.files[0][1].endswith("governance.a.yaml")


# @verifies REQ-0011
def test_an_overlay_type_error_is_still_caught(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON, ORGANIZATION)})
    _write(tmp_path / "apps" / "a" / "governance.a.yaml", {"defaults": {"expose": "yes"}})
    report = check_governance([tmp_path / "apps"])
    assert ("schema", None) in _kinds(report, ERROR)


# @verifies REQ-0011
def test_an_unknown_key_warns_and_fails_under_strict(tmp_path):
    _app(
        tmp_path,
        "a",
        {"ds.a": {**_rule("internal", PERSON, ORGANIZATION), "access_levl": "open"}},
    )
    report = check_governance([tmp_path / "apps"])
    assert report.ok() and not report.ok(strict=True)
    assert ("schema", None) in _kinds(report, WARNING)


# @verifies REQ-0011
def test_an_overlay_dir_is_searched_like_the_base_directory(tmp_path):
    """D18: the deployment keeps its overlays in a tree of its own."""
    _app(tmp_path, "grid", {"ds.g": _rule("internal", ORGANIZATION)})
    overlays = tmp_path / "deployment" / "governance"
    _write(
        overlays / "governance.grid.yaml",
        {"sources": {"ds.g": {"row_filters": [ALLOW_PERSON, ORGANIZATION]}}},
    )
    assert not check_governance([tmp_path / "apps"]).ok()
    report = check_governance([tmp_path / "apps"], overlay_dirs=[overlays])
    assert report.ok(strict=True), report.findings
    rule = GovernanceResolver.from_file_with_override(
        tmp_path / "apps" / "grid" / "governance.yaml",
        infer_from_dir=True,
        overlay_dirs=[overlays],
    ).resolve("ds.g")
    assert rule.row_filters == [ALLOW_PERSON, ORGANIZATION]


# @verifies REQ-0011
def test_two_overlays_for_one_base_are_refused(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON, ORGANIZATION)})
    _write(tmp_path / "apps" / "a" / "governance.a.yaml", {"defaults": {}})
    overlays = tmp_path / "deployment"
    _write(overlays / "governance.a.yaml", {"defaults": {}})
    report = check_governance([tmp_path / "apps"], overlay_dirs=[overlays])
    assert "more than one place" in report.failures()[0].detail
    with pytest.raises(ValueError, match="exactly one may apply"):
        GovernanceResolver.from_file_with_override(
            tmp_path / "apps" / "a" / "governance.yaml",
            infer_from_dir=True,
            overlay_dirs=[overlays],
        )


# @verifies REQ-0011
def test_an_overlay_naming_no_checked_base_warns(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON, ORGANIZATION)})
    overlays = tmp_path / "deployment"
    _write(overlays / "governance.other.yaml", {"defaults": {}})
    report = check_governance([tmp_path / "apps"], overlay_dirs=[overlays])
    assert report.ok() and not report.ok(strict=True)


# @verifies REQ-0011
def test_the_command_exits_1_on_failure_and_0_on_success(tmp_path, capsys):
    good = _app(tmp_path / "good", "a", {"ds.a": _rule("internal", PERSON, ORGANIZATION)})
    bad = _app(tmp_path / "bad", "a", {"ds.a": _rule("internal", PERSON)})
    assert main(["validate", str(good)]) == 0
    assert main(["validate", str(bad)]) == 1
    out = capsys.readouterr().out
    assert "row-policy" in out and "FAILED" in out


# @verifies REQ-0011
def test_the_command_reports_json(tmp_path, capsys):
    bad = _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON)})
    assert main(["validate", "--format", "json", str(bad)]) == 1
    data = json.loads(capsys.readouterr().out)
    assert data["ok"] is False
    assert data["findings"][0]["rule"] == "row-policy"


# --- REQ-0012: both layers, stated -------------------------------------------------


# @verifies REQ-0012
@pytest.mark.parametrize(
    "filters, missing",
    [
        ((), "person and organization"),
        ((PERSON,), "organization"),
        ((ORGANIZATION,), "person"),
    ],
)
@pytest.mark.parametrize("access", ["internal", "restricted", None])
def test_a_non_open_dataset_declares_both_layers(tmp_path, filters, missing, access):
    _app(tmp_path, "a", {"ds.a": _rule(access, *filters)})
    report = check_governance([tmp_path / "apps"])
    [finding] = report.failures()
    assert finding.rule == "row-policy"
    assert f"no {missing} layer" in finding.detail


# @verifies REQ-0012
def test_allow_all_states_a_layer(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("internal", ALLOW_PERSON, ORGANIZATION)})
    assert check_governance([tmp_path / "apps"]).ok(strict=True)


# @verifies REQ-0012
def test_an_open_dataset_is_listed_not_failed(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("open")})
    report = check_governance([tmp_path / "apps"])
    assert report.ok(strict=True)
    assert _kinds(report, INFO) == [("row-policy", "ds.a")]


# @verifies REQ-0012
@pytest.mark.parametrize("rule", [{"expose": False, "access_level": "internal"},
                                  {"expose": True, "access_level": "secret"}])
def test_an_unserved_dataset_needs_no_row_policy(tmp_path, rule):
    _app(tmp_path, "a", {"ds.a": rule})
    assert check_governance([tmp_path / "apps"]).ok(strict=True)


# @verifies REQ-0012
@pytest.mark.parametrize("handler", ["allow_all", "deny_all"])
def test_a_layer_verdict_must_state_binds(handler):
    with pytest.raises(ValueError, match="must state binds"):
        GovernanceResolver.from_dict(
            {"defaults": {}, "sources": {"ds.a": {"row_filters": [{"handler": handler}]}}}
        )


# @verifies REQ-0012
def test_a_layer_verdict_without_binds_fails_the_check(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("internal", {"handler": "deny_all"}, ORGANIZATION)})
    report = check_governance([tmp_path / "apps"])
    assert [f.rule for f in report.failures()] == ["schema", "schema"]


# @verifies REQ-0012
def test_allow_all_and_deny_all_in_one_layer_contradict(tmp_path):
    _app(
        tmp_path,
        "a",
        {"ds.a": _rule("internal", PERSON, ALLOW_ORGANIZATION, DENY_ORGANIZATION)},
    )
    report = check_governance([tmp_path / "apps"])
    assert _kinds(report, ERROR) == [("layer-verdict", "ds.a")]


# @verifies REQ-0012
def test_an_unknown_handler_is_refused_unless_named(tmp_path):
    plugin = {"handler": "my_plugin", "binds": "organization", "args": {}}
    _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON, plugin)})
    assert _kinds(check_governance([tmp_path / "apps"]), ERROR) == [("handler", "ds.a")]
    assert check_governance([tmp_path / "apps"], handlers=["my_plugin"]).ok(strict=True)


# @verifies REQ-0012
def test_a_builtin_in_the_wrong_layer_is_refused(tmp_path):
    unbound = {"handler": "organization_match", "args": {"column": "community_id"}}
    _app(tmp_path, "a", {"ds.a": _rule("internal", unbound, ALLOW_ORGANIZATION)})
    [finding] = check_governance([tmp_path / "apps"]).failures()
    assert finding.rule == "handler" and "binds person" in finding.detail


# --- REQ-0013: consent coherence on dataspace offers -------------------------------


def _offer(*filters: dict, **extra) -> dict:
    dataspace = {"expose": True, **extra.pop("dataspace", {})}
    return {**_rule("internal", *filters), "dataspace": dataspace, **extra}


# @verifies REQ-0013
def test_a_person_filter_on_impersonal_data_is_a_contradiction(tmp_path):
    _app(tmp_path, "a", {"ds.a": _offer(PERSON, ORGANIZATION)})
    assert _kinds(check_governance([tmp_path / "apps"]), ERROR) == [
        ("consent-coherence", "ds.a")
    ]


# @verifies REQ-0013
def test_a_consented_person_filter_is_coherent(tmp_path):
    _app(
        tmp_path,
        "a",
        {"ds.a": _offer(PERSON, ORGANIZATION, dataspace={"consent_required": True})},
    )
    assert check_governance([tmp_path / "apps"]).ok(strict=True)


# @verifies REQ-0013
def test_allow_all_is_not_a_person_filter(tmp_path):
    _app(tmp_path, "a", {"ds.a": _offer(ALLOW_PERSON, ORGANIZATION)})
    assert check_governance([tmp_path / "apps"]).ok(strict=True)
    _app(tmp_path, "a", {"ds.a": _offer(ALLOW_PERSON, ORGANIZATION, classification="pii")})
    assert _kinds(check_governance([tmp_path / "apps"]), ERROR) == [
        ("consent-coherence", "ds.a")
    ]


# @verifies REQ-0013
def test_personal_data_behind_person_deny_all_is_coherent(tmp_path):
    deny_person = {"handler": "deny_all", "binds": "person"}
    _app(tmp_path, "a", {"ds.a": _offer(deny_person, ORGANIZATION, classification="pii")})
    assert check_governance([tmp_path / "apps"]).ok(strict=True)


# @verifies REQ-0013
def test_consent_is_checked_only_on_dataspace_offers(tmp_path):
    _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON, ORGANIZATION)})
    assert check_governance([tmp_path / "apps"]).ok(strict=True)


# --- REQ-0014: the owners registry -------------------------------------------------


# @verifies REQ-0014
def test_ownership_names_resolve_against_the_owners(tmp_path):
    owners = _write(tmp_path / "owners.yaml", OWNERS)
    ok = {"ownership": [{"name": "rec", "type": "DATA_OWNER"}]}
    bad = {"ownership": [{"name": "nobody", "type": "DATA_OWNER"}]}
    _app(tmp_path, "a", {"ds.a": {**_rule("internal", PERSON, ORGANIZATION), **ok}})
    assert check_governance([tmp_path / "apps"], owners_files=[owners]).ok(strict=True)
    _app(tmp_path, "a", {"ds.a": {**_rule("internal", PERSON, ORGANIZATION), **bad}})
    [finding] = check_governance([tmp_path / "apps"], owners_files=[owners]).failures()
    assert finding.rule == "owners" and "'nobody'" in finding.detail


# @verifies REQ-0014
def test_each_owners_file_is_strict(tmp_path):
    """The sync-orgs loader: a partial overlay is refused; pass the merged registry."""
    _app(tmp_path, "a", {"ds.a": _rule("internal", PERSON, ORGANIZATION)})
    base = _write(tmp_path / "owners.yaml", OWNERS)
    partial = _write(tmp_path / "owners.env.yaml", {"owners": [{"id": "example-rec"}]})
    typo = _write(
        tmp_path / "owners.typo.yaml",
        {"owners": [{**OWNERS["owners"][0], "organisation": {}}]},
    )
    for extra in (partial, typo):
        report = check_governance([tmp_path / "apps"], owners_files=[base, extra])
        assert {f.rule for f in report.failures()} == {"owners"}


# @verifies REQ-0014
def test_the_dataspace_keys_validate_when_well_formed():
    """D19: optional, but defined — a typo inside them is still refused."""
    entry = {
        **OWNERS["owners"][0],
        "did": "did:web:rec.example.org",
        "status": "verified",
        "verified_by": "operator",
        "evidence_ref": "ticket-1",
        "collects_consent": True,
        "dataspace": {
            "legal_name": "Example REC",
            "registration_number": "ex-00001",
            "hq_country_code": "IT",
            "roles": ["Provider"],
            "dsp_address": "https://rec.example.org/protocol",
            "accepted": {"agreement": "terms", "version": "1", "locale": "en"},
        },
    }
    validate_owners({"owners": [entry]})
    for bad in (
        {**entry, "status": "approved"},
        {**entry, "dataspace": {**entry["dataspace"], "legal_nmae": "x"}},
    ):
        with pytest.raises(ValueError):
            validate_owners({"owners": [bad]})


# @verifies REQ-0014
def test_an_organization_bound_offer_needs_its_owner_did(tmp_path):
    owners = _write(tmp_path / "owners.yaml", OWNERS)
    with_did = _write(
        tmp_path / "owners.did.yaml",
        {"owners": [{**OWNERS["owners"][0], "did": "did:web:rec.example.org"}]},
    )
    owned = {"ownership": [{"name": "rec", "type": "DATA_OWNER"}]}
    _app(tmp_path, "a", {"ds.a": {**_offer(ALLOW_PERSON, ORGANIZATION), **owned}})
    apps = [tmp_path / "apps"]
    assert check_governance(apps, owners_files=[owners]).ok(strict=True)
    [finding] = check_governance(apps, owners_files=[owners], dataspace=True).failures()
    assert finding.rule == "owners" and "no did" in finding.detail
    assert check_governance(apps, owners_files=[with_did], dataspace=True).ok(strict=True)
    assert not check_governance(apps, dataspace=True).ok()
