"""The owner registry: aliases, the Keycloak block, and counting owners.

Each test here corresponds to a way one of the five previous copies got it
wrong, so they are regression tests rather than API exercises.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from celine.governance import (
    GovernanceValidationError,
    OwnerEntry,
    OwnersRegistry,
    load_owners_yaml,
    validate_owners,
)

# A registry shaped like a real deployment: generic placeholders carried by the
# open-source pipelines, resolved to the deployment's actual organisations.
OWNERS_YAML = textwrap.dedent(
    """
    owners:
      - id: greenland
        type: schema:NGO
        name: Greenland Soc. Coop.
        url: https://www.greenland.it
        organization:
          create: true
          role: rec
          attributes:
            country: IT
        aliases: [rec]

      - id: set-distribuzione
        type: schema:Corporation
        name: SET Distribuzione S.p.A.
        url: https://www.setdistribuzione.it
        did: did:web:set.dataspaces.localhost
        organization:
          create: true
          role: dso
        aliases: [dso]

      - id: spxl
        type: schema:Corporation
        name: Spindox Labs srl
        url: https://spindoxlabs.com/en
    """
)


@pytest.fixture()
def registry(tmp_path: Path) -> OwnersRegistry:
    path = tmp_path / "owners.yaml"
    path.write_text(OWNERS_YAML, encoding="utf-8")
    return load_owners_yaml(path)


# ---------------------------------------------------------------------------
# aliases
# ---------------------------------------------------------------------------


def test_alias_resolves_to_the_deployments_owner(registry: OwnersRegistry) -> None:
    """The whole point: open-source files say `rec`, the deployment decides who.

    Two of the five copies lacked `aliases` entirely, so this returned None and
    the caller fell back to a synthetic `urn:owner:rec` — which is not a
    resolvable identifier and never matched the DCAT formatter's URI index.
    """
    assert registry.canonical_uri("rec") == "https://www.greenland.it"
    assert registry.by_id("rec").id == "greenland"


def test_did_outranks_url(registry: OwnersRegistry) -> None:
    assert registry.canonical_uri("dso") == "did:web:set.dataspaces.localhost"


def test_an_id_is_never_shadowed_by_someone_elses_alias() -> None:
    """`spxl` stays itself even though another entry claims it as an alias."""
    reg = OwnersRegistry(
        [
            OwnerEntry(id="impostor", url="https://impostor.example", aliases=["spxl"]),
            OwnerEntry(id="spxl", url="https://spindoxlabs.com/en"),
        ]
    )
    assert reg.canonical_uri("spxl") == "https://spindoxlabs.com/en"


def test_alias_precedence_does_not_depend_on_file_order() -> None:
    """Same two entries, opposite order, same answer.

    Aliases are registered only once every id is known, so an owner declared
    after the entry that claims its name is still safe.
    """
    entries = [
        OwnerEntry(id="spxl", url="https://spindoxlabs.com/en"),
        OwnerEntry(id="impostor", url="https://impostor.example", aliases=["spxl"]),
    ]
    assert OwnersRegistry(entries).canonical_uri("spxl") == "https://spindoxlabs.com/en"
    assert OwnersRegistry(list(reversed(entries))).canonical_uri("spxl") == (
        "https://spindoxlabs.com/en"
    )


def test_conflicting_alias_is_resolved_by_first_claim_not_silently() -> None:
    """The schema says aliases are unique but cannot enforce it across entries.

    Without this the winner depended on file order, silently and differently per
    deployment. First claim wins, and the loser is logged.
    """
    reg = OwnersRegistry(
        [
            OwnerEntry(id="first", url="https://first.example", aliases=["rec"]),
            OwnerEntry(id="second", url="https://second.example", aliases=["rec"]),
        ]
    )
    assert reg.by_id("rec").id == "first"


def test_aliases_map_is_reported_for_diagnostics(registry: OwnersRegistry) -> None:
    assert registry.aliases() == {"rec": "greenland", "dso": "set-distribuzione"}


# ---------------------------------------------------------------------------
# counting
# ---------------------------------------------------------------------------


def test_len_counts_owners_not_lookup_keys(registry: OwnersRegistry) -> None:
    """Three owners and two aliases used to report five.

    The export CLI prints this number, so it read "Loaded 16 owner(s)" for a
    fourteen-owner deployment registry.
    """
    assert len(registry) == 3
    assert len(registry.all()) == 3
    assert {e.id for e in registry.all()} == {"greenland", "set-distribuzione", "spxl"}


def test_membership_covers_ids_and_aliases(registry: OwnersRegistry) -> None:
    assert "greenland" in registry
    assert "rec" in registry
    assert "nobody" not in registry


# ---------------------------------------------------------------------------
# the Keycloak block
# ---------------------------------------------------------------------------


def test_organization_block_survives_loading(registry: OwnersRegistry) -> None:
    """`extra="ignore"` used to drop it, leaving policies with nothing to read."""
    org = registry.by_id("greenland").organization
    assert org is not None
    assert org.create is True
    assert org.role == "rec"
    assert org.attributes == {"country": "IT"}


def test_owner_without_organization_has_no_kc_org(registry: OwnersRegistry) -> None:
    assert registry.by_id("greenland").has_kc_org is True
    assert registry.by_id("spxl").has_kc_org is False


def test_identity_registry_spelling_is_accepted() -> None:
    """The IR database column — and so its API — says `organization_config`.

    One model reads both a seed file and a live IR response. `ds` had a model
    that knew only the IR spelling and silently discarded the block when it
    loaded a YAML.
    """
    entry = OwnerEntry.model_validate(
        {"id": "greenland", "organization_config": {"create": True, "role": "rec"}}
    )
    assert entry.has_kc_org is True
    assert entry.organization.role == "rec"
    # Dumping returns the YAML spelling, which is what governance tooling writes.
    assert "organization" in entry.model_dump()


# ---------------------------------------------------------------------------
# role
# ---------------------------------------------------------------------------


def test_role_is_distinct_from_the_keycloak_role() -> None:
    """Two fields named `role`, two different questions.

    `role` is the party's relationship to the data; `organization.role` is the
    Keycloak organization type. Nothing consumes `role` yet — it round-trips so
    that declaring it now does not have to wait for the consumer.
    """
    entry = OwnerEntry.model_validate(
        {
            "id": "greenland",
            "role": "controller",
            "organization": {"create": True, "role": "rec"},
        }
    )
    assert entry.role == "controller"
    assert entry.organization.role == "rec"


def test_role_defaults_to_unset() -> None:
    assert OwnerEntry(id="spxl").role is None


# ---------------------------------------------------------------------------
# uri lookup
# ---------------------------------------------------------------------------


def test_by_uri_finds_both_did_and_url(registry: OwnersRegistry) -> None:
    """The DCAT formatter looks up whatever URI was persisted on the entry."""
    assert registry.by_uri("https://www.greenland.it").id == "greenland"
    assert registry.by_uri("did:web:set.dataspaces.localhost").id == "set-distribuzione"
    assert registry.by_uri("https://www.setdistribuzione.it").id == "set-distribuzione"
    assert registry.by_uri("urn:owner:rec") is None


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------


def test_missing_file_raises_unless_allowed(tmp_path: Path) -> None:
    """The two implementations disagreed; the caller now chooses."""
    missing = tmp_path / "nope.yaml"
    with pytest.raises(FileNotFoundError):
        load_owners_yaml(missing)
    assert len(load_owners_yaml(missing, missing_ok=True)) == 0


def test_validate_on_load_catches_what_the_model_tolerates(tmp_path: Path) -> None:
    """A misspelled key is dropped by `extra="ignore"` and the owner loses its URI.

    This is the failure mode strictness exists for: parsing succeeds, the entry
    looks fine, and `canonical_uri` returns None — so the exporter falls back to
    a synthetic `urn:owner:` that no DCAT consumer can dereference.
    """
    path = tmp_path / "owners.yaml"
    path.write_text(
        "owners:\n"
        "  - id: greenland\n"
        "    type: schema:NGO\n"
        "    name: Greenland Soc. Coop.\n"
        "    urls: https://www.greenland.it\n",
        encoding="utf-8",
    )

    lenient = load_owners_yaml(path)
    assert lenient.canonical_uri("greenland") is None  # silently identifier-less

    with pytest.raises(GovernanceValidationError) as exc:
        load_owners_yaml(path, validate=True)
    assert "urls" in str(exc.value)


def test_validate_owners_accepts_a_real_registry() -> None:
    import yaml

    validate_owners(yaml.safe_load(OWNERS_YAML), source="<test>")


def test_validate_owners_rejects_an_unknown_schema_type() -> None:
    with pytest.raises(GovernanceValidationError) as exc:
        validate_owners(
            {"owners": [{"id": "x", "type": "schema:Robot", "name": "X"}]},
            source="<test>",
        )
    assert "type" in str(exc.value)


def test_validate_owners_accepts_the_role_field() -> None:
    """`additionalProperties: false` means an undeclared field is a hard failure."""
    validate_owners(
        {"owners": [{"id": "x", "type": "schema:NGO", "name": "X", "role": "publisher"}]},
        source="<test>",
    )
