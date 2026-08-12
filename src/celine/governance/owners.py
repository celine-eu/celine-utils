"""Owner identity registry.

Maps the short alias strings used in governance.yaml ``ownership`` blocks to
canonical machine-readable identifiers (DID or URL) and rich metadata.

There were five copies of this before it was consolidated here — ``celine-utils``,
``dataset-api``, ``celine-superset``, ``celine-policies`` and ``ds`` — and they
disagreed on more than style: two lacked ``aliases`` entirely, so the generic
placeholders that open-source pipelines carry never resolved to the deployment's
real organisations; ``ds`` named the Keycloak block ``organization_config`` after
the identity-registry column, so reading a YAML silently dropped it.

Two fields are named ``role`` and they are **not** the same thing:

- :attr:`OwnerEntry.role` — the party's role with respect to the *data*
  (``publisher``, ``controller``, …). Declarative only; nothing consumes it yet.
- :attr:`OwnerOrganization.role` — the *Keycloak* organization role, emitted as
  ``attributes.type`` on the KC org by ``celine-policies keycloak sync-orgs``.

The ontology for ``type`` follows Schema.org (https://schema.org/) which is the
most broadly understood vocabulary and aligns with DCAT-AP's use of
``foaf:Agent`` for publishers — Schema.org types are emitted alongside
``foaf:Organization`` in JSON-LD output for full compatibility.

Common type values:
  schema:Organization           — generic fallback
  schema:Corporation            — for-profit company / srl / ltd
  schema:GovernmentOrganization — public authority / ministry / agency
  schema:ResearchOrganization   — university, institute, research centre
  schema:NGO                    — non-governmental / non-profit organisation
  schema:Project                — project consortium without separate legal entity
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class OwnerOrganization(BaseModel):
    """Keycloak organization provisioning block.

    Read by ``celine-policies keycloak sync-orgs``: entries with ``create: true``
    are provisioned as Keycloak organizations, ``role`` becomes the KC
    ``attributes.type``, and ``attributes`` are set alongside it.
    """

    model_config = ConfigDict(extra="ignore")

    create: bool = False
    role: Optional[str] = None
    attributes: Dict[str, str] = Field(default_factory=dict)


class OwnerEntry(BaseModel):
    """Canonical identity record for a governance owner alias.

    Fields
    ------
    id      : alias used in governance.yaml ``ownership`` blocks (e.g. ``spxl``)
    type    : Schema.org type CURIE — governs the ``@type`` emitted in JSON-LD
    name    : human-readable display name
    role    : the party's role with respect to the data (``publisher``,
              ``controller``, …). **Declared but not yet consumed** — see the
              module docstring for why it is not ``organization.role``
    did     : ``did:web:`` URI when the owner operates a dataspace connector
    url     : canonical homepage URI — used as publisher URI when no DID is set
    aliases : alternative lookup keys, so an open-source governance file can say
              ``dso`` and a deployment's registry decides who that is
    organization : Keycloak provisioning block, if any

    ``organization`` also accepts ``organization_config``: that is the column
    name in the identity-registry database and therefore the key in its API
    responses, while the YAML files say ``organization``. Accepting both lets one
    model read a seed file and a live IR response — ``ds`` previously had a model
    that only knew the IR spelling and silently discarded the block when loading
    a YAML.

    ``extra="ignore"`` is deliberate: strictness belongs to
    :func:`celine.governance.validation.validate_owners`, which checks against
    ``owners.schema.json`` (``additionalProperties: false``). Enforcing it in the
    model too would make an IR response with a new field fail to parse in
    consumers that only wanted the URI.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str
    type: str = "schema:Organization"
    name: Optional[str] = None
    role: Optional[str] = None
    did: Optional[str] = None
    url: Optional[str] = None
    aliases: List[str] = Field(default_factory=list)
    organization: Optional[OwnerOrganization] = Field(
        default=None,
        validation_alias=AliasChoices("organization", "organization_config"),
    )

    @property
    def canonical_uri(self) -> Optional[str]:
        """DID takes priority over URL as the published identifier."""
        return self.did or self.url

    @property
    def has_kc_org(self) -> bool:
        return self.organization is not None and self.organization.create


class OwnersRegistry:
    """Loaded registry supporting O(1) lookup by id, alias, or canonical URI.

    Ids and aliases are held in **separate** maps. That is not bookkeeping: an
    earlier version merged them, so ``len(registry)`` counted aliases as owners
    and the export CLI reported "Loaded 16 owner(s)" for a fourteen-owner file.
    Keeping them apart also makes the precedence explicit — an id always wins
    over an alias, so a deployment cannot lose an owner by another owner
    claiming its name as an alias.
    """

    def __init__(self, entries: Optional[List[OwnerEntry]] = None) -> None:
        entries = list(entries or [])

        self._by_id: Dict[str, OwnerEntry] = {}
        for e in entries:
            if e.id in self._by_id:
                logger.warning("Duplicate owner id %r — later entry wins", e.id)
            self._by_id[e.id] = e

        # Aliases are registered only after every id is known, so the id/alias
        # precedence does not depend on the order entries appear in the file.
        self._by_alias: Dict[str, OwnerEntry] = {}
        for e in entries:
            for alias in e.aliases:
                if alias in self._by_id:
                    logger.warning(
                        "Owner alias %r (from %r) shadows an owner id — ignoring the alias",
                        alias,
                        e.id,
                    )
                    continue
                claimed = self._by_alias.get(alias)
                if claimed is not None and claimed.id != e.id:
                    # owners.schema.json says aliases must be unique but cannot
                    # express it, so resolution would otherwise depend on file
                    # order — silently, and differently per deployment.
                    logger.warning(
                        "Owner alias %r claimed by both %r and %r — keeping %r",
                        alias,
                        claimed.id,
                        e.id,
                        claimed.id,
                    )
                    continue
                self._by_alias[alias] = e

        # Indexed by DID and URL so the DCAT formatter can look up stored URIs.
        self._by_uri: Dict[str, OwnerEntry] = {}
        for e in entries:
            if e.did:
                self._by_uri[e.did] = e
            if e.url:
                self._by_uri[e.url] = e

    def by_id(self, alias: str) -> Optional[OwnerEntry]:
        """Look up by the key used in governance.yaml: id first, then alias."""
        return self._by_id.get(alias) or self._by_alias.get(alias)

    def by_uri(self, uri: str) -> Optional[OwnerEntry]:
        """Look up by canonical URI (DID or URL) — used by the DCAT formatter."""
        return self._by_uri.get(uri)

    def canonical_uri(self, alias: str) -> Optional[str]:
        """Return the canonical URI for an id or alias: DID over URL."""
        entry = self.by_id(alias)
        return entry.canonical_uri if entry else None

    def all(self) -> List[OwnerEntry]:
        """Every owner, once. Aliases are lookup keys, not owners."""
        return list(self._by_id.values())

    def aliases(self) -> Dict[str, str]:
        """Map of alias → owner id, for diagnostics and CLI output."""
        return {alias: e.id for alias, e in self._by_alias.items()}

    def __len__(self) -> int:
        """The number of owners — not the number of lookup keys."""
        return len(self._by_id)

    def __contains__(self, alias: object) -> bool:
        return isinstance(alias, str) and self.by_id(alias) is not None


def load_owners_yaml(
    path: Path,
    *,
    missing_ok: bool = False,
    validate: bool = False,
) -> OwnersRegistry:
    """Load an owners.yaml file and return an OwnersRegistry.

    Parameters
    ----------
    missing_ok
        Return an empty registry instead of raising when the file is absent.
        The two implementations this consolidates disagreed — ``celine-utils``
        raised, ``ds`` returned empty — so the choice is the caller's rather
        than a silent behaviour change for one of them.
    validate
        Check the document against ``owners.schema.json`` before parsing. Off by
        default so adopting this loader cannot turn an existing warning into a
        crash; callers that provision real identities should turn it on.

    Raises
    ------
    FileNotFoundError
        If the path does not exist and ``missing_ok`` is false.
    GovernanceValidationError
        If ``validate`` is set and the document does not conform.
    """
    if not path.exists():
        if missing_ok:
            logger.debug("No owners registry at %s — using an empty one", path)
            return OwnersRegistry()
        raise FileNotFoundError(path)

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    if validate:
        # Imported here rather than at module scope: validation pulls jsonschema,
        # and a consumer that only resolves URIs should not pay for it.
        from celine.governance.validation import validate_owners

        validate_owners(raw, source=str(path))

    entries = [OwnerEntry.model_validate(item) for item in (raw.get("owners") or [])]
    logger.debug("Loaded %d owner entries from %s", len(entries), path)
    return OwnersRegistry(entries)
