"""Resolve ``ownership[].name`` to the organisation a deployment's registry names.

A governance file may name an owner by a **role placeholder** — ``rec``, ``dso`` —
so that an open-source pipeline ships without a customer's identifier. The
placeholder is not an owner. Every consumer that read the name as written turned
it into something nobody could match: an asset ``owner`` label, an ODRL assigner,
a recipient. This module replaces each name with the id of the owner it resolves
to, so what leaves a resolved rule is always an organisation id.

Resolution is a **post-merge** step. A deployer overlay may replace ``ownership``
outright, and resolving the base first would report placeholders the overlay has
already withdrawn. :meth:`celine.governance.resolver.GovernanceResolver.from_file`
and friends apply it after the overlay for that reason.

The lookup is anything with ``by_id(name)`` returning an object that has an ``id``
or ``None`` — :class:`celine.governance.owners.OwnersRegistry` is one, and so is a
consumer's own adapter. ``by_id`` tries ids before aliases, so a name that is
already an owner id resolves to itself.

**Placeholders are a deployment's map, not the owners file's.** ``placeholders``
(``{"rec": "example-rec"}``) is supplied separately, from the deployment's own
local configuration, so an owners file lists organisations and nothing else and
the public governance files keep their generic placeholders. ``aliases:`` in an
owners file still resolves while it exists; it is on its way out. An owner id
always wins over a placeholder of the same name, as it wins over an alias, so a
map cannot take an organisation's name away from it.

``strict`` has no default on purpose: whether an unresolved name fails the load
or is kept as written with a warning is not decided yet, and a caller stating it
keeps both answers open.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Protocol, TypeVar

from celine.governance.models import GovernanceConfig, GovernanceOwner, GovernanceRule

logger = logging.getLogger(__name__)

R = TypeVar("R", bound=GovernanceRule)
C = TypeVar("C", bound=GovernanceConfig)

#: The location name :func:`unresolved_owners` uses for the file's ``defaults``.
DEFAULTS = "defaults"


class OwnerLookup(Protocol):
    """What resolution needs from an owners registry."""

    def by_id(self, alias: str) -> Optional[Any]: ...


class UnresolvedOwnerError(ValueError):
    """One or more ``ownership[].name`` values name no owner in the registry.

    ``names`` maps each unresolved name to the places it was declared
    (``defaults`` or a ``sources`` key) — every one of them, not the first, so a
    deployment fixes its owners file in one pass.
    """

    def __init__(self, names: Dict[str, List[str]]):
        self.names = names
        detail = "\n".join(
            f"  - {name!r} in {', '.join(where)}" for name, where in sorted(names.items())
        )
        super().__init__(f"ownership names no owner in the registry:\n{detail}")


class _WithPlaceholders:
    """``owners``, consulted through a deployment's placeholder map first."""

    def __init__(self, owners: OwnerLookup, placeholders: Mapping[str, str]):
        self._owners = owners
        self._placeholders = placeholders

    def by_id(self, alias: str) -> Optional[Any]:
        entry = self._owners.by_id(alias)
        if entry is not None and getattr(entry, "id", None) == alias:
            if alias in self._placeholders:
                logger.warning(
                    "Placeholder %r is also an owner id — the owner id wins", alias
                )
            return entry
        target = self._placeholders.get(alias)
        if target is None:
            return entry
        return self._owners.by_id(target)


def _lookup(
    owners: OwnerLookup, placeholders: Optional[Mapping[str, str]]
) -> OwnerLookup:
    return _WithPlaceholders(owners, placeholders) if placeholders else owners


def _owner_id(name: str, owners: OwnerLookup) -> Optional[str]:
    entry = owners.by_id(name)
    return None if entry is None else getattr(entry, "id", None)


def _resolve(rule: R, owners: OwnerLookup) -> tuple[R, List[str]]:
    """The rule with every resolvable name replaced, and the names that were not."""
    if not rule.ownership:
        return rule, []

    unresolved: List[str] = []
    resolved: List[GovernanceOwner] = []
    seen: set[tuple[str, str]] = set()
    for owner in rule.ownership:
        owner_id = _owner_id(owner.name, owners)
        if owner_id is None:
            if owner.name not in unresolved:
                unresolved.append(owner.name)
            new = owner
        elif owner_id == owner.name:
            new = owner
        else:
            new = owner.model_copy(update={"name": owner_id})
        # A file naming both `rec` and the id it stands for lists one owner
        # twice once resolved; only an exact repeat is dropped, so a second
        # `type` for the same organisation is kept as the file stated it.
        key = (new.name, new.type)
        if key in seen:
            continue
        seen.add(key)
        resolved.append(new)

    unchanged = len(resolved) == len(rule.ownership) and all(
        a is b for a, b in zip(resolved, rule.ownership)
    )
    if unchanged:
        return rule, unresolved
    # `model_copy(update=...)` keeps the rule's class — a consumer's subclass
    # survives — and marks `ownership` set, which it already was: a rule whose
    # ownership was never declared returned above.
    return rule.model_copy(update={"ownership": resolved}), unresolved


def resolve_ownership(
    rule: R,
    owners: OwnerLookup,
    *,
    strict: bool,
    placeholders: Optional[Mapping[str, str]] = None,
) -> R:
    """Return ``rule`` with each ``ownership[].name`` replaced by its owner id.

    ``placeholders`` maps a generic name to an owner id and is consulted after
    the owner ids and before the registry's aliases.

    The input is not modified. ``type`` is kept as written. A name that resolves
    to nothing raises :class:`UnresolvedOwnerError` under ``strict``; otherwise
    it is kept as written and a warning is logged.
    """
    resolved, unresolved = _resolve(rule, _lookup(owners, placeholders))
    if unresolved:
        _report({name: ["<rule>"] for name in unresolved}, strict=strict)
    return resolved


def resolve_config_ownership(
    config: C,
    owners: OwnerLookup,
    *,
    strict: bool,
    placeholders: Optional[Mapping[str, str]] = None,
) -> C:
    """Resolve ``defaults`` and every ``sources`` rule of a whole config.

    Under ``strict`` every unresolved name in the file is collected before
    raising, not only the first.
    """
    missing: Dict[str, List[str]] = {}
    owners = _lookup(owners, placeholders)

    def note(where: str, names: List[str]) -> None:
        for name in names:
            missing.setdefault(name, []).append(where)

    defaults, names = _resolve(config.defaults, owners)
    note(DEFAULTS, names)

    sources = {}
    for key, rule in config.sources.items():
        sources[key], names = _resolve(rule, owners)
        note(key, names)

    if missing:
        _report(missing, strict=strict)

    if defaults is config.defaults and all(
        sources[k] is config.sources[k] for k in sources
    ):
        return config
    return config.model_copy(update={"defaults": defaults, "sources": sources})


def unresolved_owners(
    config: GovernanceConfig,
    owners: OwnerLookup,
    *,
    placeholders: Optional[Mapping[str, str]] = None,
) -> Dict[str, List[str]]:
    """Every ``ownership[].name`` that resolves to no owner, and where it is declared.

    Returns ``{name: [location, ...]}`` where a location is ``"defaults"`` or a
    ``sources`` key. An empty dict means every name resolves. Reports rather than
    raises, so a checker can collect findings across many files.
    """
    missing: Dict[str, List[str]] = {}
    owners = _lookup(owners, placeholders)
    rules = [(DEFAULTS, config.defaults), *config.sources.items()]
    for where, rule in rules:
        for owner in rule.ownership:
            if _owner_id(owner.name, owners) is None:
                places = missing.setdefault(owner.name, [])
                if where not in places:
                    places.append(where)
    return missing


def _report(missing: Dict[str, List[str]], *, strict: bool) -> None:
    if strict:
        raise UnresolvedOwnerError(missing)
    for name, where in sorted(missing.items()):
        logger.warning(
            "Ownership name %r (%s) resolves to no owner — kept as written",
            name,
            ", ".join(where),
        )
