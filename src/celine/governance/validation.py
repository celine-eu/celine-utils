"""Validate a governance file against its JSON Schema.

Until this module existed, ``governance.schema.json`` was **published but never
executed** — no code in ``celine-utils``, ``dataset-api`` or ``celine-superset``
loaded it. It described what the models ought to accept and was checked by
nobody, so the two could drift, and both failed open:

- ``$defs.governanceBlock`` has no ``additionalProperties: false``, so an unknown
  key validates.
- The models use ``extra="ignore"``, so an unknown key is dropped.

A misspelled ``access_levl: open`` was therefore accepted by the schema,
discarded by the model, and the dataset silently took the default.

Schemas are read from the installed package via ``importlib.resources``, never a
path walk from ``__file__`` — the latter breaks the moment the code runs from a
wheel or a container.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any, Dict, List

import yaml

from celine.governance.models import KNOWN_KEYS, KNOWN_ROOT_KEYS

logger = logging.getLogger(__name__)

GOVERNANCE_SCHEMA = "governance.schema.json"
OWNERS_SCHEMA = "owners.schema.json"
FACET_SCHEMA = "GovernanceDatasetFacet.schema.json"


class GovernanceValidationError(ValueError):
    """A governance file did not conform to its schema."""

    def __init__(self, source: str, errors: List[str]):
        self.source = source
        self.errors = errors
        detail = "\n".join(f"  - {e}" for e in errors)
        super().__init__(f"{source} failed governance validation:\n{detail}")


@lru_cache(maxsize=None)
def load_schema(name: str = GOVERNANCE_SCHEMA) -> Dict[str, Any]:
    """Load a packaged JSON Schema by filename."""
    text = resources.files("celine.governance.schema").joinpath(name).read_text("utf-8")
    return json.loads(text)


def schema_errors(data: Dict[str, Any], schema_name: str = GOVERNANCE_SCHEMA) -> List[str]:
    """Return every schema violation in ``data``, sorted by document position.

    Every error, not the first: a governance file is edited by hand and reporting
    one problem per run turns a five-minute fix into five round trips.
    """
    import jsonschema

    validator_cls = jsonschema.validators.validator_for(load_schema(schema_name))
    validator = validator_cls(load_schema(schema_name))
    return [
        f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path))
    ]


def unknown_keys(data: Dict[str, Any]) -> List[str]:
    """Report keys the grammar does not define, at the root and per block.

    Separate from :func:`schema_errors` because the schema cannot catch these —
    ``governanceBlock`` permits additional properties, and tightening it is a
    breaking change to seventeen files that have never been checked. This gives
    the warning without the breakage.

    The **root** scan exists for the same reason one layer up. The root object
    also permits additional properties, and the parser reads its three keys by
    name, so a misspelled ``depends-on:`` is accepted by the schema, ignored by
    the parser, and reported by nobody — the file validates, the dependency graph
    comes out empty, and nothing connects the two. Two key sets rather than one,
    because they guard different scopes: :data:`KNOWN_ROOT_KEYS` the document,
    :data:`KNOWN_KEYS` a block within it.
    """
    found: List[str] = []

    for key in data:
        if key not in KNOWN_ROOT_KEYS:
            found.append(f"<root>: {key}")

    def scan(block: Any, where: str) -> None:
        if not isinstance(block, dict):
            return
        inner = block.get("governance") if "governance" in block else block
        if not isinstance(inner, dict):
            return
        for key in inner:
            if key not in KNOWN_KEYS:
                found.append(f"{where}: {key}")

    scan(data.get("defaults") or {}, "defaults")
    for pattern, rule in (data.get("sources") or {}).items():
        scan(rule or {}, f"sources/{pattern}")
    return found


def validate(
    data: Dict[str, Any],
    *,
    source: str = "<dict>",
    strict: bool = False,
) -> List[str]:
    """Validate a parsed governance document.

    Schema violations always raise. Unknown keys **warn** by default and raise
    under ``strict``.

    The default is not timidity. Seventeen governance files have never been
    schema-checked, and making unknown keys fatal on the day validation arrives
    turns adopting this package into a seventeen-file cleanup discovered at
    import time — in an exporter, in CI, at the worst moment. Warn, fix, then
    flip the default.
    """
    errors = schema_errors(data)
    if errors:
        raise GovernanceValidationError(source, errors)

    unknown = unknown_keys(data)
    if unknown:
        if strict:
            raise GovernanceValidationError(
                source, [f"unknown key {u}" for u in unknown]
            )
        for u in unknown:
            logger.warning("%s: unknown governance key %s — ignored", source, u)
    return unknown


def validate_file(path: Path, *, strict: bool = False) -> List[str]:
    """Load and validate a ``governance.yaml``."""
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return validate(data, source=str(path), strict=strict)


def validate_owners(data: Dict[str, Any], *, source: str = "<dict>") -> None:
    """Validate a parsed ``owners.yaml`` against ``owners.schema.json``.

    Unlike :func:`validate` there is no lenient mode: ``owners.schema.json`` sets
    ``additionalProperties: false`` and constrains ``type`` to an enum, so it can
    say precisely what is wrong, and an owners file is short enough that fixing
    it is not a migration.

    The strictness earns its keep downstream. An entry missing ``id`` is skipped
    without comment by ``celine-policies``' loader, so a typo in a registry does
    not fail — it quietly produces one fewer Keycloak organization, and the
    missing org surfaces later as an authorization failure with no obvious cause.
    """
    errors = schema_errors(data, OWNERS_SCHEMA)
    if errors:
        raise GovernanceValidationError(source, errors)


def validate_owners_file(path: Path) -> None:
    """Load and validate an ``owners.yaml``."""
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    validate_owners(data, source=str(path))
