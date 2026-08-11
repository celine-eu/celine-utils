"""Deprecated re-export of :mod:`celine.governance.levels`."""

from __future__ import annotations

from celine.governance.levels import (  # noqa: F401
    AccessRequirement,
    DataClassification,
    GovernanceAccessLevel,
    normalize_access_level,
    normalize_classification,
)

__all__ = [
    "AccessRequirement",
    "GovernanceAccessLevel",
    "DataClassification",
    "normalize_access_level",
    "normalize_classification",
]
