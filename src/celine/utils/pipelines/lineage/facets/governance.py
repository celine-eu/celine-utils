# src/celine/utils/pipelines/lineage/facets/governance.py
"""
Custom OpenLineage facet for governance metadata.
"""

from typing import List, Optional

import attr
from openlineage.client.facet import BaseFacet

SCHEMA_URL = "https://celine-eu.github.io/schema/GovernanceDatasetFacet.schema.json"


@attr.s(auto_attribs=True)
class GovernanceDatasetFacet(BaseFacet):
    """
    Custom dataset facet that encodes governance metadata.

    This follows the OpenLineage custom facet rules:
      - extends BaseFacet
      - will be emitted under the key "governance"
      - includes _producer and _schemaURL when serialized via BaseFacet
    """

    @staticmethod
    def _get_schema():
        return SCHEMA_URL

    title: Optional[str] = None
    description: Optional[str] = None
    license: Optional[str] = None
    attribution: Optional[str] = None
    owners: Optional[List[str]] = None
    accessLevel: Optional[str] = None  # open / internal / restricted / secret
    accessRequirements: Optional[str] = (
        None  # textual policy (e.g. "public", "internal_use")
    )
    classification: Optional[str] = None  # e.g. green/yellow/red or DLP class
    tags: Optional[List[str]] = None
    retentionDays: Optional[int] = None
    documentationUrl: Optional[str] = None
    sourceSystem: Optional[str] = None
    userFilterColumn: Optional[str] = None  # NEW: column for row-level user filtering
