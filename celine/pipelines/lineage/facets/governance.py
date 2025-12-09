# celine/pipelines/lineage/facets/governance.py
from __future__ import annotations

from typing import List, Optional

from openlineage.client.facet import BaseFacet


class GovernanceDatasetFacet(BaseFacet):
    """
    Custom dataset facet that encodes governance metadata.

    This follows the OpenLineage custom facet rules:
      - extends BaseFacet
      - will be emitted under the key "governance"
      - includes _producer and _schemaURL when serialized via BaseFacet
    """

    _namespace = "celine"
    _schemaURL = "https://raw.githubusercontent.com/celine-eu/celine-utils/refs/heads/main/schemas/GovernanceDatasetFacet.json"

    def __init__(
        self,
        license: Optional[str] = None,
        owners: Optional[List[str]] = None,
        accessLevel: Optional[str] = None,
        accessRights: Optional[str] = None,
        classification: Optional[str] = None,
        tags: Optional[List[str]] = None,
        retentionDays: Optional[int] = None,
        documentationUrl: Optional[str] = None,
        sourceSystem: Optional[str] = None,
        **kwargs,
    ):
        # BaseFacet sets _producer + _schemaURL automatically
        super().__init__(**kwargs)

        # Fields must be instance attributes
        self.license = license
        self.owners = owners
        self.accessLevel = accessLevel
        self.accessRights = accessRights
        self.classification = classification
        self.tags = tags
        self.retentionDays = retentionDays
        self.documentationUrl = documentationUrl
        self.sourceSystem = sourceSystem

    # Note: field names are camelCase to follow OpenLineage JSON conventions.
    license: Optional[str] = None
    owners: Optional[List[str]] = None
    accessLevel: Optional[str] = None  # open / internal / restricted / secret
    accessRights: Optional[str] = None  # textual policy (e.g. "public", "internal_use")
    classification: Optional[str] = None  # e.g. green/yellow/red or DLP class
    tags: Optional[List[str]] = None
    retentionDays: Optional[int] = None
    documentationUrl: Optional[str] = None
    sourceSystem: Optional[str] = None
