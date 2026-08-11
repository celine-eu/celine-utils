"""Deprecated re-export of :mod:`celine.governance`.

The governance grammar moved to ``celine.governance`` so that a consumer can
parse ``governance.yaml`` without installing dbt, Meltano, Prefect or Keycloak —
which is why ``dataset-api``, ``ds`` and ``celine-superset`` each wrote their own
parser instead of importing this one.

Import from ``celine.governance`` directly. This module is kept so that a
deployed pipeline pinning an older ``celine-utils`` does not break on upgrade,
and will be removed once no caller remains.

Note the behaviour change that came with the move: overlays now merge by
``exclude_unset`` rather than truthiness, so a dataset can override an inherited
value with ``false``/``null``. That was previously impossible and is the defect
the consolidation existed to fix — see :func:`celine.governance.merge.merge_models`.
"""

from __future__ import annotations

import warnings

from celine.governance import (  # noqa: F401
    DataspaceConfig,
    DcatConfig,
    GovernanceConfig,
    GovernanceOwner,
    GovernanceResolver,
    GovernanceRule,
    OntologyConfig,
    TemporalCoverage,
)

warnings.warn(
    "celine.utils.pipelines.governance is deprecated; import from celine.governance instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "GovernanceRule",
    "GovernanceConfig",
    "GovernanceOwner",
    "GovernanceResolver",
    "DcatConfig",
    "OntologyConfig",
    "DataspaceConfig",
    "TemporalCoverage",
]
