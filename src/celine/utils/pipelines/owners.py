"""Deprecated re-export of :mod:`celine.governance.owners`.

Moved so that reading an ``owners.yaml`` does not require the pipeline stack.
Import from ``celine.governance`` directly; this module will be removed once no
caller remains.
"""

from __future__ import annotations

import warnings

from celine.governance.owners import (  # noqa: F401
    OwnerEntry,
    OwnersRegistry,
    load_owners_yaml,
)

warnings.warn(
    "celine.utils.pipelines.owners is deprecated; import from celine.governance instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["OwnerEntry", "OwnersRegistry", "load_owners_yaml"]
