from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Dict, Any

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:  # pragma: no cover - covered by the 3.10 leg of the test matrix
    # `enum.StrEnum` is 3.11+. Its defining behaviour is that members compare
    # and serialise as plain strings, which `str, Enum` has always given —
    # `StrEnum` mainly adds `str()` returning the value rather than
    # "PipelineStatus.STARTED". Nothing here relies on that difference: the
    # values are consumed through `.value` or by comparison.
    from enum import Enum

    class StrEnum(str, Enum):  # type: ignore[no-redef]
        __str__ = str.__str__


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PipelineStatus(StrEnum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PipelineTaskResult:
    """
    Result structure returned by PipelineRunner execution methods.
    Matches the previous dict format but is now typed and structured.
    """

    command: str
    status: PipelineStatus
    details: Any | None = None
    timestamp: str = field(default_factory=_utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict with the canonical shape."""
        return {
            "timestamp": self.timestamp,
            "command": self.command,
            "status": self.status,
            "details": self.details,
        }
