from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Dict, Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


PipelineTaskStatus = Literal["success", "failed"]


@dataclass
class PipelineTaskResult:
    """
    Result structure returned by PipelineRunner execution methods.
    Matches the previous dict format but is now typed and structured.
    """

    command: str
    status: PipelineTaskStatus
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
