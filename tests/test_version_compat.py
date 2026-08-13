"""The version shims, pinned by behaviour rather than by version check.

`celine-utils` declares support for 3.10 upward. Two things make that a claim
worth testing rather than a line in `pyproject.toml`:

- The claim was previously false in the other direction — `requires-python` said
  `>=3.12` and nothing needed it — so the floor has no history of being checked.
- What breaks a floor is usually an *import*, not a failing assertion. Nine of
  this package's 69 modules failed to import on 3.10 while the whole suite
  passed, because no test imported them. The CI matrix sweeps every submodule
  for that; this file pins the semantics the sweep cannot see.
"""
from __future__ import annotations

import sys

from celine.utils.pipelines.pipeline_result import PipelineStatus


def test_status_members_are_plain_strings() -> None:
    """The property every consumer actually relies on."""
    assert PipelineStatus.STARTED == "started"
    assert isinstance(PipelineStatus.COMPLETED, str)
    assert PipelineStatus("failed") is PipelineStatus.FAILED


def test_status_renders_as_its_value_not_its_repr() -> None:
    """The one place `StrEnum` and a bare `str, Enum` genuinely differ.

    `class X(str, Enum)` inherits `Enum.__str__`, so `str(X.A)` is
    ``"X.A"`` — which silently corrupts anything that formats a status into a
    log line, a JSON payload or a lineage facet. The 3.10 shim overrides
    `__str__` to match `StrEnum`; this is what says so.
    """
    assert str(PipelineStatus.STARTED) == "started"
    assert f"{PipelineStatus.STARTED}" == "started"
    assert "%s" % PipelineStatus.STARTED == "started"


def test_status_serialises_as_a_string() -> None:
    import json

    assert json.dumps({"status": PipelineStatus.COMPLETED}) == '{"status": "completed"}'


def test_the_shim_is_only_used_below_311() -> None:
    """Guard against the shim quietly shadowing the real thing forever."""
    import enum

    from celine.utils.pipelines import pipeline_result

    if sys.version_info >= (3, 11):
        assert pipeline_result.StrEnum is enum.StrEnum
    else:
        assert pipeline_result.StrEnum is not getattr(enum, "StrEnum", None)
