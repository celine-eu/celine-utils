"""Load ``governance.yaml`` and resolve a rule for a dataset."""

from __future__ import annotations

import fnmatch
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

from celine.governance.merge import merge_configs, merge_rules
from celine.governance.models import (
    KNOWN_KEYS,
    Dependency,
    GovernanceConfig,
    GovernanceRule,
)

logger = logging.getLogger(__name__)


def parse_rule(data: Dict[str, Any]) -> GovernanceRule:
    """Build a :class:`GovernanceRule` from one raw block.

    **Goes through ``model_validate`` on a dict containing only the keys the
    block actually declared.** That is not a style choice: pydantic records those
    keys in ``model_fields_set``, and every overlay in
    :mod:`celine.governance.merge` reads it to tell *unset* from *set to a
    falsy value*. Constructing with keyword arguments — the previous
    implementation — marks every field as set, which silently degrades the merge
    to "override always wins" and makes ``expose: false`` inexpressible.

    Unknown keys are collected into ``extra`` rather than dropped, so a
    consumer can still see what a file said even when the grammar does not
    describe it. :data:`celine.governance.models.KNOWN_KEYS` decides the split.
    """
    block = (data.get("governance") if "governance" in data else data) or {}

    payload: Dict[str, Any] = {k: v for k, v in block.items() if k in KNOWN_KEYS}

    unknown = {k: v for k, v in block.items() if k not in KNOWN_KEYS}
    if unknown:
        payload["extra"] = unknown

    return GovernanceRule.model_validate(payload)


class GovernanceResolver:
    """Resolve governance for an OpenLineage dataset name.

    Matching precedence:

    1. exact key match in ``sources``
    2. glob / fnmatch over the keys, longest pattern wins
    3. ``defaults`` alone
    """

    def __init__(self, config: GovernanceConfig):
        self.config = config

    @classmethod
    def from_file(cls, path: Path) -> "GovernanceResolver":
        logger.debug("Loading governance config from %s", path)
        if not path.exists():
            logger.warning("Governance config file not found at %s", path)
            return cls(GovernanceConfig())

        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "GovernanceResolver":
        defaults = parse_rule(raw.get("defaults") or {})
        sources = {
            pattern: parse_rule(rule_data or {})
            for pattern, rule_data in (raw.get("sources") or {}).items()
        }

        # `if "depends_on" in raw`, not `raw.get(...) or None`: absent and `[]`
        # are different statements — "has not declared its inputs" versus
        # "declares it has none" — and only the first is a file awaiting
        # migration. See `GovernanceConfig.depends_on`.
        depends_on = (
            [Dependency.model_validate(d) for d in (raw.get("depends_on") or [])]
            if "depends_on" in raw
            else None
        )

        payload: Dict[str, Any] = {
            "defaults": defaults,
            "depends_on": depends_on,
            "sources": sources,
        }
        if "active" in raw:
            payload["active"] = raw["active"]

        return cls(GovernanceConfig.model_validate(payload))

    @classmethod
    def from_file_with_override(
        cls,
        base_path: Path,
        overlay_name: Optional[str] = None,
        *,
        infer_from_dir: bool = False,
    ) -> "GovernanceResolver":
        """Load ``governance.yaml`` and overlay ``governance.<name>.yaml`` beside it.

        A deployer overlay states what differs in one environment. It is merged
        with the same rules as a dataset overlays its file's defaults, so it can
        now **withdraw** as well as add — which is the case the old truthiness
        merge could not express.

        The overlay name is resolved in order: the ``overlay_name`` argument, the
        ``GOVERNANCE_OVERLAY_NAME`` environment variable, then — only when
        ``infer_from_dir`` — the name of the directory holding the file.

        ``infer_from_dir`` is opt-in rather than default because the two callers
        this consolidates genuinely disagreed: ``dataset-api`` inferred the app
        name from the parent directory, ``ds`` did not and returned the base
        unchanged when no name was given. Inferring for everyone would make ``ds``
        start honouring overlays it deliberately ignores; defaulting to off would
        make ``dataset-api`` silently stop applying them. Both keep their
        behaviour by passing what they mean.
        """
        base = cls.from_file(base_path)

        name = overlay_name or os.getenv("GOVERNANCE_OVERLAY_NAME")
        if not name and infer_from_dir:
            name = base_path.parent.name
        if not name:
            return base

        overlay_path = base_path.parent / f"governance.{name}.yaml"
        if not overlay_path.is_file():
            return base

        logger.info("Merging deployer override %s", overlay_path)
        overlay = cls.from_file(overlay_path)
        return cls(merge_configs(base.config, overlay.config))

    @classmethod
    def auto_discover(
        cls,
        app_name: Optional[str] = None,
        project_dir: Optional[str] = None,
    ) -> "GovernanceResolver":
        """Locate ``governance.yaml`` by convention.

        1. ``GOVERNANCE_CONFIG_PATH`` env var (absolute path)
        2. ``PIPELINES_ROOT/apps/<app_name>/governance.yaml``
        3. ``<project_dir>/../governance.yaml`` (for dbt/meltano project dirs)
        4. fallback: empty config
        """
        env_path = os.getenv("GOVERNANCE_CONFIG_PATH")
        if env_path:
            p = Path(env_path)
            if p.is_file():
                return cls.from_file(p)
            logger.warning(
                "GOVERNANCE_CONFIG_PATH=%s does not exist or is not a file", env_path
            )

        if app_name:
            root = Path(os.environ.get("PIPELINES_ROOT", "./"))
            candidate = root / "apps" / app_name / "governance.yaml"
            if candidate.is_file():
                return cls.from_file(candidate)

        if project_dir:
            candidate = Path(project_dir).parent / "governance.yaml"
            if candidate.is_file():
                return cls.from_file(candidate)

        logger.info("No governance config found; using empty defaults.")
        return cls(GovernanceConfig())

    def resolve(self, dataset_name: str) -> GovernanceRule:
        """Resolve governance for ``dataset_name`` (e.g. ``db.schema.table``)."""
        sources = self.config.sources

        if dataset_name in sources:
            return merge_rules(self.config.defaults, sources[dataset_name])

        best_match: Optional[Tuple[str, GovernanceRule]] = None
        for pattern, rule in sources.items():
            # `fnmatchcase`, not `fnmatch`: the latter applies os.path.normcase,
            # so a pattern's case-sensitivity would depend on the host OS.
            if fnmatch.fnmatchcase(dataset_name, pattern):
                # Longest matching pattern wins — a more specific glob is a more
                # deliberate statement than a broad one.
                if best_match is None or len(pattern) > len(best_match[0]):
                    best_match = (pattern, rule)

        if best_match:
            return merge_rules(self.config.defaults, best_match[1])

        return self.config.defaults
