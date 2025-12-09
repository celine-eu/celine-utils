# celine/pipelines/governance.py
from __future__ import annotations

import fnmatch
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pydantic import BaseModel, Field

from celine.common.logger import get_logger

logger = get_logger(__name__)


class GovernanceOwner(BaseModel):
    name: str
    type: str = Field(default="OWNER")  # semantic, not enforced by spec


class GovernanceRule(BaseModel):
    license: Optional[str] = None
    ownership: List[GovernanceOwner] = Field(default_factory=list)
    access_level: Optional[str] = None  # open / internal / restricted / secret
    access_rights: Optional[str] = None  # free-form (e.g. "public", "internal")
    classification: Optional[str] = None  # your color / risk class
    tags: List[str] = Field(default_factory=list)
    retention_days: Optional[int] = None
    documentation_url: Optional[str] = None
    source_system: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)


class GovernanceConfig(BaseModel):
    defaults: GovernanceRule = Field(default_factory=GovernanceRule)
    sources: Dict[str, GovernanceRule] = Field(default_factory=dict)


class GovernanceResolver:
    """
    Load governance.yaml and resolve a GovernanceRule for a given OpenLineage dataset
    name (e.g. 'datasets.gold.grid_wind_risks').

    Matching precedence:
      1. exact key match in sources
      2. glob / fnmatch on keys
      3. defaults
    """

    def __init__(self, config: GovernanceConfig):
        self.config = config

    @classmethod
    def from_file(cls, path: Path) -> "GovernanceResolver":
        logger.debug(f"Loading governance config from {path}")
        if not path.exists():
            logger.warning(f"Governance config file not found at {path}")
            return cls(GovernanceConfig())

        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        def _parse_rule(data: Dict[str, Any]) -> GovernanceRule:
            # Allow either embedded under "governance" or at root
            block = (data.get("governance") if "governance" in data else data) or {}
            owners_raw = block.get("ownership") or []
            owners = [
                (
                    GovernanceOwner(**o)
                    if isinstance(o, dict)
                    else GovernanceOwner(name=str(o))
                )
                for o in owners_raw
            ]
            return GovernanceRule(
                license=block.get("license"),
                ownership=owners,
                access_level=block.get("access_level"),
                access_rights=block.get("access_rights"),
                classification=block.get("classification"),
                tags=block.get("tags") or [],
                retention_days=block.get("retention_days"),
                documentation_url=block.get("documentation_url"),
                source_system=block.get("source_system"),
                extra={
                    k: v
                    for k, v in block.items()
                    if k
                    not in {
                        "license",
                        "ownership",
                        "access_level",
                        "access_rights",
                        "classification",
                        "tags",
                        "retention_days",
                        "documentation_url",
                        "source_system",
                    }
                },
            )

        defaults = _parse_rule(raw.get("defaults") or {})

        sources_cfg: Dict[str, GovernanceRule] = {}
        for pattern, rule_data in (raw.get("sources") or {}).items():
            sources_cfg[pattern] = _parse_rule(rule_data or {})

        cfg = GovernanceConfig(defaults=defaults, sources=sources_cfg)
        return cls(cfg)

    @classmethod
    def auto_discover(
        cls,
        app_name: Optional[str] = None,
        project_dir: Optional[str] = None,
    ) -> "GovernanceResolver":
        """
        Try to locate governance.yaml using a few conventions:

        1. GOV_CONFIG_PATH env var (absolute path)
        2. PIPELINES_ROOT/apps/<app_name>/governance.yaml
        3. <project_dir>/../governance.yaml (for dbt/meltano project dirs)
        4. fallback: empty config
        """
        # 1) Explicit override
        env_path = os.getenv("GOVERNANCE_CONFIG_PATH")
        if env_path:
            p = Path(env_path)
            if p.is_file():
                return cls.from_file(p)
            logger.warning(
                f"GOVERNANCE_CONFIG_PATH={env_path} does not exist or is not a file"
            )

        # 2) PIPELINES_ROOT/apps/<app_name>/governance.yaml
        if app_name:
            root = Path(os.environ.get("PIPELINES_ROOT", "./"))
            candidate = root / "apps" / app_name / "governance.yaml"
            if candidate.is_file():
                return cls.from_file(candidate)

        # 3) project_dir/../governance.yaml
        if project_dir:
            pd = Path(project_dir)
            candidate = pd.parent / "governance.yaml"
            if candidate.is_file():
                return cls.from_file(candidate)

        # 4) fallback: empty config
        logger.info("No governance config found; using empty defaults.")
        return cls(GovernanceConfig())

    def resolve(self, dataset_name: str) -> GovernanceRule:
        """
        Resolve governance for a given dataset name.
        dataset_name is the OpenLineage Dataset.name (e.g. 'db.schema.table').
        """
        sources = self.config.sources

        # Exact match first
        if dataset_name in sources:
            return self._merge(self.config.defaults, sources[dataset_name])

        # Then glob / fnmatch
        best_match: Optional[Tuple[str, GovernanceRule]] = None
        for pattern, rule in sources.items():
            if fnmatch.fnmatch(dataset_name, pattern):
                # simple heuristic: prefer the longest matching pattern
                if best_match is None or len(pattern) > len(best_match[0]):
                    best_match = (pattern, rule)

        if best_match:
            _, rule = best_match
            return self._merge(self.config.defaults, rule)

        # Fallback: defaults only
        return self.config.defaults

    @staticmethod
    def _merge(base: GovernanceRule, override: GovernanceRule) -> GovernanceRule:
        """
        Overlay override on top of base. Lists are merged (union), scalars overridden
        if not None on override.
        """

        def pick(a, b):
            return b if b is not None else a

        owners = override.ownership or base.ownership
        tags = sorted(set(base.tags or []) | set(override.tags or []))

        return GovernanceRule(
            license=pick(base.license, override.license),
            ownership=owners,
            access_level=pick(base.access_level, override.access_level),
            access_rights=pick(base.access_rights, override.access_rights),
            classification=pick(base.classification, override.classification),
            tags=tags,
            retention_days=pick(base.retention_days, override.retention_days),
            documentation_url=pick(base.documentation_url, override.documentation_url),
            source_system=pick(base.source_system, override.source_system),
            extra={**base.extra, **override.extra},
        )
