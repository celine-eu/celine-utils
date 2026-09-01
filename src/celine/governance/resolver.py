"""Load ``governance.yaml`` and resolve a rule for a dataset."""

from __future__ import annotations

import fnmatch
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, TypeVar, overload

import yaml

from celine.governance.merge import merge_configs, merge_rules
from celine.governance.models import (
    Dependency,
    GovernanceConfig,
    GovernanceRule,
)

logger = logging.getLogger(__name__)

R = TypeVar("R", bound=GovernanceRule)


def _rule_keys(model_cls: type[GovernanceRule]) -> frozenset[str]:
    """The keys ``model_cls`` claims from a block; everything else is ``extra``.

    Read off the model rather than off :data:`celine.governance.models.KNOWN_KEYS`
    **because a subclass has more of them**. ``ds`` extends
    :class:`~celine.governance.models.GovernanceRule` with its ODRL ``policy``
    block; splitting its files against the base grammar's key set would drop
    ``policy`` into ``extra``, where the field reads as its default forever and
    nothing warns — the failure this module's docstrings keep pointing at, one
    level up.

    For :class:`~celine.governance.models.GovernanceRule` itself this is exactly
    ``KNOWN_KEYS``, and ``test_known_keys_matches_the_model`` holds the two
    together. That constant stays the published statement of the base grammar —
    ``ds`` imports it — and is the thing to edit when a field is added here.
    """
    return frozenset(model_cls.model_fields) - {"extra"}


@overload
def parse_rule(data: Dict[str, Any]) -> GovernanceRule: ...


@overload
def parse_rule(data: Dict[str, Any], model_cls: type[R]) -> R: ...


def parse_rule(
    data: Dict[str, Any], model_cls: type[GovernanceRule] = GovernanceRule
) -> GovernanceRule:
    """Build a :class:`GovernanceRule` — or a subclass of one — from a raw block.

    **Goes through ``model_validate`` on a dict containing only the keys the
    block actually declared.** That is not a style choice: pydantic records those
    keys in ``model_fields_set``, and every overlay in
    :mod:`celine.governance.merge` reads it to tell *unset* from *set to a
    falsy value*. Constructing with keyword arguments — the previous
    implementation — marks every field as set, which silently degrades the merge
    to "override always wins" and makes ``expose: false`` inexpressible.

    Unknown keys are collected into ``extra`` rather than dropped, so a
    consumer can still see what a file said even when the grammar does not
    describe it. :func:`_rule_keys` decides the split, off ``model_cls`` — so a
    consumer's subclass keeps its own fields instead of watching them land in
    ``extra``.

    ``model_cls`` must be :class:`GovernanceRule` or a subclass of it; anything
    else is a different grammar and this function has nothing to say about it.
    """
    block = (data.get("governance") if "governance" in data else data) or {}

    known = _rule_keys(model_cls)
    payload: Dict[str, Any] = {k: v for k, v in block.items() if k in known}

    unknown = {k: v for k, v in block.items() if k not in known}
    if unknown:
        payload["extra"] = unknown

    return model_cls.model_validate(payload)


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
        """Load one named governance file.

        **Raises :class:`FileNotFoundError` when the path is not there.** It used
        to log a warning and return an empty config, and that is a different
        question answered: a caller handing over a path has already said which
        file it means, so absence is *what you asked for is not there* — not
        *nothing was asked for*, which is what :meth:`auto_discover` handles.

        The warning was not enough, and there is a measurement rather than an
        opinion behind that. ``ds`` renamed its ``governance/`` directory without
        updating the connector's default path: the provider started clean, served
        an empty dataset list and empty sharing offers, and the files sat on disk
        the whole time. The first symptom was an unrelated end-to-end test failing
        on a fixture that was right there in the file.

        The built-in exception, not a governance-specific one: ``path.open()``
        raises it anyway, and a consumer catching it needs no import from this
        package.
        """
        logger.debug("Loading governance config from %s", path)
        if not path.is_file():
            raise FileNotFoundError(f"Governance config file not found: {path}")

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

        A **missing base** raises, through :meth:`from_file`. A **missing overlay**
        does not: the overlay is looked up by convention rather than named by the
        caller, so its absence is the "nothing was asked for" case.

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

        **Step 4 stays**, unlike :meth:`from_file`'s. Discovery by convention that
        finds nothing has found nothing, and an empty config is the truthful
        answer to that. Every ``from_file`` call below is already guarded by
        ``is_file()``, so none of them reaches the new exception.
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
