"""Overlay one governance block onto another. One implementation, for everyone.

Ported from ``ds`` ``libs/governance/resolver.py::_merge_models``, which was the
only correct implementation of the four that existed. It is reproduced here
rather than reinvented, because three parsers reading one file format and
reaching different conclusions is the defect this package exists to remove.
"""

from __future__ import annotations

from typing import Optional, TypeVar

from pydantic import BaseModel

from celine.governance.models import (
    DataspaceConfig,
    DcatConfig,
    GovernanceConfig,
    GovernanceRule,
)

M = TypeVar("M", bound=BaseModel)


def _deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for key, value in b.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def merge_models(base: Optional[M], override: Optional[M], model_cls: type[M]) -> Optional[M]:
    """Override's *explicitly set* fields on top of base, recursively.

    **``exclude_unset``, not ``exclude_defaults``, and never truthiness.**

    All three drop a field the source never mentioned — which is the point, so an
    unmentioned field cannot overwrite an inherited value with a default. They
    differ on a field the source *did* mention whose value equals the default:

    - ``exclude_defaults`` drops it, so it cannot tell *silent* from *said no*.
    - Truthiness (``base.x or override.x``) is worse still — it cannot express
      *off* at all once the base says *on*.

    That made one instruction unexpressible, and ``ds`` hit it in production:
    :attr:`DataspaceConfig.expose` defaults to ``False``, so an overlay saying
    ``expose: false`` — the obvious way to withdraw a dataset — dumped to nothing
    and the base's ``expose: true`` survived. The dataset stayed in the
    catalogue and the overlay that withdrew it validated clean. The documented
    workaround was ``access_level: secret``, which is a different statement about
    a different thing.

    It generalises past ``expose``: every boolean defaulting to ``False`` and
    every optional defaulting to ``None`` had the same hole. ``conforms_to: null``
    means *this dataset has no payload model* — a different claim from declaring
    nothing — and under truthiness it silently inherits one instead.

    Pydantic tracks this per instance in ``model_fields_set``, populated by
    ``model_validate``. **That is why parsing must go through ``model_validate``
    and not keyword arguments** — constructing with kwargs marks every field as
    set, and this whole mechanism degrades to "override always wins". Validating
    the merged dict carries the set forward, so a chain of overlays keeps working.
    """
    if base is None:
        return override
    if override is None:
        return base
    return model_cls.model_validate(
        _deep_merge(
            base.model_dump(exclude_unset=True),
            override.model_dump(exclude_unset=True),
        )
    )


def merge_dataspace(
    base: Optional[DataspaceConfig], override: Optional[DataspaceConfig]
) -> Optional[DataspaceConfig]:
    """Field-wise overlay, then two rules that are not "override wins".

    - ``purpose`` is a **union**. An overlay adds a reason for processing; it
      does not silently retract the ones the base declared.
    - ``consent_required`` and ``contract_required`` are **OR**. Once something
      is required it cannot be un-required by a file layered on top: an overlay
      may tighten, never loosen.

    ``expose`` is deliberately **not** in that list. OR-ing it would mean *once
    offered, always offered* — a loosening, and the precise bug this merge
    replaces. It follows the ordinary ``exclude_unset`` rule so that an overlay
    can withdraw a dataset.
    """
    merged = merge_models(base, override, DataspaceConfig)
    if merged is None or base is None or override is None:
        return merged
    merged.purpose = sorted(set(base.purpose) | set(override.purpose))
    merged.consent_required = base.consent_required or override.consent_required
    merged.contract_required = base.contract_required or override.contract_required
    return merged


def merge_rules(base: GovernanceRule, override: GovernanceRule) -> GovernanceRule:
    """Overlay ``override`` onto ``base``.

    Generic ``exclude_unset`` merge, then the fields whose semantics are not
    "override wins":

    ============== ==========================================================
    ``tags``       union — an overlay adds keywords, it does not retract them
    ``ownership``  whole replacement when non-empty; a partial owner list is
                   not a meaningful statement
    ``row_filters``whole replacement when non-empty. **Not** merged field-wise:
                   filters are a set of independent gates and interleaving two
                   lists by position would silently build a filter that neither
                   file declared
    ``extra``      dict merge, override wins per key
    ``ontology``   whole replacement. Its two fields are *alternatives*
                   (``spec`` XOR ``spec_file``), so a field-wise overlay could
                   produce a rule declaring both — which the schema forbids and
                   the mapping resolver rejects as "two answers to what one
                   column means"
    ============== ==========================================================
    """
    merged = merge_models(base, override, GovernanceRule)
    assert merged is not None  # both operands are non-None by signature

    merged.tags = sorted(set(base.tags or []) | set(override.tags or []))
    merged.ownership = override.ownership or base.ownership
    merged.row_filters = override.row_filters or base.row_filters
    merged.extra = {**base.extra, **override.extra}
    merged.ontology = override.ontology if override.ontology is not None else base.ontology
    merged.dcat = merge_models(base.dcat, override.dcat, DcatConfig)
    merged.dataspace = merge_dataspace(base.dataspace, override.dataspace)
    return merged


def merge_configs(base: GovernanceConfig, override: GovernanceConfig) -> GovernanceConfig:
    """Overlay a whole governance file onto another — a deployer override.

    Defaults merge with defaults; a source present in both merges rule-wise; a
    source only the overlay declares is added as-is.

    ``depends_on`` is **whole replacement when the overlay states one**, the same
    rule as ``ownership``, ``row_filters`` and ``ontology``, and for the same
    reason: a partial input list is not a meaningful statement. Union would be
    worse than merely wrong here — substituting a producer is the thing a
    deployment overlay exists to do, and under a union an upstream the deployment
    satisfies another way could never be withdrawn.

    Unstated inherits, which is why this reads ``is not None`` and not
    truthiness: ``depends_on: []`` in an overlay declares *no inputs* and must
    survive, exactly as ``expose: false`` must. Truthiness would silently restore
    the base's list — the bug this merge layer was written to remove.
    """
    sources = dict(base.sources)
    for key, rule in override.sources.items():
        sources[key] = merge_rules(sources[key], rule) if key in sources else rule
    return GovernanceConfig(
        defaults=merge_rules(base.defaults, override.defaults),
        depends_on=(
            override.depends_on if override.depends_on is not None else base.depends_on
        ),
        sources=sources,
    )
