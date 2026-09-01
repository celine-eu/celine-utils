"""Overlay one governance block onto another. One implementation, for everyone.

Ported from ``ds`` ``libs/governance/resolver.py::_merge_models``, which was the
only correct implementation of the four that existed. It is reproduced here
rather than reinvented, because three parsers reading one file format and
reaching different conclusions is the defect this package exists to remove.
"""

from __future__ import annotations

from typing import Optional, TypeVar, cast

from pydantic import BaseModel

from celine.governance.models import (
    DataspaceConfig,
    DcatConfig,
    GovernanceConfig,
    GovernanceRule,
)

M = TypeVar("M", bound=BaseModel)
R = TypeVar("R", bound=GovernanceRule)
D = TypeVar("D", bound=DataspaceConfig)
C = TypeVar("C", bound=GovernanceConfig)


def _operand_cls(
    base: Optional[BaseModel],
    override: Optional[BaseModel],
    default: type[BaseModel],
    explicit: Optional[type[BaseModel]] = None,
) -> type[BaseModel]:
    """Which class the merged instance is validated into.

    The models here are ``extra="ignore"`` **so that a consumer can subclass
    them** — ``ds`` carries its ODRL ``policy`` block and its richer
    ``DataspaceSpec`` that way. That same setting is what made every merge in
    this module lossy for such a consumer: validating a subclass instance into
    the hardcoded base class returned a smaller object with the subclass's
    fields dropped, silently. A model narrower than its input throwing the rest
    away is the defect this package exists to remove; it was doing it itself.

    So the class follows the operands. ``explicit`` wins when given, for the
    caller who means something the operands do not say.

    **Operands of different classes raise.** Picking the more derived one would
    be a silent choice about which fields survive, which is the same failure in
    a new place; picking the base would reintroduce the old one. A caller who
    genuinely means to mix two shapes passes ``explicit`` and says so.
    """
    if explicit is not None:
        return explicit

    classes = {type(o) for o in (base, override) if o is not None}
    if len(classes) > 1:
        names = ", ".join(sorted(c.__name__ for c in classes))
        raise TypeError(
            f"cannot merge operands of different model classes ({names}); "
            f"validate both into one class first, or pass model_cls to say "
            f"which one the result is"
        )
    return classes.pop() if classes else default


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
    base: Optional[D],
    override: Optional[D],
    *,
    model_cls: Optional[type[D]] = None,
) -> Optional[D]:
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

    The result is the operands' own class — ``ds``'s ``DataspaceSpec`` carries
    the EDC sub-objects through instead of losing them. See :func:`_operand_cls`,
    including what happens when the two operands disagree about their class.
    """
    cls = cast("type[D]", _operand_cls(base, override, DataspaceConfig, model_cls))
    merged = merge_models(base, override, cls)
    if merged is None or base is None or override is None:
        return merged
    merged.purpose = sorted(set(base.purpose) | set(override.purpose))
    merged.consent_required = base.consent_required or override.consent_required
    merged.contract_required = base.contract_required or override.contract_required
    return merged


def merge_rules(
    base: R, override: R, *, model_cls: Optional[type[R]] = None
) -> R:
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

    The result is the operands' own class, and the nested ``dcat`` and
    ``dataspace`` merges follow their own — see :func:`_operand_cls`. ``ds``
    subclasses this model to carry its ODRL ``policy`` block; before that the
    merge validated it back into :class:`GovernanceRule` and dropped the block
    without saying so.
    """
    cls = cast("type[R]", _operand_cls(base, override, GovernanceRule, model_cls))
    merged = merge_models(base, override, cls)
    assert merged is not None  # both operands are non-None by signature

    merged.tags = sorted(set(base.tags or []) | set(override.tags or []))
    merged.ownership = override.ownership or base.ownership
    merged.row_filters = override.row_filters or base.row_filters
    merged.extra = {**base.extra, **override.extra}
    merged.ontology = override.ontology if override.ontology is not None else base.ontology
    # The nested classes follow their own operands rather than the rule's: a
    # subclass may replace one sub-object and inherit the others, and reading
    # them off `cls` would put that back the way it was.
    merged.dcat = merge_models(
        base.dcat,
        override.dcat,
        cast("type[DcatConfig]", _operand_cls(base.dcat, override.dcat, DcatConfig)),
    )
    merged.dataspace = merge_dataspace(base.dataspace, override.dataspace)
    return merged


def merge_configs(
    base: C, override: C, *, model_cls: Optional[type[C]] = None
) -> C:
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
    cls = cast("type[C]", _operand_cls(base, override, GovernanceConfig, model_cls))

    sources = dict(base.sources)
    for key, rule in override.sources.items():
        sources[key] = merge_rules(sources[key], rule) if key in sources else rule

    # The generic overlay first, so a subclass's own root-level fields survive
    # with `exclude_unset` semantics rather than being dropped by a constructor
    # that names three keys. The three below are then assigned over it, because
    # their rules are not "override wins" — assignment marks them set, which is
    # what the previous keyword construction did for every field.
    merged = merge_models(base, override, cls)
    assert merged is not None  # both operands are non-None by signature

    merged.defaults = merge_rules(base.defaults, override.defaults)
    merged.depends_on = (
        override.depends_on if override.depends_on is not None else base.depends_on
    )
    merged.sources = sources
    return merged
