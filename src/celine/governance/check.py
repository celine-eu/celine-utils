"""Check governance trees the way they will be served, before anything serves them.

What :func:`celine.governance.validation.validate` does for one document, this does
for a deployment's whole input: every ``governance.yaml`` under the given paths,
**materialized** with its deployer overlay by the same resolver the exporter uses,
plus the owners registry the ownership names resolve against.

It exists because nothing on any import path called the validator: a file that
failed to load was skipped and the run exited 0, an entry with a typo'd
``row_filters`` read as no filter, and a dataset with no row policy was served
whole. The model is ``celine-policies``' ``sync-orgs``: validate before any
side effect, collect every error, and exit non-zero, so a mistake cannot produce
a quietly smaller result.

Findings are **collected, never raised**: a governance tree is edited by hand,
and one problem per run turns a cleanup into round trips. Three severities —
``error`` fails the run, ``warning`` fails it under ``strict``, ``info`` never
does (it lists what a reader should know, such as an ``open`` dataset relying on
being open).

The rules:

1. **schema** — each file against ``governance.schema.json``; an overlay without
   the root ``required`` (it is checked again as part of its base). Unknown keys
   are warnings.
2. **row-policy** — an exposed, non-``secret``, non-``open`` dataset declares both
   layers, ``person`` and ``organization`` (a filter's ``binds``). An undeclared
   layer is refused at query time under the deny default. ``open`` datasets
   relying on being open are listed as ``info``.
3. **handler** — every handler is a built-in or named by the caller (deployment
   plugins are not coordinated); a built-in's ``binds`` matches its layer.
4. **layer-verdict** — ``allow_all`` / ``deny_all`` state ``binds`` (the model
   refuses them otherwise); both in one layer is a contradiction.
5. **consent-coherence** — ported from ``ds``' ``check_consent_coherence`` for
   datasets offered into the dataspace: a person filter needs
   ``consent_required`` or ``pii``, and personal data needs a person filter.
   ``allow_all`` / ``deny_all`` are not person filters.
6. **owners** — each owners file strictly against ``owners.schema.json``; every
   ``ownership`` name resolves; with ``dataspace``, every owner of a dataspace
   offer narrowed by an organization filter has a DID.
"""

from __future__ import annotations

import glob
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import yaml

from celine.governance.exposure import dataspace_expose, effective_expose
from celine.governance.models import (
    BUILTIN_ROW_FILTER_HANDLERS,
    DENY_ALL,
    LAYER_HANDLERS,
    ROW_FILTER_BINDS,
    GovernanceRule,
    row_filter_binds,
)
from celine.governance.owners import OwnerEntry, OwnersRegistry
from celine.governance.ownership import unresolved_owners
from celine.governance.resolver import GovernanceResolver
from celine.governance.validation import OWNERS_SCHEMA, schema_errors, unknown_keys

ERROR = "error"
WARNING = "warning"
INFO = "info"

BASE_NAME = "governance.yaml"


@dataclass(frozen=True)
class CheckFinding:
    severity: str
    rule: str
    source: str
    detail: str
    dataset: Optional[str] = None


@dataclass
class CheckReport:
    findings: List[CheckFinding] = field(default_factory=list)
    #: The base files checked, each with the overlay merged into it, if any.
    files: List[Tuple[str, Optional[str]]] = field(default_factory=list)
    datasets: int = 0

    def failures(self, *, strict: bool = False) -> List[CheckFinding]:
        failing = {ERROR, WARNING} if strict else {ERROR}
        return [f for f in self.findings if f.severity in failing]

    def ok(self, *, strict: bool = False) -> bool:
        return not self.failures(strict=strict)

    def as_dict(self, *, strict: bool = False) -> Dict[str, Any]:
        return {
            "ok": self.ok(strict=strict),
            "strict": strict,
            "datasets": self.datasets,
            "files": [{"base": b, "overlay": o} for b, o in self.files],
            "findings": [asdict(f) for f in self.findings],
        }


class _Collector:
    def __init__(self) -> None:
        self.findings: List[CheckFinding] = []

    def add(
        self,
        severity: str,
        rule: str,
        source: Any,
        detail: str,
        dataset: Optional[str] = None,
    ) -> None:
        self.findings.append(CheckFinding(severity, rule, str(source), detail, dataset))


def _load_yaml(path: Path, out: _Collector) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except (OSError, yaml.YAMLError) as exc:
        out.add(ERROR, "parse", path, f"cannot be read: {exc}")
        return None
    if data is None:
        return {}
    if not isinstance(data, dict):
        out.add(ERROR, "parse", path, f"is a {type(data).__name__}, not a mapping")
        return None
    return data


def expand_paths(paths: Iterable[Any], out: _Collector) -> List[Path]:
    """Every ``governance.yaml`` the arguments name — a file, a directory, or a glob.

    An argument matching nothing is an error: a missing checkout otherwise checks
    fewer files and passes, which is how the catalogue sync loses apps today.
    """
    found: Dict[Path, None] = {}
    for arg in paths:
        p = Path(arg)
        if p.is_file():
            if p.name != BASE_NAME:
                out.add(
                    ERROR,
                    "input",
                    p,
                    f"is not a {BASE_NAME}; an overlay is checked with its base "
                    "(pass the base, and --overlay-dir if the overlay lives elsewhere)",
                )
                continue
            matched = [p]
        elif p.is_dir():
            matched = sorted(p.rglob(BASE_NAME))
        else:
            matched = sorted(
                Path(m) for m in glob.glob(str(arg), recursive=True) if Path(m).is_file()
            )
            matched = [m for m in matched if m.name == BASE_NAME]
        if not matched:
            out.add(ERROR, "input", arg, f"matches no {BASE_NAME}")
        for m in matched:
            found.setdefault(m, None)
    return list(found)


def load_owners_files(
    paths: Sequence[Any], out: _Collector
) -> Tuple[Optional[OwnersRegistry], Dict[str, Dict[str, Any]]]:
    """Validate each owners file strictly, then merge them, later shadowing earlier.

    The ``sync-orgs`` loader exactly: each file is checked before it is merged, so
    a partial per-environment overlay is refused — pass the materialized registry
    instead. Returns the registry and the raw entries by id (the registry's model
    ignores the dataspace keys a DID may live in).
    """
    if not paths:
        return None, {}
    merged: Dict[str, Dict[str, Any]] = {}
    for arg in paths:
        path = Path(arg)
        if not path.is_file():
            out.add(ERROR, "owners", path, "owners file not found")
            continue
        raw = _load_yaml(path, out)
        if raw is None:
            continue
        errors = schema_errors(raw, OWNERS_SCHEMA)
        for e in errors:
            out.add(ERROR, "owners", path, e)
        if errors:
            continue
        for entry in raw.get("owners") or []:
            merged[entry["id"]] = entry
    entries: List[OwnerEntry] = []
    for owner_id, entry in merged.items():
        try:
            entries.append(OwnerEntry.model_validate(entry))
        except ValueError as exc:
            out.add(ERROR, "owners", owner_id, f"cannot be parsed: {exc}")
    return OwnersRegistry(entries), merged


def _access(rule: GovernanceRule) -> str:
    return (rule.access_level or "").lower()


def _filters(rule: GovernanceRule) -> List[Dict[str, Any]]:
    return [f for f in rule.row_filters if isinstance(f, dict)]


def _layer(row_filter: Dict[str, Any]) -> Optional[str]:
    try:
        return row_filter_binds(row_filter)
    except ValueError:
        return None


def _check_handlers(
    rule: GovernanceRule,
    known: Set[str],
    source: Path,
    dataset: str,
    out: _Collector,
) -> None:
    by_layer: Dict[str, Set[str]] = {}
    for index, row_filter in enumerate(_filters(rule)):
        handler = row_filter.get("handler")
        if not isinstance(handler, str) or not handler:
            out.add(ERROR, "handler", source, f"row_filters[{index}] names no handler", dataset)
            continue
        layer = _layer(row_filter)
        if layer is None:
            # The model refuses this at parse time; reaching it means a caller
            # built the rule around the model.
            out.add(
                ERROR,
                "handler",
                source,
                f"row_filters[{index}] ({handler}) binds {row_filter.get('binds')!r}; "
                f"expected one of {', '.join(ROW_FILTER_BINDS)}",
                dataset,
            )
            continue
        by_layer.setdefault(layer, set()).add(handler)
        if handler not in known:
            out.add(
                ERROR,
                "handler",
                source,
                f"row_filters[{index}] names unknown handler {handler!r} — not a built-in; "
                "a deployment plugin must be named with --handlers",
                dataset,
            )
            continue
        builtin = BUILTIN_ROW_FILTER_HANDLERS.get(handler)
        if builtin is not None and builtin != layer:
            out.add(
                ERROR,
                "handler",
                source,
                f"row_filters[{index}] ({handler}) binds {layer}, but the handler binds "
                f"{builtin} — the data plane refuses the mismatch at import",
                dataset,
            )
    for layer, handlers in sorted(by_layer.items()):
        if LAYER_HANDLERS <= handlers:
            out.add(
                ERROR,
                "layer-verdict",
                source,
                f"the {layer} layer declares both allow_all and deny_all",
                dataset,
            )


def _check_row_policy(
    rule: GovernanceRule, source: Path, dataset: str, out: _Collector
) -> None:
    access = _access(rule)
    if not effective_expose(rule) or access == "secret":
        return
    declared = {layer for f in _filters(rule) if (layer := _layer(f)) is not None}
    missing = [layer for layer in ROW_FILTER_BINDS if layer not in declared]
    if not missing:
        return
    layers = " and ".join(missing)
    if access == "open":
        out.add(
            INFO,
            "row-policy",
            source,
            f"open, no {layers} layer declared — served to any authenticated caller",
            dataset,
        )
        return
    out.add(
        ERROR,
        "row-policy",
        source,
        f"{access or 'no access_level'}, no {layers} layer declared — refused under "
        "the deny default; declare the layer (allow_all states it admits every row)",
        dataset,
    )


def _check_consent(
    rule: GovernanceRule, source: Path, dataset: str, out: _Collector
) -> None:
    """``ds``' ``check_consent_coherence`` (D-3b), on what the dataspace offers."""
    if not dataspace_expose(rule) or _access(rule) == "secret":
        return
    filters = _filters(rule)
    if any(f.get("handler") == DENY_ALL and _layer(f) == "person" for f in filters):
        # The person layer admits nobody: no personal row leaves, whatever the
        # consent declarations say.
        return
    person = [
        f for f in filters if _layer(f) == "person" and f.get("handler") not in LAYER_HANDLERS
    ]
    organization = [
        f
        for f in filters
        if _layer(f) == "organization" and f.get("handler") not in LAYER_HANDLERS
    ]
    has_filter = bool(rule.user_filter_column or person)
    consent = bool(rule.dataspace and rule.dataspace.consent_required)
    declares_personal = consent or rule.classification == "pii"
    if has_filter and not declares_personal:
        out.add(
            ERROR,
            "consent-coherence",
            source,
            "a row filter binding a person is declared but neither "
            "dataspace.consent_required nor classification: pii is — the file asserts "
            "per-subject access control while declaring the data impersonal",
            dataset,
        )
    if declares_personal and not has_filter and organization:
        out.add(
            ERROR,
            "consent-coherence",
            source,
            "consent_required or classification: pii is declared but every row filter "
            "binds an organization — consent cannot be enforced per subject; mark the "
            "per-subject filter binds: person or add one",
            dataset,
        )
        return
    if consent and not has_filter:
        out.add(
            WARNING,
            "consent-coherence",
            source,
            "consent_required is set but no person filter is declared — consent "
            "cannot be enforced per subject",
            dataset,
        )
    if rule.classification == "pii" and not has_filter:
        out.add(
            WARNING,
            "consent-coherence",
            source,
            "classified pii but no person filter is declared",
            dataset,
        )
    for row_filter in person:
        args = row_filter.get("args")
        column = args.get("column") if isinstance(args, dict) else None
        if not (isinstance(column, str) and column.strip()):
            out.add(
                ERROR,
                "consent-coherence",
                source,
                f"row_filter {row_filter.get('handler')!r} has an empty column",
                dataset,
            )


def _owner_did(entry: Dict[str, Any]) -> Optional[str]:
    block = entry.get("dataspace")
    nested = block.get("did") if isinstance(block, dict) else None
    return entry.get("did") or nested


def _check_dids(
    rule: GovernanceRule,
    raw_owners: Dict[str, Dict[str, Any]],
    registry: OwnersRegistry,
    source: Path,
    dataset: str,
    out: _Collector,
) -> None:
    if not dataspace_expose(rule) or _access(rule) == "secret":
        return
    narrowed = any(
        _layer(f) == "organization" and f.get("handler") not in LAYER_HANDLERS
        for f in _filters(rule)
    )
    if not narrowed:
        return
    for owner in rule.ownership:
        entry = registry.by_id(owner.name)
        if entry is None:
            continue  # reported by the ownership check
        if not _owner_did(raw_owners.get(entry.id, {})):
            out.add(
                ERROR,
                "owners",
                source,
                f"owner {entry.id!r} has no did — an organization-bound dataspace offer "
                "needs its owner's participant DID",
                dataset,
            )


def check_governance(
    paths: Sequence[Any],
    *,
    overlay_dirs: Sequence[Any] = (),
    owners_files: Sequence[Any] = (),
    handlers: Iterable[str] = (),
    dataspace: bool = False,
) -> CheckReport:
    """Run rules 1–6 over every ``governance.yaml`` under ``paths``. Never raises."""
    out = _Collector()
    report = CheckReport()
    known = set(BUILTIN_ROW_FILTER_HANDLERS) | {h for h in handlers if h}
    dirs = [Path(d) for d in overlay_dirs]
    for d in dirs:
        if not d.is_dir():
            out.add(ERROR, "input", d, "overlay directory not found")

    registry, raw_owners = load_owners_files(owners_files, out)
    if dataspace and registry is None:
        out.add(ERROR, "input", "--dataspace", "needs the owners registry (--owners)")

    used_overlays: Set[Path] = set()
    for base in expand_paths(paths, out):
        try:
            overlay = GovernanceResolver.overlay_path_for(
                base, infer_from_dir=True, overlay_dirs=[d for d in dirs if d.is_dir()]
            )
        except ValueError as exc:
            out.add(ERROR, "input", base, str(exc))
            continue
        report.files.append((str(base), str(overlay) if overlay else None))

        documents = [(base, False)] + ([(overlay, True)] if overlay else [])
        readable = True
        for path, is_overlay in documents:
            raw = _load_yaml(path, out)
            if raw is None:
                readable = False
                continue
            for e in schema_errors(raw, overlay=is_overlay):
                out.add(ERROR, "schema", path, e)
            for u in unknown_keys(raw):
                out.add(WARNING, "schema", path, f"unknown key {u}")
            if is_overlay:
                used_overlays.add(path.resolve())
        if not readable:
            continue

        try:
            config = GovernanceResolver.from_file_with_override(
                base, infer_from_dir=True, overlay_dirs=[d for d in dirs if d.is_dir()]
            ).config
        except Exception as exc:  # the parse is the check: report, never raise
            out.add(ERROR, "schema", base, f"does not load as governance: {exc}")
            continue

        if registry is not None:
            for name, where in sorted(unresolved_owners(config, registry).items()):
                out.add(
                    ERROR,
                    "owners",
                    base,
                    f"ownership name {name!r} resolves to no owner ({', '.join(where)})",
                )

        resolver = GovernanceResolver(config)
        for dataset in config.sources:
            report.datasets += 1
            rule = resolver.resolve(dataset)
            _check_handlers(rule, known, base, dataset, out)
            _check_row_policy(rule, base, dataset, out)
            _check_consent(rule, base, dataset, out)
            if dataspace and registry is not None:
                _check_dids(rule, raw_owners, registry, base, dataset, out)

    for d in dirs:
        if not d.is_dir():
            continue
        for overlay in sorted(d.glob("governance.*.yaml")):
            if overlay.resolve() not in used_overlays:
                out.add(
                    WARNING,
                    "input",
                    overlay,
                    "overlay applies to no base in this run — its app was not checked",
                )

    report.findings = out.findings
    return report
