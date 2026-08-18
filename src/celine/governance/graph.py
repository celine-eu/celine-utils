"""The inter-pipeline dependency graph, built from ``governance.yaml`` files.

``sources:`` says what a pipeline **produces**; ``depends_on:`` says what it
**consumes**. Put a set of those files side by side and the edges between
pipelines fall out — which is the one thing no tool in the platform could see
before, because each app is its own dbt project and ``ref()`` never crosses a
project boundary. Across pipelines there was no graph at all: ordering lived in
cron offsets, in a hand-maintained tier table, and in prose.

**Dependencies name datasets, never pipelines.** Several producers can satisfy
one dataset through a shared alias — ``mt`` and ``owm`` both publish
``weather__forecast_hourly``, and ``weather`` is specifically built not to know
which one ran — and a deployment may substitute its own producer entirely.
Resolution is therefore a search over declared outputs, not a name lookup. It is
also what keeps an open-source file free of any private name: an entry that
resolves to nothing here is satisfied somewhere that was not scanned, and saying
so requires naming no repository.

**This module is in the thin core deliberately** (REQ-0001). Nothing here imports
beyond the standard library and this package, so a scheduler, ``dataset-api`` or
``ds`` can read the graph without inheriting dbt, Meltano, Prefect or Keycloak.

Usage::

    from celine.governance.graph import build_graph, discover, load_pipelines

    found = discover(["apps/*", "../other/pipelines/apps/*"])
    graph = build_graph(load_pipelines(found.files))

    for tier in graph.tiers():
        print(", ".join(tier))
    for finding in graph.findings:
        print(finding.kind, finding.detail)
"""

from __future__ import annotations

import fnmatch
import glob as _glob
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from celine.governance.models import Dependency
from celine.governance.resolver import GovernanceResolver

#: The only filename that becomes a node. ``governance.<name>.yaml`` beside it is
#: a deployer overlay, not a pipeline, and treating one as a node would invent a
#: pipeline that does not exist.
GOVERNANCE_FILENAME = "governance.yaml"

#: Finding kinds, in the order a reader should care about them.
UNRESOLVED = "unresolved"
SCHEDULE_INVERSION = "schedule-inversion"
SCHEDULE_COLLISION = "schedule-collision"
#: The same two problems, found on a pair where at least one app deploys several
#: flows. Governance is per app and cannot say which flow produces which dataset,
#: so the pairing may not be the one that moves the data. Advisory, not counted
#: by ``--strict`` — a check that cries wolf gets ignored, and then so does the
#: one that was right.
SCHEDULE_UNVERIFIED = "schedule-unverified"
MULTIPLE_PRODUCERS = "multiple-producers"
CYCLE = "cycle"
SELF_DEPENDENCY = "self-dependency"
INACTIVE_PRODUCER = "inactive-producer"
NOT_DEPLOYED = "not-deployed"
EXTERNAL_SATISFIED = "external-satisfied"


# =============================================================================
# Data
# =============================================================================


@dataclass(frozen=True)
class Flow:
    """One deployed, scheduled unit of execution.

    Deliberately **not** the same thing as a :class:`Pipeline`. A governance file
    describes an app; a deployment schedules *flows*, and one app can host several
    on independent crons — `om` deploys four. Ordering bugs live at this
    granularity, so the schedule side of the graph has to model it even though
    governance cannot.
    """

    name: str
    app: str
    cron: str


@dataclass(frozen=True)
class Pipeline:
    """One ``governance.yaml``, as a node."""

    name: str
    path: Path
    produces: Tuple[str, ...]
    active: bool = True

    #: ``None`` means the file has not declared its inputs; ``()`` means it
    #: declares it has none. Keeping them apart is what lets a report separate a
    #: genuine root from a file awaiting migration.
    depends_on: Optional[Tuple[Dependency, ...]] = None

    @property
    def declared(self) -> bool:
        return self.depends_on is not None


@dataclass(frozen=True)
class Edge:
    producer: str
    consumer: str
    #: The producer's ``sources`` key that satisfied the pattern.
    dataset: str
    #: The ``depends_on`` pattern that matched it, which may be a glob.
    pattern: str
    optional: bool = False


@dataclass(frozen=True)
class Finding:
    kind: str
    detail: str
    pipeline: Optional[str] = None
    dataset: Optional[str] = None


@dataclass(frozen=True)
class Discovery:
    files: Tuple[Path, ...]
    #: ``governance.<name>.yaml`` files a pattern matched. Returned rather than
    #: dropped: a caller who globbed a directory of overlays and got an empty
    #: graph is owed the reason.
    skipped_overlays: Tuple[Path, ...] = ()


@dataclass(frozen=True)
class DependencyGraph:
    pipelines: Tuple[Pipeline, ...]
    edges: Tuple[Edge, ...]
    findings: Tuple[Finding, ...]

    def producers_of(self, name: str) -> List[str]:
        return sorted({e.producer for e in self.edges if e.consumer == name})

    def consumers_of(self, name: str) -> List[str]:
        return sorted({e.consumer for e in self.edges if e.producer == name})

    def run_order(self) -> List[str]:
        """The tiers flattened — a run order that is always safe.

        Sequential, so it holds on one machine with no coordination, which is what
        a person populating a database from scratch actually does. Where the
        parallelism matters, read :meth:`tiers` instead: everything within one is
        independent.

        A pipeline in a cycle appears in neither, because there is no order that
        satisfies it and inventing one would be read as an instruction.
        """
        return [name for tier in self.tiers() for name in tier]

    def tiers(self) -> List[List[str]]:
        """Topological levels: everything in tier N may run in parallel.

        This is the generated replacement for the tier tables maintained by hand
        in ``celine-pipelines/docs/local-runtime.md`` and demo3's playbook.

        Pipelines caught in a cycle are omitted here and reported as a ``cycle``
        finding instead. Emitting them in an arbitrary tier would be worse than
        omitting them, because a tier table is read as an instruction.
        """
        names = [p.name for p in self.pipelines]
        upstream: Dict[str, Set[str]] = {n: set() for n in names}
        for edge in self.edges:
            if edge.producer in upstream and edge.consumer in upstream:
                upstream[edge.consumer].add(edge.producer)

        placed: Set[str] = set()
        tiers: List[List[str]] = []
        remaining = set(names)

        while remaining:
            ready = sorted(n for n in remaining if upstream[n] <= placed)
            if not ready:
                break  # everything left is in, or behind, a cycle
            tiers.append(ready)
            placed |= set(ready)
            remaining -= set(ready)

        return tiers


# =============================================================================
# Discovery
# =============================================================================


def discover(patterns: Sequence[str]) -> Discovery:
    """Expand shell globs to ``governance.yaml`` files.

    A pattern may name a file or a directory; a directory contributes the
    ``governance.yaml`` directly inside it. That is what makes ``apps/*`` — the
    way a person thinks about it — work as well as ``apps/*/governance.yaml``.

    The scan set is the caller's to choose, and that is load-bearing rather than
    lazy: a deployment repository can hold unmaintained copies of open-source
    apps, and every dataset they declare would otherwise be reported as having
    two producers. Which trees are in the graph is a judgement, not a discovery.
    """
    files: List[Path] = []
    overlays: List[Path] = []

    for pattern in patterns:
        for hit in sorted(_glob.glob(pattern, recursive=True)):
            path = Path(hit)
            if path.is_dir():
                candidate = path / GOVERNANCE_FILENAME
                if candidate.is_file():
                    files.append(candidate)
            elif path.name == GOVERNANCE_FILENAME:
                files.append(path)
            elif path.name.startswith("governance.") and path.suffix in (".yaml", ".yml"):
                overlays.append(path)

    return Discovery(
        files=tuple(_unique(files)),
        skipped_overlays=tuple(_unique(overlays)),
    )


def _unique(paths: Iterable[Path]) -> List[Path]:
    seen: Set[Path] = set()
    out: List[Path] = []
    for p in paths:
        resolved = p.resolve()
        if resolved not in seen:
            seen.add(resolved)
            out.append(p)
    return out


def load_pipelines(paths: Iterable[Path]) -> List[Pipeline]:
    """Read each file into a :class:`Pipeline`.

    A pipeline is identified by the directory holding its ``governance.yaml``,
    which is the same identity ``GovernanceResolver.auto_discover`` and the
    ``pipeline run`` CLI use. Where two scanned trees hold a directory of the same
    name — an open-source app and a deployment's copy of it — enough of the path
    is prepended to tell them apart, because a report naming both `grid` and
    `grid` is a report nobody can act on.
    """
    paths = list(paths)
    names = _disambiguate([p.parent for p in paths])

    pipelines: List[Pipeline] = []
    for path, name in zip(paths, names):
        config = GovernanceResolver.from_file(path).config
        pipelines.append(
            Pipeline(
                name=name,
                path=path,
                produces=tuple(config.sources.keys()),
                active=config.active,
                depends_on=(
                    tuple(config.depends_on) if config.depends_on is not None else None
                ),
            )
        )
    return pipelines


def _disambiguate(dirs: Sequence[Path]) -> List[str]:
    """Shortest trailing path segments that tell every directory apart.

    One extra segment is not enough on its own: the colliding case in practice is
    an open-source `apps/grid` against a deployment's copy at `apps.legacy/grid`
    or another checkout's `apps/grid`, and the parent is `apps` on both sides. So
    the depth grows until the names are actually distinct, per colliding group
    rather than globally — one collision should not lengthen every other name.
    """
    resolved = [d.resolve() for d in dirs]
    names = [d.name for d in resolved]
    max_depth = max((len(d.parts) for d in resolved), default=1)

    for depth in range(2, max_depth + 1):
        counts = Counter(names)
        if all(count == 1 for count in counts.values()):
            break
        names = [
            "/".join(d.parts[-depth:]) if counts[name] > 1 else name
            for d, name in zip(resolved, names)
        ]

    return names


# =============================================================================
# Matching
# =============================================================================


def matches(pattern: str, key: str) -> bool:
    """Does a ``depends_on`` pattern name a dataset a ``sources`` key declares?

    Matched **both ways**, deliberately. Either side may be a glob: a consumer
    writes ``datasets.*_gold.pv_overture_buildings`` because the schema segment
    differs per deployment, and a producer may itself declare a family of datasets
    under one pattern.

    The two-sided form is what makes the schema segment survivable. Every
    ``sources`` key spells ``ds_dev_*``, which is the platform default and stays
    the default everywhere — environments are separated by infrastructure, not by
    renaming schemas. But a consumer resolves the schema through
    ``CELINE_SILVER_SCHEMA`` / ``CELINE_GOLD_SCHEMA``, which a deployment may point
    anywhere, and a literal-only match would then bind a declared dependency to one
    deployment's naming. A missing edge is worse than missing metadata: metadata
    falls back to a default, an ordering is read as an instruction.

    ``fnmatchcase``, not ``fnmatch``: the latter applies ``os.path.normcase``, so
    the case-sensitivity of a governance file would depend on the host OS.

    Two globs generally do not match each other, and that is accepted rather than
    solved — it needs a pattern-intersection algorithm to earn a case that has not
    come up.
    """
    return fnmatch.fnmatchcase(key, pattern) or fnmatch.fnmatchcase(pattern, key)


# =============================================================================
# Building
# =============================================================================


def build_graph(pipelines: Sequence[Pipeline]) -> DependencyGraph:
    """Resolve every declared dependency against every declared output."""
    edges: List[Edge] = []
    findings: List[Finding] = []

    findings.extend(_duplicate_producer_findings(pipelines))

    for consumer in pipelines:
        for dep in consumer.depends_on or ():
            hits = [
                (producer.name, key)
                for producer in pipelines
                for key in producer.produces
                if matches(dep.dataset, key)
            ]

            own = [name for name, _ in hits if name == consumer.name]
            if own:
                findings.append(
                    Finding(
                        kind=SELF_DEPENDENCY,
                        detail=(
                            f"{consumer.name} depends on {dep.dataset}, which it "
                            f"produces itself — dropped from the graph"
                        ),
                        pipeline=consumer.name,
                        dataset=dep.dataset,
                    )
                )
            hits = [(name, key) for name, key in hits if name != consumer.name]

            if not hits:
                # `not own` matters: a dependency satisfied only by the pipeline
                # itself is already reported above, and the dataset demonstrably
                # exists — calling it unresolved as well would send the reader
                # hunting for a producer that is in front of them.
                if not dep.external and not own:
                    findings.append(
                        Finding(
                            kind=UNRESOLVED,
                            detail=(
                                f"{consumer.name} depends on {dep.dataset}, which no "
                                f"scanned pipeline produces and which is not marked "
                                f"`external: true` — a typo, or a tree not globbed"
                            ),
                            pipeline=consumer.name,
                            dataset=dep.dataset,
                        )
                    )
                continue

            if dep.external:
                findings.append(
                    Finding(
                        kind=EXTERNAL_SATISFIED,
                        detail=(
                            f"{consumer.name} marks {dep.dataset} external, but "
                            f"{', '.join(sorted({n for n, _ in hits}))} produces it in "
                            f"this scan — the graph closed"
                        ),
                        pipeline=consumer.name,
                        dataset=dep.dataset,
                    )
                )

            for producer_name, key in hits:
                edges.append(
                    Edge(
                        producer=producer_name,
                        consumer=consumer.name,
                        dataset=key,
                        pattern=dep.dataset,
                        optional=dep.optional,
                    )
                )

    graph = DependencyGraph(
        pipelines=tuple(pipelines),
        edges=tuple(edges),
        findings=tuple(findings),
    )

    inactive = {p.name for p in pipelines if not p.active}
    for edge in edges:
        if edge.producer in inactive and edge.consumer not in inactive:
            findings.append(
                Finding(
                    kind=INACTIVE_PRODUCER,
                    detail=(
                        f"{edge.consumer} is active and reads {edge.dataset} from "
                        f"{edge.producer}, which is marked inactive — whatever it reads "
                        f"is as old as the last time that pipeline ran"
                    ),
                    pipeline=edge.consumer,
                    dataset=edge.dataset,
                )
            )

    cycled = {p.name for p in pipelines} - {n for tier in graph.tiers() for n in tier}
    if cycled:
        findings.append(
            Finding(
                kind=CYCLE,
                detail=(
                    "cycle, or downstream of one — no run order exists for: "
                    + ", ".join(sorted(cycled))
                ),
            )
        )

    return DependencyGraph(
        pipelines=tuple(pipelines),
        edges=tuple(edges),
        findings=tuple(_ordered(findings)),
    )


def _duplicate_producer_findings(pipelines: Sequence[Pipeline]) -> List[Finding]:
    """One dataset declared by two files has two answers to "who owns this".

    Compared by **exact key**, not by glob. A glob-vs-glob comparison would report
    every broad pattern as colliding with everything it covers, and the signal
    worth having is the literal one: two files claiming the same dataset.
    """
    claims: Dict[str, List[str]] = {}
    for pipeline in pipelines:
        for key in pipeline.produces:
            claims.setdefault(key, []).append(pipeline.name)

    return [
        Finding(
            kind=MULTIPLE_PRODUCERS,
            detail=(
                f"{key} is declared by {', '.join(sorted(owners))} — two answers to "
                f"who owns it, and an ambiguous producer for anything depending on it"
            ),
            dataset=key,
        )
        for key, owners in sorted(claims.items())
        if len(set(owners)) > 1
    ]


# =============================================================================
# Schedules
# =============================================================================


def load_flows(path: Path) -> List[Flow]:
    """Read a deployment's scheduled flows.

    The format is the one a Prefect deployment manifest already has, reduced to
    what ordering depends on::

        flows:
          - name: weather
            path: /pipelines/apps/weather/flows/pipeline.py   # or: app: weather
            schedule:
              cron: "15 * * * *"

    ``app`` may be given directly or derived from ``path``, because a manifest
    carries the app in the path and asking someone to restate it is asking for
    two answers. An entry without a cron is not scheduled and is skipped: a flow
    triggered by hand has no ordering to check.

    Schedules are **not** read from ``governance.yaml``, and that is a decision
    rather than an omission. A cron is a deployment fact — the same pipeline runs
    on different schedules in staging and production, and one app's flows run on
    several — while a governance file is one per app and is shared across every
    deployment that installs it.
    """
    import yaml

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    flows: List[Flow] = []
    for entry in raw.get("flows") or []:
        cron = ((entry.get("schedule") or {}).get("cron")) or entry.get("cron")
        if not cron:
            continue
        app = entry.get("app") or _app_from_path(entry.get("path") or "")
        if not app:
            continue
        flows.append(Flow(name=entry.get("name") or app, app=app, cron=str(cron)))
    return flows


def _app_from_path(path: str) -> Optional[str]:
    parts = Path(path).parts
    for index, part in enumerate(parts):
        if part.startswith("apps") and index + 1 < len(parts):
            return parts[index + 1]
    return None


def _minutes(field: str) -> Set[int]:
    """Expand a cron minute field into the minutes it fires on.

    Handles the four forms the deployment actually uses — ``*``, ``*/5``,
    ``2,12,22``, ``3-58/5`` — plus ``20/15``, which is not standard cron but is
    accepted by Prefect as "from 20, every 15".
    """
    out: Set[int] = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            part, _, raw_step = part.partition("/")
            step = int(raw_step) if raw_step.isdigit() else 1
        if part in ("*", ""):
            start, end = 0, 59
        elif "-" in part:
            raw_start, _, raw_end = part.partition("-")
            start, end = int(raw_start), int(raw_end)
        elif part.isdigit():
            # A bare number with a step means "from here on", not "only here".
            start = int(part)
            end = 59 if step > 1 else start
        else:
            continue
        out |= set(range(start, end + 1, step))
    return out


def _hourly(cron: str) -> bool:
    """Does this fire every hour of every day? Only then are minutes comparable."""
    fields = cron.split()
    return len(fields) == 5 and all(f == "*" for f in fields[1:])


def check_schedules(graph: "DependencyGraph", flows: Sequence[Flow]) -> List[Finding]:
    """Compare the declared graph against a deployment's crons.

    Two checks, and both are deliberately narrow: a false ordering report costs
    more than a missed one, because it teaches the reader to ignore the output.

    **Collision** — the producer and the consumer can start in the same minute.
    Whether the consumer sees this run's output or the previous one then depends
    on which container starts first, which nothing controls.

    **Inversion** — both fire hourly, never in the same minute, and the consumer
    is always earlier in the hour. It succeeds every time and is always reading
    the previous hour's data, so nothing ever reports it.

    Pairs on different periods — hourly against daily, daily against monthly — are
    compared only for an identical cron string. Reasoning about a real offset
    between them needs the run duration, which is not in any file.
    """
    by_app: Dict[str, List[Flow]] = {}
    for flow in flows:
        by_app.setdefault(flow.app, []).append(flow)

    findings: List[Finding] = []
    seen: Set[Tuple[str, str]] = set()

    for edge in graph.edges:
        producer_app = _app_of(graph, edge.producer)
        consumer_app = _app_of(graph, edge.consumer)
        if (edge.producer, edge.consumer) in seen:
            continue
        seen.add((edge.producer, edge.consumer))

        # One flow each side means the pairing is the one that moves the data.
        # More than one and it may not be: `demo3` deploys a five-minute meters
        # flow and a monthly batch flow, and only the batch writes the grid
        # tables `grid` reads. Comparing the meters flow against `grid-nowcast`
        # produces a collision that is arithmetically true and means nothing.
        ambiguous = (
            len(by_app.get(producer_app, ())) > 1 or len(by_app.get(consumer_app, ())) > 1
        )

        for producer in by_app.get(producer_app, ()):
            for consumer in by_app.get(consumer_app, ()):
                findings.extend(_compare(edge, producer, consumer, ambiguous))

    scheduled = set(by_app)
    for pipeline in graph.pipelines:
        app = _app_of(graph, pipeline.name)
        if pipeline.active and app not in scheduled:
            findings.append(
                Finding(
                    kind=NOT_DEPLOYED,
                    detail=(
                        f"{pipeline.name} is marked active but no scheduled flow in "
                        f"this deployment runs it"
                    ),
                    pipeline=pipeline.name,
                )
            )

    return findings


def _app_of(graph: "DependencyGraph", name: str) -> str:
    """The directory name, even when the node was disambiguated by its path."""
    return name.rsplit("/", 1)[-1]


def _compare(
    edge: Edge, producer: Flow, consumer: Flow, ambiguous: bool = False
) -> List[Finding]:
    caveat = (
        " — one of these apps deploys several flows, so this pairing may not be the"
        " one that moves the data"
        if ambiguous
        else ""
    )

    def finding(kind: str, detail: str) -> List[Finding]:
        return [
            Finding(
                kind=SCHEDULE_UNVERIFIED if ambiguous else kind,
                detail=detail + caveat,
                pipeline=edge.consumer,
                dataset=edge.dataset,
            )
        ]

    if producer.cron == consumer.cron:
        return finding(
            SCHEDULE_COLLISION,
            f"{consumer.name} and {producer.name} both run '{producer.cron}', yet "
            f"{edge.consumer} reads {edge.dataset} from {edge.producer} — which run "
            f"it sees depends on which starts first",
        )

    if not (_hourly(producer.cron) and _hourly(consumer.cron)):
        return []

    producer_minutes = _minutes(producer.cron.split()[0])
    consumer_minutes = _minutes(consumer.cron.split()[0])

    if producer_minutes & consumer_minutes:
        shared = sorted(producer_minutes & consumer_minutes)
        return finding(
            SCHEDULE_COLLISION,
            f"{consumer.name} ('{consumer.cron}') and {producer.name} "
            f"('{producer.cron}') both fire at minute "
            f"{', '.join(str(m) for m in shared[:6])}"
            f"{'…' if len(shared) > 6 else ''}, and {edge.consumer} reads "
            f"{edge.dataset} from {edge.producer}",
        )

    if (
        consumer_minutes
        and producer_minutes
        and max(consumer_minutes) < min(producer_minutes)
    ):
        return finding(
            SCHEDULE_INVERSION,
            f"{consumer.name} ('{consumer.cron}') always runs before {producer.name} "
            f"('{producer.cron}') within the hour, yet {edge.consumer} reads "
            f"{edge.dataset} from {edge.producer} — it succeeds every time on the "
            f"previous hour's data",
        )

    return []


_ORDER = [
    UNRESOLVED,
    SCHEDULE_INVERSION,
    SCHEDULE_COLLISION,
    SCHEDULE_UNVERIFIED,
    MULTIPLE_PRODUCERS,
    CYCLE,
    SELF_DEPENDENCY,
    INACTIVE_PRODUCER,
    NOT_DEPLOYED,
    EXTERNAL_SATISFIED,
]


def _ordered(findings: Iterable[Finding]) -> List[Finding]:
    return sorted(
        findings,
        key=lambda f: (
            _ORDER.index(f.kind) if f.kind in _ORDER else len(_ORDER),
            f.pipeline or "",
            f.dataset or "",
        ),
    )
