"""``celine-utils governance graph`` — the inter-pipeline dependency graph.

A presenter. Every decision about what the graph *is* lives in
:mod:`celine.governance.graph`, which is in the thin core so that a scheduler or
an API service can read the same graph without installing a CLI. This module
turns it into something to look at, and decides an exit code.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import List, Optional

import typer

from celine.governance.graph import (
    _ORDER,
    EXTERNAL_SATISFIED,
    SCHEDULE_UNVERIFIED,
    DependencyGraph,
    Finding,
    build_graph,
    check_schedules,
    discover,
    load_flows,
    load_pipelines,
)


def _finding_order(finding: Finding):
    kind = finding.kind
    return (
        _ORDER.index(kind) if kind in _ORDER else len(_ORDER),
        finding.pipeline or "",
        finding.dataset or "",
    )

# Everything except `external-satisfied` is something to fix. That one reports
# that a wider scan closed the graph, which is the system working.
INFORMATIONAL = {EXTERNAL_SATISFIED, SCHEDULE_UNVERIFIED}


def _render_order(graph: DependencyGraph) -> str:
    """The flat run order — what to run, in what sequence, one at a time."""
    by_name = {p.name: p for p in graph.pipelines}
    width = len(str(len(graph.run_order())))
    lines = []
    for position, name in enumerate(graph.run_order(), start=1):
        suffix = "" if by_name[name].active else "   # inactive"
        lines.append(f"{position:>{width}}. {name}{suffix}")
    return "\n".join(lines)


def _render_tree(graph: DependencyGraph) -> str:
    lines: List[str] = []
    by_name = {p.name: p for p in graph.pipelines}

    for index, tier in enumerate(graph.tiers()):
        lines.append(f"\ntier {index}")
        for name in tier:
            upstreams = graph.producers_of(name)
            pipeline = by_name[name]
            # A separate label: `name` stays the identity the edges are keyed on.
            label = name if pipeline.active else f"{name} (inactive)"
            if upstreams:
                optional = {
                    e.producer for e in graph.edges if e.consumer == name and e.optional
                }
                inactive = {u for u in upstreams if not by_name[u].active}
                shown = ", ".join(
                    u
                    + (" (optional)" if u in optional else "")
                    + (" (inactive)" if u in inactive else "")
                    for u in upstreams
                )
                lines.append(f"  {label}  <- {shown}")
            elif not pipeline.declared:
                lines.append(f"  {label}  (inputs not declared)")
            else:
                lines.append(f"  {label}")

    # Only the ones that actually resolved to nothing. An `external: true` entry
    # that a wider scan satisfied is an ordinary edge and is already drawn above;
    # listing it here as well would tell the reader to go and find a producer that
    # the tier table just showed them.
    resolved = {e.pattern for e in graph.edges}
    unmet = sorted(
        {
            d.dataset
            for p in graph.pipelines
            for d in (p.depends_on or ())
            if d.external and d.dataset not in resolved
        }
    )
    if unmet:
        lines.append("\nsatisfied outside this scan")
        for dataset in unmet:
            lines.append(f"  {dataset}")

    return "\n".join(lines)


def _render_mermaid(graph: DependencyGraph) -> str:
    lines = ["graph LR"]
    for pipeline in graph.pipelines:
        lines.append(f'    {_ident(pipeline.name)}["{pipeline.name}"]')
    for edge in graph.edges:
        arrow = "-.->" if edge.optional else "-->"
        lines.append(f"    {_ident(edge.producer)} {arrow} {_ident(edge.consumer)}")
    return "\n".join(lines)


def _ident(name: str) -> str:
    return name.replace("/", "__").replace("-", "_").replace(".", "_")


def _render_json(graph: DependencyGraph) -> str:
    return json.dumps(
        {
            "pipelines": [
                {
                    "name": p.name,
                    "path": str(p.path),
                    "declared": p.declared,
                    "produces": list(p.produces),
                    "depends_on": [d.model_dump() for d in (p.depends_on or ())],
                }
                for p in graph.pipelines
            ],
            "edges": [
                {
                    "producer": e.producer,
                    "consumer": e.consumer,
                    "dataset": e.dataset,
                    "pattern": e.pattern,
                    "optional": e.optional,
                }
                for e in graph.edges
            ],
            "tiers": graph.tiers(),
            "findings": [
                {
                    "kind": f.kind,
                    "pipeline": f.pipeline,
                    "dataset": f.dataset,
                    "detail": f.detail,
                }
                for f in graph.findings
            ],
        },
        indent=2,
    )


def graph_command(
    paths: Optional[List[str]] = typer.Argument(
        None,
        help=(
            "Globs naming pipelines or governance files — 'apps/*', "
            "'apps/*/governance.yaml', '../other/pipelines/apps/*'. A directory "
            "contributes the governance.yaml inside it. Defaults to 'apps/*'."
        ),
    ),
    output: str = typer.Option(
        "tree", "--format", "-f", help="tree | order | json | mermaid"
    ),
    schedules: Optional[str] = typer.Option(
        None,
        "--schedules",
        help=(
            "A deployment's scheduled flows, to check the crons against the graph. "
            "YAML: flows: [{name, app or path, schedule: {cron}}]."
        ),
    ),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Exit 1 when anything other than an informational finding is reported.",
    ),
):
    """Show which pipelines feed which, from every governance.yaml matched.

    `sources:` in each file says what a pipeline produces and `depends_on:` says
    what it consumes; this resolves the second against the first across every file
    given. Each dbt project stops at its own boundary, so this is the only view of
    the ordering across pipelines.

    **Which trees you glob is a judgement, not a detail.** A deployment repository
    may hold unmaintained copies of open-source apps, and including them reports
    every dataset they declare as having two producers.
    """
    patterns = paths or ["apps/*"]

    found = discover(patterns)
    if not found.files:
        typer.echo(f"No governance.yaml matched: {', '.join(patterns)}", err=True)
        raise typer.Exit(1)

    graph = build_graph(load_pipelines(found.files))

    if schedules:
        schedule_path = Path(schedules)
        if not schedule_path.is_file():
            typer.echo(f"No such schedules file: {schedule_path}", err=True)
            raise typer.Exit(2)
        flows = load_flows(schedule_path)
        graph = replace(
            graph,
            findings=tuple(
                sorted(
                    list(graph.findings) + check_schedules(graph, flows),
                    key=_finding_order,
                )
            ),
        )

    if output == "json":
        typer.echo(_render_json(graph))
    elif output == "mermaid":
        typer.echo(_render_mermaid(graph))
    elif output == "order":
        typer.echo(_render_order(graph))
    elif output == "tree":
        typer.echo(_render_tree(graph))
    else:
        typer.echo(
            f"Unknown format '{output}'. Use tree, order, json or mermaid.", err=True
        )
        raise typer.Exit(2)

    actionable = [f for f in graph.findings if f.kind not in INFORMATIONAL]

    if output != "json":
        declared = sum(1 for p in graph.pipelines if p.declared)
        typer.echo(
            f"\n{len(graph.pipelines)} pipelines "
            f"({declared} declaring inputs), {len(graph.edges)} edges",
            err=True,
        )
        for path in found.skipped_overlays:
            typer.echo(f"  skipped overlay: {path}", err=True)
        for finding in graph.findings:
            typer.echo(f"  {finding.kind}: {finding.detail}", err=True)

    if strict and actionable:
        raise typer.Exit(1)
