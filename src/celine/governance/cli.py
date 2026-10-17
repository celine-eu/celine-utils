"""``celine-governance`` — the governance checks, on the thin core.

``argparse``, not typer: this entry point must run wherever ``celine.governance``
imports, which is on ``pydantic``, ``pyyaml`` and ``jsonschema`` alone. The
``celine-utils`` CLI needs the ``pipelines`` extra and cannot be the gate a
deployment runs before it imports a catalogue.

Exit codes: 0 every check passed, 1 a check failed, 2 the command was misused.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional, Sequence

from celine.governance.check import ERROR, INFO, WARNING, CheckReport, check_governance


def _split(values: Optional[List[str]]) -> List[str]:
    return [v.strip() for value in values or [] for v in value.split(",") if v.strip()]


def _print_text(report: CheckReport, *, strict: bool) -> None:
    for f in report.findings:
        severity = ERROR if (strict and f.severity == WARNING) else f.severity
        where = f"{f.source}: {f.dataset}" if f.dataset else f.source
        print(f"{severity.upper():7} {f.rule:17} {where}: {f.detail}")
    counts = {s: 0 for s in (ERROR, WARNING, INFO)}
    for f in report.findings:
        counts[f.severity] += 1
    overlays = sum(1 for _, o in report.files if o)
    print(
        f"{len(report.files)} file(s) ({overlays} with an overlay), "
        f"{report.datasets} dataset(s): {counts[ERROR]} error(s), "
        f"{counts[WARNING]} warning(s){' (fatal: --strict)' if strict else ''}, "
        f"{counts[INFO]} note(s) — {'OK' if report.ok(strict=strict) else 'FAILED'}"
    )


def _validate(args: argparse.Namespace) -> int:
    report = check_governance(
        args.paths,
        overlay_dirs=args.overlay_dir or [],
        owners_files=args.owners or [],
        handlers=_split(args.handlers),
        dataspace=args.dataspace,
    )
    if args.format == "json":
        json.dump(report.as_dict(strict=args.strict), sys.stdout, indent=2)
        print()
    else:
        _print_text(report, strict=args.strict)
    return 0 if report.ok(strict=args.strict) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="celine-governance",
        description="Check governance.yaml trees before anything imports them.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser(
        "validate",
        help="validate governance files, materialized with their overlays",
        description=(
            "Validate every governance.yaml under PATHS — each merged with its "
            "deployer overlay governance.<app>.yaml, found beside it or in an "
            "--overlay-dir — against the schema, the row policy, the handler "
            "names, consent coherence and the owners registry. Collects every "
            "finding; exits 1 if any error remains."
        ),
    )
    validate.add_argument(
        "paths",
        nargs="+",
        metavar="PATH",
        help="a governance.yaml, a directory searched recursively, or a glob",
    )
    validate.add_argument(
        "--strict", action="store_true", help="warnings fail the run too"
    )
    validate.add_argument(
        "--overlay-dir",
        action="append",
        metavar="DIR",
        help="another directory holding governance.<app>.yaml overlays (repeatable)",
    )
    validate.add_argument(
        "--owners",
        action="append",
        metavar="FILE",
        help="an owners registry; several merge, later shadowing earlier (repeatable)",
    )
    validate.add_argument(
        "--handlers",
        action="append",
        metavar="NAME[,NAME…]",
        help="row filter handlers the deployment adds as plugins (repeatable)",
    )
    validate.add_argument(
        "--dataspace",
        action="store_true",
        help="also require a DID for every owner of an organization-bound dataspace offer",
    )
    validate.add_argument(
        "--format", choices=("text", "json"), default="text", help="output format"
    )
    validate.set_defaults(func=_validate)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
