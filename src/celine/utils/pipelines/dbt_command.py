"""Translate a CELINE selector spec into a dbt CLI argument list.

A pipeline stage names what it wants as one string — ``"silver"``,
``"-s gold,tag:wind"``, ``"test -s tag:meters"``, ``"build --exclude tag:slow"`` —
and this module turns that into the argv dbt is actually given. Kept separate from
:mod:`celine.utils.pipelines.pipeline_runner`, and importing nothing but ``shlex``,
so it is testable under a core-only install where ``openlineage`` and ``sqlalchemy``
are absent.
"""

from __future__ import annotations

import shlex

# dbt subcommands a stage may name in leading position. A first token outside this
# set is a node selector, not a verb — which is why a model named `run` or `build`
# is shadowed and has to be selected explicitly with `-s`.
DBT_VERBS = ("run", "build", "test", "seed", "snapshot")

#: What a spec means when it names no verb: `"silver"` is `dbt run --select silver`.
DEFAULT_VERB = "run"

# Flags that already carry the node selection, so `--select` is not injected.
_SELECTION_FLAGS = frozenset({"-s", "--select", "--selector", "-m", "--models"})


def _is_selection_flag(part: str) -> bool:
    # `--select=silver` is the same instruction as `--select silver`; a plain
    # membership test misses the joined form and injects a second `--select`.
    return part.split("=", 1)[0] in _SELECTION_FLAGS


def build_dbt_command(spec: str) -> list[str]:
    """Build the dbt argv for ``spec``.

    ``"silver"``                -> ``dbt --no-use-colors run --select silver``
    ``"-s gold,tag:wind"``      -> ``dbt --no-use-colors run -s gold,tag:wind``
    ``"build -s silver"``       -> ``dbt --no-use-colors build -s silver``
    ``"test"``                  -> ``dbt --no-use-colors test``
    ``"staging --exclude x"``   -> ``dbt --no-use-colors run --select staging --exclude x``
    """
    parts = shlex.split(spec)

    verb = DEFAULT_VERB
    if parts and parts[0] in DBT_VERBS:
        verb, parts = parts[0], parts[1:]

    command = ["dbt", "--no-use-colors", verb]

    if parts and not any(_is_selection_flag(p) for p in parts):
        command.append("--select")

    return command + parts
