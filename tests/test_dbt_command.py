"""Tests for the selector-spec -> dbt argv translation.

Imports ``celine.utils.pipelines.dbt_command`` and nothing else. That module is
stdlib-only on purpose, so this file still runs under `task test:thin`, where
``openlineage`` and ``sqlalchemy`` are absent and ``pipeline_runner`` — the only
caller — cannot be imported.
"""

import pytest

from celine.utils.pipelines.dbt_command import DBT_VERBS, build_dbt_command

PREFIX = ["dbt", "--no-use-colors"]


class TestDefaultVerb:
    def test_bare_selector_becomes_a_run_with_select(self):
        assert build_dbt_command("silver") == PREFIX + ["run", "--select", "silver"]

    def test_multiple_bare_selectors_are_passed_through_after_select(self):
        assert build_dbt_command("staging silver") == PREFIX + [
            "run",
            "--select",
            "staging",
            "silver",
        ]

    def test_empty_spec_is_a_bare_run(self):
        assert build_dbt_command("") == PREFIX + ["run"]


class TestSelectionFlags:
    @pytest.mark.parametrize("flag", ["-s", "--select", "--selector", "-m", "--models"])
    def test_an_explicit_selection_flag_suppresses_injection(self, flag):
        assert build_dbt_command(f"{flag} gold,tag:wind") == PREFIX + [
            "run",
            flag,
            "gold,tag:wind",
        ]

    def test_the_joined_form_also_suppresses_injection(self):
        # `--select=x` is the same instruction as `--select x`; a plain membership
        # test misses it and appends a second `--select`.
        assert build_dbt_command("--select=silver") == PREFIX + [
            "run",
            "--select=silver",
        ]

    def test_exclude_alone_does_not_count_as_selection(self):
        assert build_dbt_command("staging --exclude tag:meters") == PREFIX + [
            "run",
            "--select",
            "staging",
            "--exclude",
            "tag:meters",
        ]


class TestLeadingVerb:
    @pytest.mark.parametrize("verb", DBT_VERBS)
    def test_a_leading_verb_is_consumed_not_selected(self, verb):
        assert build_dbt_command(verb) == PREFIX + [verb]

    def test_build_with_a_selection_flag(self):
        assert build_dbt_command("build -s silver") == PREFIX + [
            "build",
            "-s",
            "silver",
        ]

    def test_build_with_a_bare_selector_gets_select_injected(self):
        assert build_dbt_command("build silver") == PREFIX + [
            "build",
            "--select",
            "silver",
        ]

    def test_test_with_a_selection_flag_is_unchanged(self):
        # The one case the old special-casing covered; it must keep working.
        assert build_dbt_command("test -s tag:meters") == PREFIX + [
            "test",
            "-s",
            "tag:meters",
        ]

    def test_test_with_a_bare_selector_gets_select_injected(self):
        # Under the old rule this produced `dbt test tag:meters`, which dbt rejects
        # as an unexpected positional argument.
        assert build_dbt_command("test tag:meters") == PREFIX + [
            "test",
            "--select",
            "tag:meters",
        ]

    def test_a_redundant_leading_run_is_not_treated_as_a_selector(self):
        # `dbt_run("run staging")` — as apps/copernicus writes it — used to expand
        # to `run --select run staging`, selecting a node named `run` that does not
        # exist alongside the one that does.
        assert build_dbt_command("run staging") == PREFIX + [
            "run",
            "--select",
            "staging",
        ]

    def test_a_verb_in_non_leading_position_is_a_selector(self):
        assert build_dbt_command("-s build") == PREFIX + ["run", "-s", "build"]


class TestQuoting:
    def test_the_spec_is_split_as_a_shell_would(self):
        assert build_dbt_command('test -s "tag:meters tag:grid"') == PREFIX + [
            "test",
            "-s",
            "tag:meters tag:grid",
        ]
