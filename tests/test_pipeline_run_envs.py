"""`celine-utils pipeline run envs` keeps stdout for `export` lines only.

Its output is meant to be sourced (`source <(...)`, `eval "$(...)"`). A refusal
printed on stdout reaches the shell as code and fails as a syntax error far from
its cause; on stderr it reads as what it is, and stdout stays empty.
"""

from __future__ import annotations

import os

import pytest
from typer.testing import CliRunner

pytest.importorskip("celine.sdk.posture")

from celine.utils.cli.commands.pipeline.run import pipeline_run_app

POSTURE_VARS = ("CELINE_ENV", "ENVIRONMENT", "CELINE_OIDC_CLIENT_SECRET", "PREFECT_MODE")


@pytest.fixture(autouse=True)
def app_dir(monkeypatch: pytest.MonkeyPatch, tmp_path):
    for name in POSTURE_VARS + ("APP_NAME", "PIPELINES_ROOT"):
        monkeypatch.delenv(name, raising=False)
    app = tmp_path / "apps" / "example_app"
    (app / "dbt").mkdir(parents=True)
    monkeypatch.chdir(app)
    # The command writes what it resolves into os.environ; give it back.
    saved = dict(os.environ)
    yield app
    os.environ.clear()
    os.environ.update(saved)


def test_a_refusal_goes_to_stderr_and_stdout_stays_empty():
    result = CliRunner().invoke(pipeline_run_app, ["envs"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "CELINE_OIDC_CLIENT_SECRET" in result.stderr


def test_in_dev_stdout_is_export_lines_only(monkeypatch):
    monkeypatch.setenv("CELINE_ENV", "dev")

    result = CliRunner().invoke(pipeline_run_app, ["envs"])

    assert result.exit_code == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines and all(line.startswith("export ") for line in lines)
    assert "export APP_NAME=example_app" in lines
