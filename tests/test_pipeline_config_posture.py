"""`PipelineConfig` refuses the dev client-secret fallback outside dev (REQ-0009).

The secret falls back to the client id, which is what the local realm expects and
what anyone can derive from the client list. Only `CELINE_ENV=dev` (then
`ENVIRONMENT`) accepts it; `PREFECT_MODE` is a scheduling switch and must not.
"""

from __future__ import annotations

import logging

import pytest

pytest.importorskip("celine.sdk.posture")

from celine.sdk.posture import InsecureConfiguration

from celine.utils.pipelines.pipeline_config import (
    PIPELINES_CLIENT_ID,
    PipelineConfig,
)

POSTURE_VARS = ("CELINE_ENV", "ENVIRONMENT", "CELINE_OIDC_CLIENT_SECRET", "PREFECT_MODE")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path):
    for name in POSTURE_VARS:
        monkeypatch.delenv(name, raising=False)
    # AppBaseSettings reads .env files from the working directory.
    monkeypatch.chdir(tmp_path)


# @verifies REQ-0009
@pytest.mark.parametrize("env", [None, "prod", "staging", "test", "local", "ci", "devv"])
def test_the_fallback_is_refused_outside_dev(monkeypatch, env):
    if env is not None:
        monkeypatch.setenv("CELINE_ENV", env)

    with pytest.raises(InsecureConfiguration, match="CELINE_OIDC_CLIENT_SECRET"):
        PipelineConfig()


# @verifies REQ-0009
@pytest.mark.parametrize("secret", ["", PIPELINES_CLIENT_ID])
def test_an_explicit_placeholder_is_refused_too(monkeypatch, secret):
    monkeypatch.setenv("CELINE_ENV", "prod")
    monkeypatch.setenv("CELINE_OIDC_CLIENT_SECRET", secret)

    with pytest.raises(InsecureConfiguration):
        PipelineConfig()


# @verifies REQ-0009
def test_a_real_secret_is_accepted_outside_dev(monkeypatch):
    monkeypatch.setenv("CELINE_ENV", "prod")
    monkeypatch.setenv("CELINE_OIDC_CLIENT_SECRET", "a-generated-secret")

    cfg = PipelineConfig()

    assert cfg.sdk.oidc.client_secret == "a-generated-secret"
    assert cfg.sdk.oidc.client_id == PIPELINES_CLIENT_ID


# @verifies REQ-0009
@pytest.mark.parametrize("name", ["CELINE_ENV", "ENVIRONMENT"])
def test_dev_keeps_the_fallback_with_a_warning(monkeypatch, caplog, name):
    monkeypatch.setenv(name, "dev")

    with caplog.at_level(logging.WARNING):
        cfg = PipelineConfig()

    assert cfg.sdk.oidc.client_secret == PIPELINES_CLIENT_ID
    assert "CELINE_OIDC_CLIENT_SECRET" in caplog.text


# @verifies REQ-0009
def test_prefect_mode_does_not_relax_it(monkeypatch):
    monkeypatch.setenv("PREFECT_MODE", "dev")

    with pytest.raises(InsecureConfiguration):
        PipelineConfig()


# @verifies REQ-0009
def test_the_secret_is_read_when_the_config_is_built(monkeypatch):
    """Not at import: a secret exported after the import must still count."""
    monkeypatch.setenv("CELINE_ENV", "prod")
    monkeypatch.setenv("CELINE_OIDC_CLIENT_SECRET", "first")
    assert PipelineConfig().sdk.oidc.client_secret == "first"

    monkeypatch.setenv("CELINE_OIDC_CLIENT_SECRET", "second")
    assert PipelineConfig().sdk.oidc.client_secret == "second"
