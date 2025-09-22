import pytest
import os
from pathlib import Path

from celine.pipelines.pipeline_config import PipelineConfig

ROOT = Path(__file__).parent


@pytest.fixture(scope="session", autouse=True)
def set_envs():
    """
    Ensure required environment variables are set for integration tests.
    """
    test_app_root = Path(ROOT.parent / "apps" / "test")

    env_defaults = {
        "POSTGRES_HOST": "172.17.0.1",
        "PIPELINES_ROOT": "./",  # point to test apps
        "APP_NAME": "test",
        "POSTGRES_USER": "postgres",
        "POSTGRES_PASSWORD": "postgres",
        "POSTGRES_PORT": "15432",
        "POSTGRES_DB": "datasets",
        "MELTANO_PROJECT_ROOT": str(test_app_root / "meltano"),
        "DBT_PROJECT_DIR": str(test_app_root / "dbt"),
        "DBT_PROFILES_DIR": str(test_app_root / "dbt"),
        "OPENLINEAGE_URL": "http://172.17.0.1:5000",
    }

    for key, value in env_defaults.items():
        os.environ[key] = value

    # yield control back to pytest
    yield


@pytest.fixture(scope="session")
def pipeline_cfg() -> PipelineConfig:
    """
    Build a PipelineConfig object from env vars for tests.
    """
    return PipelineConfig()
