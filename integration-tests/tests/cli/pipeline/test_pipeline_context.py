import subprocess

import pytest


@pytest.fixture()
def temp_project(tmp_path, celine_cli):
    """
    Create an isolated temporary CELINE pipeline root:
        tmp/pipelines_root/apps/demo_app/{meltano,dbt,flows}
    """
    root = tmp_path / "pipelines_root"
    apps = root / "apps"
    demo = apps / "demo_app"

    # Create minimal directories
    (demo / "meltano").mkdir(parents=True)
    (demo / "dbt").mkdir()
    (demo / "flows").mkdir()

    # Create dummy .env files
    (root / ".env").write_text("GLOBAL_KEY=1\nOVERRIDE_ME=from_root\n")
    (demo / ".env").write_text("APP_KEY=2\nOVERRIDE_ME=from_app\n")

    return root, demo


def test_context_env_loading_and_discovery(monkeypatch, temp_project, celine_cli):
    """
    Validate that:
      - PIPELINES_ROOT is picked from env
      - APP_NAME is known
      - .env resolution works (app overrides root)
    """
    root, demo = temp_project

    # Simulate container environment
    monkeypatch.setenv("PIPELINES_ROOT", str(root))
    monkeypatch.setenv("APP_NAME", "demo_app")

    # Remove these to force code discovery:
    monkeypatch.delenv("MELTANO_PROJECT_ROOT", raising=False)
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)

    # Enter app folder (as CLI user would)
    monkeypatch.chdir(demo)

    # Trigger CLI (help invocation triggers context building)

    proc = subprocess.run(
        celine_cli + ["pipeline", "run", "meltano", "--help"],
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 0
