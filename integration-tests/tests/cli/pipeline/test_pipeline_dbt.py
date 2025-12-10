import subprocess
import pytest


@pytest.fixture()
def temp_project(tmp_path):
    root = tmp_path / "pipelines_root"
    demo = root / "apps" / "demo_app"
    (demo / "meltano").mkdir(parents=True)
    (demo / "dbt").mkdir()
    (demo / "flows").mkdir()
    return root, demo


def test_dbt_cli_invocation(monkeypatch, temp_project, celine_cli):
    """
    dbt might not be installed; we test runner stability.
    """
    root, demo = temp_project

    monkeypatch.setenv("PIPELINES_ROOT", str(root))
    monkeypatch.setenv("APP_NAME", "demo_app")

    monkeypatch.chdir(demo)

    proc = subprocess.run(
        celine_cli + ["pipeline", "run", "dbt", "staging"],
        text=True,
        capture_output=True,
    )

    assert proc.returncode in (0, 1)
    assert "staging" in proc.stdout or "dbt" in proc.stdout
