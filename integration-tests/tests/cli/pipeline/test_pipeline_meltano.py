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


def test_meltano_cli_invocation(monkeypatch, temp_project, celine_cli):
    """
    We don't need meltano installed — pipeline code must NOT crash on discovery.
    """
    root, demo = temp_project

    monkeypatch.setenv("PIPELINES_ROOT", str(root))
    monkeypatch.setenv("APP_NAME", "demo_app")

    monkeypatch.chdir(demo)

    proc = subprocess.run(
        celine_cli + ["pipeline", "run", "meltano", "run import"],
        text=True,
        capture_output=True,
    )

    # Even if meltano is not installed → our CLI should not crash
    assert proc.returncode in (0, 1)
    assert "dev" in proc.stdout
