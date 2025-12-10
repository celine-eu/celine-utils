import subprocess
import pytest


@pytest.fixture()
def temp_project(tmp_path):
    root = tmp_path / "pipelines_root"
    demo = root / "apps" / "demo_app"
    flows = demo / "flows"

    (demo / "meltano").mkdir(parents=True)
    (demo / "dbt").mkdir()
    flows.mkdir()

    return root, demo, flows


def test_prefect_flow_execution(monkeypatch, temp_project, celine_cli):
    """
    Test successful execution of dynamically created flow.
    """
    root, demo, flows = temp_project

    monkeypatch.setenv("PIPELINES_ROOT", str(root))
    monkeypatch.setenv("APP_NAME", "demo_app")

    # Create test flow file in temp workspace
    (flows / "simple.py").write_text(
        "def main():\n" "    print('OK_FROM_FLOW')\n" "    return 'RESULT'\n"
    )

    monkeypatch.chdir(demo)

    proc = subprocess.run(
        celine_cli
        + [
            "pipeline",
            "run",
            "prefect",
            "--flow",
            "simple",
            "--function",
            "main",
        ],
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 0
    assert "OK_FROM_FLOW" in proc.stdout
    assert "Execution completed" in proc.stdout


def test_prefect_missing_flow(monkeypatch, temp_project, celine_cli):
    root, demo, flows = temp_project

    monkeypatch.setenv("PIPELINES_ROOT", str(root))
    monkeypatch.setenv("APP_NAME", "demo_app")

    monkeypatch.chdir(demo)

    proc = subprocess.run(
        celine_cli
        + [
            "pipeline",
            "run",
            "prefect",
            "--flow",
            "i_do_not_exist",
            "--function",
            "main",
        ],
        text=True,
        capture_output=True,
    )

    assert proc.returncode != 0
    assert "Failed loading flow" in proc.stdout


def test_prefect_missing_function(monkeypatch, temp_project, celine_cli):
    root, demo, flows = temp_project

    monkeypatch.setenv("PIPELINES_ROOT", str(root))
    monkeypatch.setenv("APP_NAME", "demo_app")

    (flows / "another.py").write_text("def okay(): return 1\n")

    monkeypatch.chdir(demo)

    proc = subprocess.run(
        celine_cli
        + [
            "pipeline",
            "run",
            "prefect",
            "--flow",
            "another",
            "--function",
            "missing",
        ],
        text=True,
        capture_output=True,
    )

    assert proc.returncode != 0
    assert "not found" in proc.stdout
