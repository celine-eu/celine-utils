import yaml
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner

from celine.utils.cli.commands.governance.generate import generate_app

runner = CliRunner()


# ------------------------------------------------------------------------------
# Helper mocks
# ------------------------------------------------------------------------------


def mock_marquez_response(names):
    """Creates a mocked Marquez API JSON structure."""
    return {
        "datasets": [{"name": n} for n in names],
        "totalCount": len(names),
    }


# ------------------------------------------------------------------------------
# Test: URL resolution
# ------------------------------------------------------------------------------


def test_resolve_marquez_url_env(monkeypatch):
    monkeypatch.setenv("OPENLINEAGE_URL", "http://env-mq:5000")

    from celine.utils.cli.commands.governance.generate import _resolve_marquez_url

    assert _resolve_marquez_url(None) == "http://env-mq:5000"


def test_resolve_marquez_url_default(monkeypatch):
    monkeypatch.delenv("OPENLINEAGE_URL", raising=False)

    from celine.utils.cli.commands.governance.generate import _resolve_marquez_url

    assert _resolve_marquez_url(None) == "http://localhost:5000"


# ------------------------------------------------------------------------------
# Test: Namespace resolution
# ------------------------------------------------------------------------------


def test_resolve_namespace_env(monkeypatch):
    monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "env.ns")

    from celine.utils.cli.commands.governance.generate import _resolve_namespace

    assert _resolve_namespace("app1", None) == "env.ns"


@patch("celine.utils.cli.commands.governance.generate.get_namespace")
def test_resolve_namespace_default(mock_get_ns, monkeypatch):
    mock_get_ns.return_value = "default.ns"
    monkeypatch.delenv("OPENLINEAGE_NAMESPACE", raising=False)

    from celine.utils.cli.commands.governance.generate import _resolve_namespace

    assert _resolve_namespace("app1", None) == "default.ns"


# ------------------------------------------------------------------------------
# Test: CLI non-interactive mode (--yes)
# ------------------------------------------------------------------------------


@patch("celine.utils.cli.commands.governance.generate.requests.get")
def test_generate_governance_yes_mode(mock_get, tmp_path, monkeypatch):
    """
    Non-interactive mode should generate a governance.yaml with defaults
    and an empty dict entry for each dataset.
    """
    datasets = ["a.raw.table1", "b.silver.table2"]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = mock_marquez_response(datasets)
    mock_get.return_value = mock_resp

    # Force PIPELINES_ROOT for auto path resolution
    monkeypatch.setenv("PIPELINES_ROOT", str(tmp_path))

    # NOTE: no "marquez" token here; this is a single-command app
    result = runner.invoke(
        generate_app,
        [
            "--app",
            "demo_app",
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.stdout

    out_path = tmp_path / "apps" / "demo_app" / "governance.yaml"
    assert out_path.exists()

    data = yaml.safe_load(out_path.read_text())

    assert "defaults" in data
    assert "sources" in data
    assert set(data["sources"].keys()) == set(datasets)


# ------------------------------------------------------------------------------
# Test: CLI with explicit output path
# ------------------------------------------------------------------------------


@patch("celine.utils.cli.commands.governance.generate.requests.get")
def test_generate_governance_output_override(mock_get, tmp_path):
    datasets = ["x.raw.ds1"]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = mock_marquez_response(datasets)
    mock_get.return_value = mock_resp

    output = tmp_path / "custom_governance.yaml"

    result = runner.invoke(
        generate_app,
        [
            "--app",
            "demo_app",
            "--yes",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert output.exists()

    data = yaml.safe_load(output.read_text())
    assert "sources" in data
    assert list(data["sources"].keys()) == datasets


# ------------------------------------------------------------------------------
# Test: No datasets
# ------------------------------------------------------------------------------


@patch("celine.utils.cli.commands.governance.generate.requests.get")
def test_generate_no_datasets(mock_get, tmp_path, monkeypatch):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = mock_marquez_response([])
    mock_get.return_value = mock_resp

    monkeypatch.setenv("PIPELINES_ROOT", str(tmp_path))

    result = runner.invoke(
        generate_app,
        [
            "--app",
            "foobaz",
            "--yes",
        ],
    )

    assert result.exit_code != 0
    assert "No datasets found in namespace" in result.stdout
