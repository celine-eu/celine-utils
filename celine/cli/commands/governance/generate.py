import typer
import requests
import yaml
from pathlib import Path
from typing import Optional
import os

from celine.common.logger import get_logger
from celine.pipelines.utils import get_namespace
from celine.common.keycloak import KeycloakClient, KeycloakClientConfig

logger = get_logger(__name__)

generate_app = typer.Typer(help="Generate governance.yaml from Marquez datasets")

# =============================================================================
# Internal helpers
# =============================================================================

COMMON_LICENSES = [
    "ODbL-1.0",
    "CC-BY-4.0",
    "CC0-1.0",
    "proprietary",
]

COMMON_ACCESS = [
    "internal",
    "public",
    "restricted",
]

COMMON_CLASSIFICATION = [
    "green",
    "yellow",
    "red",
    "pii",
]


def _get_auth_headers() -> dict[str, str]:
    cfg = KeycloakClientConfig()
    if not cfg.client_id or not cfg.client_secret:
        return {}

    kc = KeycloakClient(cfg)

    token: str | None = None
    try:
        logger.debug(f"Fetching JWT token {cfg.client_id} from {cfg.server_url}")
        token = kc.get_access_token()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch token: {e}")

    logger.debug(f"Got JWT token for {cfg.client_id}")
    return {"Authorization": f"Bearer {token}"}


def _resolve_marquez_url(cli_url: Optional[str]) -> str:
    if cli_url:
        return cli_url.rstrip("/")
    env = os.getenv("OPENLINEAGE_URL")
    if env:
        return env.rstrip("/")
    return "http://localhost:5000"


def _resolve_namespace(app_name: str, cli_namespace: Optional[str]) -> str:
    if cli_namespace:
        return cli_namespace

    env_ns = os.getenv("OPENLINEAGE_NAMESPACE")
    if env_ns:
        return env_ns

    return get_namespace(app_name)


def _fetch_marquez_datasets(marquez_url: str, namespace: str) -> list[str]:
    url = f"{marquez_url}/api/v1/namespaces/{namespace}/datasets"
    logger.debug(f"Fetching datasets from {url}")

    headers = _get_auth_headers()
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise RuntimeError(f"Marquez request failed: {resp.status_code} - {resp.text}")

    data = {}
    try:
        data = resp.json()
    except Exception as e:
        logger.debug(f"{resp.text}")
        raise RuntimeError(f"Failed to parse response ({resp.status_code}): {e}")

    return [d["name"] for d in data.get("datasets", [])]


# =============================================================================
# Interactive input helpers (Typer-native)
# =============================================================================


def _choose_with_custom(prompt: str, choices: list[str], default=None):
    typer.echo(prompt)
    for i, c in enumerate(choices, start=1):
        typer.echo(f"  {i}) {c}")
    typer.echo("  c) Custom value")

    value = typer.prompt("Choose", default=str(default) if default else "1")

    if value == "c":
        return typer.prompt("Enter custom value")

    try:
        idx = int(value)
        if 1 <= idx <= len(choices):
            return choices[idx - 1]
    except ValueError:
        pass

    typer.echo("Invalid choice, using default")
    return default


def _ask_tags():
    tags = []
    while True:
        tag = typer.prompt("Add a tag (leave blank to stop)", default="")
        if not tag:
            break
        tags.append(tag)
    return tags


def _pattern_suggestion(fullname: str) -> tuple[str, str, str]:
    parts = fullname.split(".")
    if len(parts) >= 3:
        schema = ".".join(parts[:2]) + ".*"
        prefix = parts[0] + ".*"
        return fullname, schema, prefix
    return fullname, fullname, fullname


# =============================================================================
# Main interactive generation
# =============================================================================


def _interactive_build(datasets: list[str]) -> dict:
    typer.echo("Interactive governance.yaml builder\n")

    yaml_doc = {
        "defaults": {
            "license": None,
            "ownership": [],
            "access_level": "internal",
            "classification": "green",
            "tags": [],
            "retention_days": 365,
            "documentation_url": None,
            "source_system": None,
        },
        "sources": {},
    }

    typer.echo("Datasets discovered:")
    for d in datasets:
        typer.echo(f" - {d}")

    typer.echo("\nStarting metadata collection...\n")

    for d in datasets:
        typer.echo(f"\nDataset: {d}")

        if not typer.confirm("Configure this dataset?", default=True):
            typer.echo("Skipping")
            continue

        exact, schema_wildcard, prefix_wildcard = _pattern_suggestion(d)

        typer.echo("Choose pattern scope:")
        typer.echo(f"  1) exact match:      {exact}")
        typer.echo(f"  2) schema wildcard:  {schema_wildcard}")
        typer.echo(f"  3) prefix wildcard:  {prefix_wildcard}")

        choice = typer.prompt("Pattern choice", default="1")
        if choice == "1":
            pattern = exact
        elif choice == "2":
            pattern = schema_wildcard
        elif choice == "3":
            pattern = prefix_wildcard
        else:
            pattern = exact

        license_val = _choose_with_custom(
            "License:", COMMON_LICENSES, default="ODbL-1.0"
        )
        access_val = _choose_with_custom(
            "Access level:", COMMON_ACCESS, default="internal"
        )
        class_val = _choose_with_custom(
            "Classification:", COMMON_CLASSIFICATION, default="green"
        )

        owner = typer.prompt("Owner (leave empty to skip)", default="")
        ownership = [{"name": owner, "type": "DATA_OWNER"}] if owner else []

        tags = _ask_tags()

        yaml_doc["sources"][pattern] = {
            "license": license_val,
            "ownership": ownership,
            "access_level": access_val,
            "classification": class_val,
            "tags": tags,
        }

    return yaml_doc


# =============================================================================
# CLI command
# =============================================================================


@generate_app.command("marquez")
def generate_governance_from_marquez(
    app_name: str = typer.Option(..., "--app", help="CELINE app name"),
    output_path: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output path (default: PIPELINES_ROOT/apps/<app>/governance.yaml)",
    ),
    marquez_url: Optional[str] = typer.Option(
        None, "--marquez", help="Marquez base URL (overrides env OPENLINEAGE_URL)"
    ),
    namespace: Optional[str] = typer.Option(
        None,
        "--namespace",
        help="OpenLineage namespace (overrides OPENLINEAGE_NAMESPACE)",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Non-interactive mode (use defaults)"
    ),
):
    mz_url = _resolve_marquez_url(marquez_url)
    typer.echo(f"Using Marquez URL: {mz_url}")

    ns = _resolve_namespace(app_name, namespace)
    typer.echo(f"Using OpenLineage namespace: {ns}")

    datasets = _fetch_marquez_datasets(mz_url, ns)
    if not datasets:
        typer.echo(f"No datasets found in namespace '{ns}'")
        raise typer.Exit(1)

    typer.echo(f"Discovered {len(datasets)} datasets\n")

    if yes:
        yaml_doc = {
            "defaults": {
                "license": None,
                "ownership": [],
                "access_level": "internal",
                "classification": "green",
                "tags": [],
                "retention_days": 365,
                "documentation_url": None,
                "source_system": None,
            },
            "sources": {name: {} for name in datasets},
        }
    else:
        yaml_doc = _interactive_build(datasets)

    if output_path:
        target = Path(output_path)
    else:
        pipeline_root = os.environ.get("PIPELINES_ROOT")
        if pipeline_root:
            target = Path(pipeline_root) / "apps" / app_name / "governance.yaml"
        else:
            target = Path("./governance.yaml")

    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w") as f:
        yaml.safe_dump(yaml_doc, f, sort_keys=False)

    typer.echo(f"Generated governance.yaml -> {target}")
