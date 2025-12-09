import typer
import requests
import yaml
from pathlib import Path
from typing import Optional
import os
from rich import print
from rich.prompt import Confirm, Prompt
from rich.table import Table

from celine.common.logger import get_logger
from celine.pipelines.utils import get_namespace

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
    "secret",
]

COMMON_CLASSIFICATION = [
    "green",
    "yellow",
    "red",
]


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

    resp = requests.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"Marquez request failed: {resp.status_code} - {resp.text}")

    data = resp.json()
    return [d["name"] for d in data.get("datasets", [])]


# =============================================================================
# Interactive input helpers
# =============================================================================


def _choose_with_custom(prompt: str, choices: list[str], default=None):
    """
    Offer a list of choices, with an option for custom free-text input.
    """
    print(f"[bold]{prompt}[/bold]")
    for i, c in enumerate(choices, start=1):
        print(f"  {i}) {c}")
    print("  c) Custom value")
    value = Prompt.ask("Choose", default=str(default) if default else "1")

    if value == "c":
        return Prompt.ask("Enter custom value")
    try:
        idx = int(value)
        if 1 <= idx <= len(choices):
            return choices[idx - 1]
    except ValueError:
        pass
    print("[yellow]Invalid choice, using default[/yellow]")
    return default


def _ask_tags():
    tags = []
    while True:
        tag = Prompt.ask("Add a tag (leave blank to stop)", default="")
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
    """
    Build governance.yaml interactively.
    """
    print("[bold green]Interactive governance.yaml builder[/bold green]\n")

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

    # Show dataset table
    print("[bold]Datasets discovered:[/bold]")
    table = Table(show_header=True, header_style="bold")
    table.add_column("Dataset")
    for d in datasets:
        table.add_row(d)
    print(table)

    print("\nStarting metadata collection...\n")

    for d in datasets:
        print(f"\n[bold blue]Dataset: {d}[/bold blue]")

        if not Confirm.ask("Configure this dataset?", default=True):
            print("[yellow]Skipping…[/yellow]")
            continue

        exact, schema_wildcard, prefix_wildcard = _pattern_suggestion(d)

        print("\nChoose pattern scope:")
        print(f"  1) exact match:      {exact}")
        print(f"  2) schema wildcard:  {schema_wildcard}")
        print(f"  3) prefix wildcard:  {prefix_wildcard}")

        choice = Prompt.ask("Pattern choice", default="1")
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

        owner = Prompt.ask("Owner (leave empty to skip)", default="")
        ownership = [{"name": owner, "type": "DATA_OWNER"}] if owner else []

        tags = _ask_tags()

        yaml_doc["sources"][pattern] = {
            "license": license_val,
            "ownership": ownership,
            "access_level": access_val,
            "classification": class_val,
            "tags": tags,
            "retention_days": None,
            "documentation_url": None,
            "source_system": None,
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
    """
    Auto-generate governance.yaml from datasets registered in Marquez.
    Supports interactive collection of governance metadata.
    """

    mz_url = _resolve_marquez_url(marquez_url)
    print(f"[green]Using Marquez URL:[/green] {mz_url}")

    ns = _resolve_namespace(app_name, namespace)
    print(f"[green]Using OpenLineage namespace:[/green] {ns}")

    datasets = _fetch_marquez_datasets(mz_url, ns)
    if not datasets:
        print(f"[red]No datasets found in namespace '{ns}'[/red]")
        raise typer.Exit(1)

    print(f"[green]Discovered {len(datasets)} datasets[/green]\n")

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

    # Determine output path
    if output_path:
        target = Path(output_path)
    else:
        pipeline_root = os.environ.get("PIPELINES_ROOT", None)
        if pipeline_root:
            target = Path(pipeline_root) / "apps" / app_name / "governance.yaml"
        else:
            target = Path("./governance.yaml")

    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w") as f:
        yaml.safe_dump(yaml_doc, f, sort_keys=False)

    print(f"\n[bold green]Generated governance.yaml → {target}[/bold green]")
