# celine/cli/commands/pipeline/init.py
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import typer
from rich import print

from celine.common.logger import get_logger


pipeline_init_app = typer.Typer(help="Initialize a new pipeline application")
logger = get_logger("celine.cli.pipeline.init")

TEMPLATE_ROOT = Path(__file__).resolve().parent / "templates" / "pipelines"

PIPELINE_SIGNATURES = [
    "meltano",
    "dbt",
    "flows",
    ".env",
    "pipeline.yaml",
    "governance.yaml",
]


def stream_subprocess(
    cmd: list[str], cwd: Path | None = None, env: dict | None = None
) -> int:
    """
    Execute a subprocess and stream stdout/stderr in real time.
    Returns the exit code.
    """
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    # Print stdout as it arrives
    assert process.stdout is not None
    for line in process.stdout:
        print(line.rstrip())

    # Print stderr as it arrives
    assert process.stderr is not None
    for line in process.stderr:
        print(line.rstrip())

    process.wait()
    return process.returncode


def _copy_template(src: Path, dst: Path, context: dict[str, str]):
    """Very small template engine replacing {{ var }}."""
    content = src.read_text()
    for k, v in context.items():
        content = content.replace(f"{{{{ {k} }}}}", v)
    dst.write_text(content)


def _looks_like_pipeline_app(folder: Path) -> bool:
    """
    Heuristic: if the folder contains any of the standard CELINE pipeline
    directories or files, treat it as an existing pipeline app.
    """
    for signature in PIPELINE_SIGNATURES:
        if (folder / signature).exists():
            return True
    return False


@pipeline_init_app.command("app")
def init_app(
    app_name: str = typer.Argument(..., help="Name of the pipeline application"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite if exists"),
):
    """
    Create a new pipeline application folder containing:

      <app_name>/
        meltano/         # via meltano init
        dbt/             # via dbt init
        flows/pipeline.py
        .env
        README.md

    Aborts if the folder already looks like a CELINE pipeline app.
    """

    root = Path.cwd()
    app_root = root / app_name

    # Abort if folder exists and looks like an app
    if app_root.exists():
        if _looks_like_pipeline_app(app_root):
            if not force:
                print(
                    f"[red]❌ Folder '{app_name}' looks like an existing pipeline app. "
                    "Use --force to remove it.[/red]"
                )
                raise typer.Exit(1)

            # force allowed → delete and recreate
            print(f"[yellow]⚠ Removing existing app folder '{app_name}'[/yellow]")
            shutil.rmtree(app_root)

    # Create new folder
    app_root.mkdir(parents=True)

    print(f"[green]Creating pipeline application: {app_name}[/green]")

    # ----------------------------------------------------------------------
    # Template root
    # ----------------------------------------------------------------------
    # Use project-level templates: ./templates/pipelines

    if not TEMPLATE_ROOT.exists():
        print(f"[red]❌ Missing templates in {TEMPLATE_ROOT}[/red]")
        raise typer.Exit(1)

    # ----------------------------------------------------------------------
    # 1) Meltano project
    # ----------------------------------------------------------------------
    meltano_dir = app_root / "meltano"
    meltano_dir.mkdir()

    try:
        print("[blue]Initializing Meltano project...[/blue]")
        rc = stream_subprocess(
            ["meltano", "init", "."],
            cwd=meltano_dir,
            env={**os.environ, "NO_COLOR": "1"},
        )
        if rc == 0:
            print("  ✔ meltano init")
        else:
            print(
                f"[yellow]  ⚠ meltano init exited with code {rc} (continuing)[/yellow]"
            )
    except Exception as e:
        print(f"[yellow]  ⚠ meltano init failed: {e} (continuing)[/yellow]")

    # ----------------------------------------------------------------------
    # Meltano config from template
    # ----------------------------------------------------------------------
    meltano_templates = TEMPLATE_ROOT / "meltano"
    src_meltano = meltano_templates / "meltano.yml.j2"
    dst_meltano = meltano_dir / "meltano.yml"

    _copy_template(
        src_meltano,
        dst_meltano,
        {
            "app_name": app_name,
        },
    )

    print("  ✔ meltano.yml")

    # ----------------------------------------------------------------------
    # 2) dbt project
    # ----------------------------------------------------------------------
    dbt_dir = app_root / "dbt"

    print("[blue]Initializing dbt project...[/blue]")
    dbt_dir.mkdir()

    dbt_subfolders = [
        "models",
        "tests",
        "macros",
        "seeds",
        "snapshots",
        "analyses",
    ]
    for sub in dbt_subfolders:
        folder = dbt_dir / sub
        folder.mkdir()
        # add .gitkeep so empty folders persist in git
        (folder / ".gitkeep").write_text("")

    print("  ✔ dbt directory structure created")

    # ----------------------------------------------------------------------
    # 2b) Copy templated dbt_project.yml & profiles.yml
    # ----------------------------------------------------------------------
    dbt_templates = TEMPLATE_ROOT / "dbt"

    dbt_project_tpl = dbt_templates / "dbt_project.yml.j2"
    dst_dbt_project = dbt_dir / "dbt_project.yml"

    profiles_tpl = dbt_templates / "profiles.yml.j2"
    dst_profiles = dbt_dir / "profiles.yml"

    print("[blue]Creating templated dbt_project.yml and profiles.yml...[/blue]")

    _copy_template(
        dbt_project_tpl,
        dst_dbt_project,
        {
            "app_name": app_name,
        },
    )

    _copy_template(
        profiles_tpl,
        dst_profiles,
        {
            "app_name": app_name,
        },
    )

    print("  ✔ dbt_project.yml")
    print("  ✔ profiles.yml")

    # ----------------------------------------------------------------------
    # 3) flows/pipeline.py example
    # ----------------------------------------------------------------------
    flows_dir = app_root / "flows"
    flows_dir.mkdir()

    src_pipeline = TEMPLATE_ROOT / "flows" / "pipeline.py.j2"
    dst_pipeline = flows_dir / "pipeline.py"
    _copy_template(src_pipeline, dst_pipeline, {"app_name": app_name})
    print("  ✔ created flows/pipeline.py")

    # ----------------------------------------------------------------------
    # 4) .env file
    # ----------------------------------------------------------------------
    env_tpl = TEMPLATE_ROOT / ".env.j2"
    dst_env = app_root / ".env"
    _copy_template(
        env_tpl,
        dst_env,
        {
            "app_name": app_name,
            "postgres_host": os.getenv("POSTGRES_HOST", "localhost"),
            "postgres_user": os.getenv("POSTGRES_USER", "postgres"),
            "postgres_password": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "postgres_port": os.getenv("POSTGRES_PORT", "5432"),
            "postgres_db": os.getenv("POSTGRES_DB", app_name),
            "schema": os.getenv("DBT_SCHEMA", "public"),
        },
    )
    print("  ✔ created .env")

    # ----------------------------------------------------------------------
    # 5) README.md
    # ----------------------------------------------------------------------
    readme_tpl = TEMPLATE_ROOT / "README.md.j2"
    dst_readme = app_root / "README.md"
    _copy_template(readme_tpl, dst_readme, {"app_name": app_name})
    print("  ✔ created README.md")

    print(f"[green]🎉 Pipeline application '{app_name}' created successfully![/green]")
