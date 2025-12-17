# celine/cli/commands/pipeline/init.py
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import typer

from celine.utils.common.logger import get_logger

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

    assert process.stdout is not None
    for line in process.stdout:
        typer.echo(line.rstrip())

    assert process.stderr is not None
    for line in process.stderr:
        typer.echo(line.rstrip())

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
        meltano/
        dbt/
        flows/pipeline.py
        .env
        README.md

    Aborts if the folder already looks like a CELINE pipeline app.
    """

    root = Path.cwd()
    app_root = root / app_name

    if app_root.exists():
        if _looks_like_pipeline_app(app_root):
            if not force:
                typer.echo(
                    f"Folder '{app_name}' looks like an existing pipeline app. "
                    "Use --force to remove it."
                )
                raise typer.Exit(1)

            typer.echo(f"Removing existing app folder '{app_name}'")
            shutil.rmtree(app_root)

    app_root.mkdir(parents=True)

    typer.echo(f"Creating pipeline application: {app_name}")

    if not TEMPLATE_ROOT.exists():
        typer.echo(f"Missing templates in {TEMPLATE_ROOT}")
        raise typer.Exit(1)

    # ------------------------------------------------------------------
    # 1) Meltano project
    # ------------------------------------------------------------------
    meltano_dir = app_root / "meltano"
    meltano_dir.mkdir()

    try:
        typer.echo("Initializing Meltano project...")
        rc = stream_subprocess(
            ["meltano", "init", "."],
            cwd=meltano_dir,
            env={**os.environ, "NO_COLOR": "1"},
        )
        if rc == 0:
            typer.echo("  meltano init completed")
        else:
            typer.echo(f"  meltano init exited with code {rc} (continuing)")
    except Exception as e:
        typer.echo(f"  meltano init failed: {e} (continuing)")

    meltano_templates = TEMPLATE_ROOT / "meltano"
    src_meltano = meltano_templates / "meltano.yml.j2"
    dst_meltano = meltano_dir / "meltano.yml"

    _copy_template(src_meltano, dst_meltano, {"app_name": app_name})
    typer.echo("  created meltano.yml")

    # ------------------------------------------------------------------
    # 2) dbt project
    # ------------------------------------------------------------------
    dbt_dir = app_root / "dbt"
    typer.echo("Initializing dbt project...")
    dbt_dir.mkdir()

    for sub in ["models", "tests", "macros", "seeds", "snapshots", "analyses"]:
        folder = dbt_dir / sub
        folder.mkdir()
        (folder / ".gitkeep").write_text("")

    typer.echo("  dbt directory structure created")

    dbt_templates = TEMPLATE_ROOT / "dbt"
    _copy_template(
        dbt_templates / "dbt_project.yml.j2",
        dbt_dir / "dbt_project.yml",
        {"app_name": app_name},
    )
    _copy_template(
        dbt_templates / "profiles.yml.j2",
        dbt_dir / "profiles.yml",
        {"app_name": app_name},
    )

    typer.echo("  created dbt_project.yml")
    typer.echo("  created profiles.yml")

    # ------------------------------------------------------------------
    # 3) flows/pipeline.py
    # ------------------------------------------------------------------
    flows_dir = app_root / "flows"
    flows_dir.mkdir()

    _copy_template(
        TEMPLATE_ROOT / "flows" / "pipeline.py.j2",
        flows_dir / "pipeline.py",
        {"app_name": app_name},
    )
    typer.echo("  created flows/pipeline.py")

    # ------------------------------------------------------------------
    # 4) .env
    # ------------------------------------------------------------------
    _copy_template(
        TEMPLATE_ROOT / ".env.j2",
        app_root / ".env",
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
    typer.echo("  created .env")

    # ------------------------------------------------------------------
    # 5) README
    # ------------------------------------------------------------------
    _copy_template(
        TEMPLATE_ROOT / "README.md.j2",
        app_root / "README.md",
        {"app_name": app_name},
    )
    typer.echo("  created README.md")

    typer.echo(f"Pipeline application '{app_name}' created successfully")
