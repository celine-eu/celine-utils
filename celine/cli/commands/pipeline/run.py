# celine/cli/commands/pipeline/run.py
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import os
from pathlib import Path
from typing import Any

import typer
from dotenv import load_dotenv
from rich import print

from celine.common.logger import get_logger
from celine.pipelines.pipeline_config import PipelineConfig
from celine.pipelines.pipeline_runner import PipelineRunner

logger = get_logger(__name__)

pipeline_run_app = typer.Typer(help="Execute complete or partial pipelines")


# =============================================================================
#  Discovery helpers
# =============================================================================


def _discover_app_root() -> Path:
    """
    Discover the root folder of the CELINE app.
    A valid app folder contains at least one of:
        meltano/, dbt/, flows/

    Walk upward from CWD until found.
    """
    cwd = Path.cwd().resolve()

    def is_app_folder(p: Path) -> bool:
        return any((p / sub).exists() for sub in ("meltano", "dbt", "flows"))

    # CWD is an app folder
    if is_app_folder(cwd):
        logger.debug(f"Discovered app root at CWD: {cwd}")
        return cwd

    # Walk upward
    for parent in cwd.parents:
        if is_app_folder(parent):
            logger.debug(f"Discovered app root at parent: {parent}")
            return parent

    raise RuntimeError(
        "Unable to determine app folder.\n"
        "No meltano/, dbt/, or flows/ found in current or parent directories.\n"
        "Run inside a CELINE application folder."
    )


def _discover_app_name(app_root: Path) -> str:
    """
    Determine APP_NAME:
      1. Use env APP_NAME if set
      2. Otherwise use name of app_root folder
    """
    env_app = os.getenv("APP_NAME")
    if env_app:
        logger.debug(f"Using APP_NAME from environment: {env_app}")
        return env_app

    logger.debug(f"Inferring APP_NAME from folder name: {app_root.name}")
    return app_root.name


def _discover_pipelines_root(app_root: Path, app_name: str) -> Path:
    """
    PIPELINES_ROOT is optional.
    If env PIPELINES_ROOT exists, use it.

    Otherwise:
      - If monorepo layout detected (…/apps/<app>), pipelines root = parent.parent
      - Else standalone app → pipelines_root = app_root
    """
    env_root = os.getenv("PIPELINES_ROOT")
    if env_root:
        pr = Path(env_root).resolve()
        logger.debug(f"Using PIPELINES_ROOT from env: {pr}")
        return pr

    # monorepo style apps/<app>
    if app_root.parent.name == "apps" and app_root.parent.parent.exists():
        root = app_root.parent.parent
        logger.debug(f"Detected monorepo pipelines root: {root}")
        return root

    logger.debug("PIPELINES_ROOT not set; using app root as project root")
    return app_root


def _load_env_files(pipelines_root: Path, app_root: Path, app_name: str) -> None:
    """
    Environment resolution rules:

    If PIPELINES_ROOT was set explicitly:
        1. Load first existing file from pipelines root:
              .env, .env.local
        2. Load first existing file from pipelines root/apps/<app>/
              .env, .env.local

    If PIPELINES_ROOT was NOT set:
        Load first existing file from APP_ROOT:
              .env, .env.local
    """

    env_files = [".env", ".env.local"]

    def load_first(candidates: list[Path], override: bool):
        for file in candidates:
            if file.exists():
                logger.debug(f"Loading environment file: {file}")
                load_dotenv(file, override=override)
                return

    pipelines_root_from_env = os.getenv("PIPELINES_ROOT") is not None

    # Global env only applies if pipelines_root was explicitly given
    if pipelines_root_from_env:
        root_candidates = [pipelines_root / f for f in env_files]
        load_first(root_candidates, override=False)

        app_candidates = [pipelines_root / "apps" / app_name / f for f in env_files]
        load_first(app_candidates, override=True)

    else:
        # standalone / CWD-only mode
        app_candidates = [app_root / f for f in env_files]
        load_first(app_candidates, override=True)


# =============================================================================
#  Runner factory
# =============================================================================


def _build_runner() -> PipelineRunner:
    """
    Build PipelineRunner with automatic discovery of:
      - app root
      - app name
      - pipelines root (optional)
      - environment files
      - dynamic env vars for PipelineConfig
    """
    try:
        app_root = _discover_app_root()
        app_name = _discover_app_name(app_root)
        pipelines_root = _discover_pipelines_root(app_root, app_name)

        # Export discovered values for PipelineConfig
        os.environ.setdefault("APP_NAME", app_name)
        os.environ.setdefault("PIPELINES_ROOT", str(pipelines_root))

        # Try to auto-assign project paths if not set
        meltano_path = app_root / "meltano"
        dbt_path = app_root / "dbt"

        if (not os.getenv("MELTANO_PROJECT_ROOT")) and meltano_path.exists():
            os.environ["MELTANO_PROJECT_ROOT"] = str(meltano_path)

        if (not os.getenv("DBT_PROJECT_DIR")) and dbt_path.exists():
            os.environ["DBT_PROJECT_DIR"] = str(dbt_path)

        if not os.getenv("DBT_PROFILES_DIR"):
            # Usually same as dbt project
            os.environ["DBT_PROFILES_DIR"] = str(dbt_path)

        # Load .env files
        _load_env_files(pipelines_root, app_root, app_name)

        cfg = PipelineConfig()
        return PipelineRunner(cfg)

    except Exception as e:
        logger.exception("Failed to build pipeline context")
        print(f"[red]Failed to build pipeline context:[/red] {e}")
        raise typer.Exit(1)


# =============================================================================
#  Flow importer
# =============================================================================


def _load_flow_module(flow_name: str) -> Any:
    """
    Load module from <app_root>/flows/<flow_name>.py
    """
    app_root = _discover_app_root()
    flow_file = app_root / "flows" / f"{flow_name}.py"

    if not flow_file.exists():
        raise FileNotFoundError(f"Flow '{flow_name}' not found at {flow_file}")

    spec = importlib.util.spec_from_file_location(flow_name, flow_file)
    if not spec or not spec.loader:
        raise ImportError(f"Cannot import flow module: {flow_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    return module


def _run_func(func: Any) -> Any:
    """Execute sync or async functions."""
    if inspect.iscoroutinefunction(func):
        return asyncio.run(func())
    return func()


# =============================================================================
#  CLI Commands
# =============================================================================


@pipeline_run_app.command("meltano")
def run_meltano(
    command: str = typer.Argument(
        "run import", help="Meltano command (default: run import)"
    )
):
    """Run Meltano via PipelineRunner."""
    runner = _build_runner()
    try:
        res = runner.run_meltano(command)
        if res.status == "failed":
            raise typer.Exit(1)
        return res
    except Exception as e:
        logger.exception("Meltano run failed")
        print(f"[red]Meltano execution failed:[/red] {e}")
        raise typer.Exit(1)


@pipeline_run_app.command("dbt")
def run_dbt(
    tag: str = typer.Argument(
        ..., help="dbt selector/tag (e.g. staging, silver, gold, test)"
    )
):
    """Run dbt via PipelineRunner."""
    runner = _build_runner()
    try:
        res = runner.run_dbt(tag)
        if res.status == "failed":
            raise typer.Exit(1)
        return res
    except Exception as e:
        logger.exception("dbt run failed")
        print(f"[red]dbt execution failed:[/red] {e}")
        raise typer.Exit(1)


@pipeline_run_app.command("prefect")
def run_prefect(
    flow: str = typer.Option(..., "--flow", "-f", help="Name of flows/<flow>.py"),
    function: str = typer.Option(
        ..., "--function", "-x", help="Function inside the flow module"
    ),
):
    """
    Run a local Prefect flow implementation (pure Python execution).

    Example:
        celine pipeline run prefect --flow sync_users --function main
    """
    _build_runner()

    try:
        module = _load_flow_module(flow)
    except Exception as e:
        logger.exception("Flow loading failed")
        print(f"[red]Failed loading flow:[/red] {e}")
        raise typer.Exit(1)

    if not hasattr(module, function):
        print(f"[red]Function '{function}' not found in flow '{flow}'.[/red]")
        raise typer.Exit(1)

    func = getattr(module, function)

    print(f"[bold blue]Executing flow function:[/bold blue] {flow}.{function}()")

    try:
        result = _run_func(func)
        print("[green]Execution completed[/green]")
        print(result)
        return result
    except Exception as e:
        logger.exception("Flow function execution failed")
        print(f"[red]Flow execution failed:[/red] {e}")
        raise typer.Exit(1)
