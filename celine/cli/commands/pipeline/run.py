# celine/cli/commands/pipeline/run.py
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import os
from pathlib import Path
from typing import Any, Optional

import typer
from rich import print
from dotenv import load_dotenv

from celine.common.logger import get_logger
from celine.pipelines.pipeline_config import PipelineConfig
from celine.pipelines.pipeline_runner import PipelineRunner

logger = get_logger(__name__)

pipeline_run_app = typer.Typer(help="Execute complete or partial pipelines")


# =============================================================================
# Environment & project discovery
# =============================================================================


def _discover_pipelines_root() -> Path:
    """
    Discover PIPELINES_ROOT.
    Priority:
      1. env PIPELINES_ROOT
      2. CWD if it contains meltano/dbt/flows
      3. parent folders up to 3 levels
    """
    env_root = os.getenv("PIPELINES_ROOT")
    if env_root:
        root = Path(env_root).resolve()
        if root.exists():
            logger.debug(f"Using PIPELINES_ROOT from env: {root}")
            return root

    cwd = Path.cwd()
    for candidate in [cwd, cwd.parent, cwd.parent.parent]:
        if (candidate / "meltano").exists() or (candidate / "dbt").exists():
            logger.debug(f"Auto-discovered PIPELINES_ROOT at: {candidate}")
            return candidate.resolve()

    raise RuntimeError(
        "Cannot determine PIPELINES_ROOT. "
        "Set PIPELINES_ROOT or run inside a project folder."
    )


def _discover_app_name(root: Path) -> str:
    """
    Discover APP_NAME.
    Priority:
      1. env APP_NAME
      2. detect current directory name inside apps/<app>
    """
    env_app = os.getenv("APP_NAME")
    if env_app:
        logger.debug(f"Using APP_NAME from env: {env_app}")
        return env_app

    # detect based on directory name
    if root.name != "apps" and (root.parent / "apps").exists():
        # If we are inside apps/<app>
        if (root / "meltano").exists() or (root / "dbt").exists():
            logger.debug(f"Detected APP_NAME from folder: {root.name}")
            return root.name

    raise RuntimeError(
        "APP_NAME is not set and cannot be auto-discovered. "
        "Set APP_NAME or run inside apps/<app> directory."
    )


def _load_env_files(root: Path, app_name: str) -> None:
    """
    Load environment files in a priority order.
    Rule:
        For ROOT:
            load the first existing file in: .env, .env.local, .env.test
        Then for APP:
            load the first existing file in: apps/<app>/.env, .env.local, .env.test

    App environment ALWAYS overrides root environment.
    """
    candidates_global = [
        root / ".env",
        root / ".env.local",
    ]

    candidates_app = [
        root / "apps" / app_name / ".env",
        root / "apps" / app_name / ".env.local",
    ]

    def load_first_existing(candidates: list[Path], override: bool) -> Optional[Path]:
        for file in candidates:
            if file.exists():
                logger.debug(f"Loading env file: {file}")
                load_dotenv(file, override=override)
                return file
        logger.debug(f"No .env file found in set: {[str(p) for p in candidates]}")
        return None

    # 1) Load ROOT env (override=False)
    load_first_existing(candidates_global, override=False)

    # 2) Load APP env (override=True)
    load_first_existing(candidates_app, override=True)


def _discover_paths(root: Path, app_name: str) -> dict[str, Optional[str]]:
    """
    Infer MELTANO_PROJECT_ROOT, DBT_PROJECT_DIR, DBT_PROFILES_DIR, FLOWS_DIR.
    """
    app_root = root / "apps" / app_name

    meltano_root = os.getenv("MELTANO_PROJECT_ROOT")
    dbt_dir = os.getenv("DBT_PROJECT_DIR")
    dbt_prof = os.getenv("DBT_PROFILES_DIR")

    if not meltano_root:
        if (app_root / "meltano").exists():
            meltano_root = str(app_root / "meltano")

    if not dbt_dir:
        if (app_root / "dbt").exists():
            dbt_dir = str(app_root / "dbt")

    if not dbt_prof:
        dbt_prof = dbt_dir

    flows_dir = app_root / "flows"

    if not meltano_root:
        raise RuntimeError("Unable to infer MELTANO_PROJECT_ROOT")
    if not dbt_dir:
        raise RuntimeError("Unable to infer DBT_PROJECT_DIR")

    return {
        "meltano_root": meltano_root,
        "dbt_dir": dbt_dir,
        "dbt_profiles": dbt_prof,
        "flows_dir": str(flows_dir) if flows_dir.exists() else None,
    }


# =============================================================================
# Runner builder
# =============================================================================


def _build_runner() -> PipelineRunner:
    """
    Build PipelineRunner with inferred envs if missing.
    """
    try:
        root = _discover_pipelines_root()
        app_name = _discover_app_name(root)
        _load_env_files(root, app_name)
        paths = _discover_paths(root, app_name)

        # Export inferred paths so PipelineConfig picks them up
        os.environ.setdefault("APP_NAME", app_name)
        os.environ.setdefault("PIPELINES_ROOT", str(root))
        os.environ.setdefault("MELTANO_PROJECT_ROOT", paths["meltano_root"] or "")
        os.environ.setdefault("DBT_PROJECT_DIR", paths["dbt_dir"] or "")
        os.environ.setdefault("DBT_PROFILES_DIR", paths["dbt_profiles"] or "")

        cfg = PipelineConfig()  # fully env-driven
        return PipelineRunner(cfg)

    except Exception as e:
        logger.exception("Failed to build PipelineRunner")
        print(f"[red]Failed to build pipeline context:[/red] {e}")
        raise typer.Exit(1)


# =============================================================================
# Flow loader
# =============================================================================


def _load_flow_module(flow_name: str) -> Any:
    """
    Load module from <PIPELINES_ROOT>/apps/<APP>/flows/<flow_name>.py
    """
    try:
        root = Path(os.environ["PIPELINES_ROOT"]).resolve()
        app_name = os.environ["APP_NAME"]
    except KeyError:
        raise RuntimeError(
            "Pipeline context not initialized — call _build_runner() first"
        )

    flow_file = root / "apps" / app_name / "flows" / f"{flow_name}.py"

    if not flow_file.exists():
        raise FileNotFoundError(f"Flow '{flow_name}' not found at {flow_file}")

    spec = importlib.util.spec_from_file_location(flow_name, flow_file)
    if not spec or not spec.loader:
        raise ImportError(f"Cannot import flow module: {flow_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    return module


def _run_func(func: Any) -> Any:
    """Run sync or async function."""
    if inspect.iscoroutinefunction(func):
        return asyncio.run(func())
    return func()


# =============================================================================
# Meltano
# =============================================================================


@pipeline_run_app.command("meltano")
def run_meltano(
    command: str = typer.Argument(
        "run import", help="Meltano command, default 'run import'"
    )
):
    """Execute Meltano through PipelineRunner."""
    runner = _build_runner()
    try:
        res = runner.run_meltano(command)
        print(res)
        return res
    except Exception as e:
        logger.exception("Meltano run failed")
        print(f"[red]Meltano execution failed:[/red] {e}")
        raise typer.Exit(1)


# =============================================================================
# dbt
# =============================================================================


@pipeline_run_app.command("dbt")
def run_dbt(
    tag: str = typer.Argument(
        ..., help="dbt selector/tag (e.g. 'staging', 'silver', 'gold', 'test')"
    )
):
    """Execute dbt run/test through PipelineRunner."""
    runner = _build_runner()
    try:
        res = runner.run_dbt(tag)
        print(res)
        return res
    except Exception as e:
        logger.exception("dbt run failed")
        print(f"[red]dbt execution failed:[/red] {e}")
        raise typer.Exit(1)


# =============================================================================
# Prefect — Local flow execution
# =============================================================================


@pipeline_run_app.command("prefect")
def run_prefect(
    flow: str = typer.Option(..., "--flow", "-f", help="Name of flows/<flow>.py"),
    function: str = typer.Option(
        ..., "--function", "-x", help="Function inside the flow module"
    ),
):
    """
    Load flows/<flow>.py and execute the given function.
    Supports async and sync functions.

    Example:
        celine pipeline run prefect --flow sync_users --function main
    """
    _build_runner()  # sets envs + context

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
    print(f"[bold blue]Executing {flow}.{function}()[/bold blue]")

    try:
        result = _run_func(func)
        print("[green]Execution completed[/green]")
        print(result)
        return result
    except Exception as e:
        logger.exception("Flow function execution failed")
        print(f"[red]Flow execution failed:[/red] {e}")
        raise typer.Exit(1)
