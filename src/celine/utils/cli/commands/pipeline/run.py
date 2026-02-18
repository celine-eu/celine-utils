# celine/cli/commands/pipeline/run.py
from __future__ import annotations

import ast
import asyncio
import importlib.util
import inspect
import os
from pathlib import Path
from typing import Any

import typer
from dotenv import load_dotenv

from celine.utils.common.logger import get_logger
from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.pipeline_runner import PipelineRunner

logger = get_logger(__name__)

pipeline_run_app = typer.Typer(help="Execute complete or partial pipelines")


# =============================================================================
# Discovery helpers
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

    if is_app_folder(cwd):
        logger.debug(f"Discovered app root at CWD: {cwd}")
        return cwd

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
    env_app = os.getenv("APP_NAME")
    if env_app:
        logger.debug(f"Using APP_NAME from environment: {env_app}")
        return env_app

    logger.debug(f"Inferring APP_NAME from folder name: {app_root.name}")
    return app_root.name


def _discover_pipelines_root(app_root: Path, app_name: str) -> Path:
    env_root = os.getenv("PIPELINES_ROOT")
    if env_root:
        pr = Path(env_root).resolve()
        logger.debug(f"Using PIPELINES_ROOT from env: {pr}")
        return pr

    if app_root.parent.name == "apps" and app_root.parent.parent.exists():
        root = app_root.parent.parent
        logger.debug(f"Detected monorepo pipelines root: {root}")
        return root

    logger.debug("PIPELINES_ROOT not set; using app root as project root")
    return app_root


def _load_env_files(pipelines_root: Path, app_root: Path, app_name: str) -> None:
    env_files = [".env", ".env.local"]

    def load_first(candidates: list[Path], override: bool):
        for file in candidates:
            if file.exists():
                logger.debug(f"Loading environment file: {file}")
                load_dotenv(file, override=override)
                return

    pipelines_root_from_env = os.getenv("PIPELINES_ROOT") is not None

    if pipelines_root_from_env:
        load_first([pipelines_root / f for f in env_files], override=False)
        load_first(
            [pipelines_root / "apps" / app_name / f for f in env_files],
            override=True,
        )
    else:
        load_first([app_root / f for f in env_files], override=True)


def _find_flow_function(flow_path: Path) -> str | None:
    """
    Scan a Python file for a function decorated with @flow.

    Uses AST parsing to find the first function with a @flow decorator.
    Returns the function name, or None if not found.
    """
    try:
        source = flow_path.read_text()
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    # Handle @flow
                    if isinstance(decorator, ast.Name) and decorator.id == "flow":
                        return node.name
                    # Handle @flow(...)
                    if isinstance(decorator, ast.Call):
                        if (
                            isinstance(decorator.func, ast.Name)
                            and decorator.func.id == "flow"
                        ):
                            return node.name
        return None
    except Exception as e:
        logger.debug(f"Failed to parse {flow_path} for @flow decorator: {e}")
        return None


# =============================================================================
# Runner factory
# =============================================================================


def _build_runner() -> PipelineRunner:
    try:
        app_root = _discover_app_root()
        app_name = _discover_app_name(app_root)
        pipelines_root = _discover_pipelines_root(app_root, app_name)

        os.environ.setdefault("APP_NAME", app_name)
        os.environ.setdefault("PIPELINES_ROOT", str(pipelines_root))

        meltano_path = app_root / "meltano"
        dbt_path = app_root / "dbt"

        if not os.getenv("MELTANO_PROJECT_ROOT") and meltano_path.exists():
            os.environ["MELTANO_PROJECT_ROOT"] = str(meltano_path)

        if not os.getenv("DBT_PROJECT_DIR") and dbt_path.exists():
            os.environ["DBT_PROJECT_DIR"] = str(dbt_path)

        if not os.getenv("DBT_PROFILES_DIR"):
            os.environ["DBT_PROFILES_DIR"] = str(dbt_path)

        _load_env_files(pipelines_root, app_root, app_name)

        cfg = PipelineConfig()

        # propagate envs
        if not os.getenv("OPENLINEAGE_URL"):
            os.environ["OPENLINEAGE_URL"] = cfg.openlineage_url

        return PipelineRunner(cfg)

    except Exception as e:
        logger.exception("Failed to build pipeline context")
        typer.echo(f"Failed to build pipeline context: {e}")
        raise typer.Exit(1)


# =============================================================================
# Flow importer
# =============================================================================


def _load_flow_module(flow_name: str) -> Any:
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
    if inspect.iscoroutinefunction(func):
        return asyncio.run(func())
    return func()


# =============================================================================
# CLI Commands
# =============================================================================


@pipeline_run_app.command("meltano")
def run_meltano(
    command: str = typer.Argument(
        "run import", help="Meltano command (default: run import)"
    )
):
    runner = _build_runner()
    try:
        res = runner.run_meltano(command)
        if res.status == "failed":
            raise typer.Exit(1)
        return res
    except Exception as e:
        logger.exception("Meltano run failed")
        typer.echo(f"Meltano execution failed: {e}")
        raise typer.Exit(1)


@pipeline_run_app.command("dbt")
def run_dbt(
    tag: str = typer.Argument(
        ..., help="dbt selector/tag (e.g. staging, silver, gold, test)"
    )
):
    runner = _build_runner()
    try:
        res = runner.run_dbt(tag)
        if res.status == "failed":
            raise typer.Exit(1)
        return res
    except Exception as e:
        logger.exception("dbt run failed")
        typer.echo(f"dbt execution failed: {e}")
        raise typer.Exit(1)


@pipeline_run_app.command("prefect")
def run_prefect(
    flow: str = typer.Option(
        "pipeline", "--flow", "-f", help="Name of flows/<flow>.py"
    ),
    function: str = typer.Option(
        None,
        "--function",
        "-x",
        help="Function inside the flow module (auto-detected if omitted)",
    ),
):
    _build_runner()

    try:
        module = _load_flow_module(flow)
    except Exception as e:
        logger.exception("Flow loading failed")
        typer.echo(f"Failed loading flow: {e}")
        raise typer.Exit(1)

    # Auto-detect function if not provided
    if function is None:
        app_root = _discover_app_root()
        flow_file = app_root / "flows" / f"{flow}.py"
        function = _find_flow_function(flow_file)

        if function is None:
            typer.echo(
                f"No @flow decorated function found in '{flow}.py'. Use --function to specify."
            )
            raise typer.Exit(1)

        logger.debug(f"Auto-detected @flow function: {function}")

    if not hasattr(module, function):
        typer.echo(f"Function '{function}' not found in flow '{flow}'.")
        raise typer.Exit(1)

    func = getattr(module, function)

    typer.echo(f"Executing flow function: {flow}.{function}()")

    try:
        result = _run_func(func)
        typer.echo("Execution completed")
        typer.echo(str(result))
        return result
    except Exception as e:
        logger.exception("Flow function execution failed")
        typer.echo(f"Flow execution failed: {e}")
        raise typer.Exit(1)
