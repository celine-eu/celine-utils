import datetime
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List, cast

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from logging import Logger
from dbt.cli.main import dbtRunnerResult

from celine.common.logger import get_logger
from celine.pipelines.pipeline_config import PipelineConfig


import os
from pathlib import Path
from typing import Optional

TASK_RESULT_SUCCESS = "success"
TASK_RESULT_FAILED = "failed"


def get_project_path(cfg: PipelineConfig, suffix: str = "") -> Optional[str]:
    """
    Resolve project path inside <PIPELINES_ROOT>/apps/<app>/<suffix>.

    Priority:
      1. cfg.app_name (from APP_NAME env var)
      2. Infer from file location (<PIPELINES_ROOT>/apps/<detected-project>)
    """
    root = os.environ.get("PIPELINES_ROOT", "./")  # default root is cwd

    if cfg.app_name:
        return str(Path(root) / "apps" / cfg.app_name / suffix.lstrip("/"))

    # try to detect project name from the file path
    parts = Path(__file__).resolve().parts
    try:
        idx = parts.index("apps")
        pid = parts[idx + 1]
        return str(Path(root) / "apps" / pid / suffix.lstrip("/"))
    except (ValueError, IndexError):
        return None


def get_task_result(
    status: bool | str = True,
    command: str | None = None,
    details: str | dict[str, Any] | None = None,
) -> dict[str, Any]:

    result: dict[str, Any] = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    if isinstance(status, bool):
        result["status"] = TASK_RESULT_SUCCESS if status else TASK_RESULT_FAILED

    if command is not None:
        result["command"] = command

    if details is not None:
        result["details"] = details

    return result


def build_engine(cfg: PipelineConfig) -> Engine:
    url = (
        f"postgresql+psycopg2://{cfg.postgres_user}:"
        f"{cfg.postgres_password}@{cfg.postgres_host}:"
        f"{cfg.postgres_port}/{cfg.postgres_db}"
    )
    return create_engine(url, future=True)


def run_meltano(
    cfg: PipelineConfig, command: str = "run import", logger: Optional[Logger] = None
) -> Dict[str, Any]:
    logger = logger or get_logger(cfg.app_name or "Pipeline")
    project_root = cfg.meltano_project_root or get_project_path(cfg, "/meltano")

    if not project_root:
        raise ValueError("MELTANO_PROJECT_ROOT not configured or resolvable")

    logger.info(f"Running Meltano command: meltano {command}")
    result: subprocess.CompletedProcess[str] = subprocess.run(
        f"meltano {command}",
        shell=True,
        capture_output=True,
        text=True,
        cwd=project_root,
    )

    if result.returncode != 0:
        logger.error(f"Meltano command failed:\n{result.stderr}")
        return get_task_result(status=False, command=command, details=result.stderr)

    return get_task_result(status=True, command=command)


def validate_raw_data(
    cfg: PipelineConfig,
    tables: Optional[List[str]] = None,
    logger: Optional[Logger] = None,
) -> Dict[str, Any]:
    logger = logger or get_logger(cfg.app_name or "Pipeline")
    tables = tables or ["current_weather_stream", "forecast_stream"]
    validation_results: Dict[str, Dict[str, Any]] = {}

    engine = build_engine(cfg)

    with engine.connect() as conn:
        for table in tables:
            query = text(
                f"""
                SELECT COUNT(*) AS total_records,
                       MAX(synced_at) AS latest_extraction,
                       COUNT(DISTINCT DATE(synced_at)) AS extraction_days
                FROM raw.{table}
                WHERE synced_at >= CURRENT_DATE - INTERVAL '7 days'
                """
            )
            result: Result = conn.execute(query)
            row = result.mappings().first()

            if row is None:
                logger.warning(f"Validation query for {table} yield no results.")
                continue

            validation_results[table] = {
                "total_records": row["total_records"],
                "latest_extraction": (
                    row["latest_extraction"].isoformat()
                    if row["latest_extraction"]
                    else None
                ),
                "extraction_days": row["extraction_days"],
            }
            logger.info(
                f"{table}: {row['total_records']} records "
                f"(latest: {validation_results[table]['latest_extraction']})"
            )

    if all(validation_results[t]["total_records"] > 0 for t in tables):
        logger.info("Validation passed")
        return get_task_result(status=True, details=validation_results)
    else:
        logger.warning("Validation failed")
        return get_task_result(status=False, details=validation_results)


def run_dbt(
    cfg: PipelineConfig, tag: str, logger: Optional[Logger] = None
) -> Dict[str, Any]:
    logger = logger or get_logger(cfg.app_name or "Pipeline")

    project_dir = cfg.dbt_project_dir or get_project_path(cfg, "/dbt")
    profiles_dir = cfg.dbt_profiles_dir or project_dir

    if not project_dir:
        raise ValueError("DBT_PROJECT_DIR not configured or resolvable")

    command: str = f"dbt run --select {tag}" if tag != "test" else "dbt test"
    logger.info(f"Running dbt: {command}")
    res_async = trigger_dbt_cli_command(
        command=command,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
    res = cast(dbtRunnerResult, res_async)

    status: str = (
        "success" if res and hasattr(res, "success") and res.success else "failed"
    )
    logger.info(f"dbt {tag} finished with status: {status}")
    return {
        "status": status,
        "tag": tag,
        "models_run": len(cast(list, res.result)) if res and res.result else 0,
    }
