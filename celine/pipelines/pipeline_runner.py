import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, List
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result

from celine.common.logger import get_logger
from celine.pipelines.pipeline_config import PipelineConfig
from celine.pipelines.lineage.meltano import MeltanoLineage

import os

TASK_RESULT_SUCCESS = "success"
TASK_RESULT_FAILED = "failed"


class PipelineRunner:
    """
    Orchestrates Meltano + dbt tasks for a given app pipeline,
    with logging, validation, and lineage integration.
    """

    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.logger = get_logger(cfg.app_name or "Pipeline")

    # ---------- Helpers ----------
    def _project_path(self, suffix: str = "") -> Optional[str]:
        root = Path(os.environ.get("PIPELINES_ROOT", "./"))  # default root is cwd

        if self.cfg.app_name:
            return str(root / "apps" / self.cfg.app_name / suffix.lstrip("/"))

        # try to detect project name from the file path
        parts = Path(__file__).resolve().parts
        try:
            idx = parts.index("apps")
            pid = parts[idx + 1]
            return str(root / "apps" / pid / suffix.lstrip("/"))
        except (ValueError, IndexError):
            return None

    def _task_result(
        self,
        status: bool | str = True,
        command: Optional[str] = None,
        details: Optional[Dict[str, Any] | str] = None,
    ) -> Dict[str, Any]:
        result: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if isinstance(status, bool):
            result["status"] = TASK_RESULT_SUCCESS if status else TASK_RESULT_FAILED
        else:
            result["status"] = status

        if command is not None:
            result["command"] = command
        if details is not None:
            result["details"] = details
        return result

    def _build_engine(self) -> Engine:
        try:
            url = (
                f"postgresql+psycopg2://{self.cfg.postgres_user}:"
                f"{self.cfg.postgres_password}@{self.cfg.postgres_host}:"
                f"{self.cfg.postgres_port}/{self.cfg.postgres_db}"
            )
            return create_engine(url, future=True)
        except Exception as e:
            self.logger.error(f"Failed to build SQLAlchemy engine: {e}")
            raise

    # ---------- Meltano ----------
    def run_meltano(self, command: str = "run import") -> Dict[str, Any]:
        project_root = self.cfg.meltano_project_root or self._project_path("/meltano")
        if not project_root:
            raise ValueError("MELTANO_PROJECT_ROOT not configured or resolvable")

        self.logger.info(f"Running Meltano command: meltano {command}")

        try:
            result: subprocess.CompletedProcess[str] = subprocess.run(
                f"meltano {command}",
                shell=True,
                capture_output=True,
                text=True,
                cwd=project_root,
            )
        except Exception as e:
            self.logger.error(f"Failed to run Meltano command: {e}")
            return self._task_result(status=False, command=command, details=str(e))

        if result.returncode != 0:
            self.logger.error(f"Meltano command failed:\n{result.stderr}")
            return self._task_result(
                status=False, command=command, details=result.stderr
            )

        self.logger.info(f"Meltano command succeeded: {command}")

        # Hook lineage
        try:
            lineage = MeltanoLineage(self.cfg)
            lineage.emit_run(job_name=command.replace("run ", ""))
        except Exception as e:
            self.logger.warning(f"Failed to emit Meltano lineage: {e}")

        return self._task_result(status=True, command=command)

    # ---------- Data Validation ----------
    def validate_raw_data(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        tables = tables or []
        validation_results: Dict[str, Dict[str, Any]] = {}

        try:
            engine = self._build_engine()
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
                        self.logger.warning(
                            f"Validation query for {table} yielded no results."
                        )
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
                    self.logger.info(
                        f"{table}: {row['total_records']} records "
                        f"(latest: {validation_results[table]['latest_extraction']})"
                    )
        except Exception as e:
            self.logger.error(f"Validation failed due to error: {e}")
            return self._task_result(status=False, details={"error": str(e)})

        if all(validation_results[t]["total_records"] > 0 for t in validation_results):
            self.logger.info("Validation passed")
            return self._task_result(status=True, details=validation_results)
        else:
            self.logger.warning("Validation failed: one or more tables empty")
            return self._task_result(status=False, details=validation_results)

    # ---------- dbt ----------
    def run_dbt(self, tag: str) -> Dict[str, Any]:
        project_dir = self.cfg.dbt_project_dir or self._project_path("/dbt")
        profiles_dir = self.cfg.dbt_profiles_dir or project_dir

        if not project_dir:
            raise ValueError("DBT_PROJECT_DIR not configured or resolvable")

        # use dbtol wrapper binary instead of dbt
        command = (
            ["dbt-ol", "run", "--select", tag] if tag != "test" else ["dbt-ol", "test"]
        )
        self.logger.info(f"Running dbt via OpenLineage wrapper: {' '.join(command)}")

        try:
            env = {
                **os.environ,
                "DBT_PROFILES_DIR": str(profiles_dir or ""),
                "OPENLINEAGE_NAMESPACE": f"celine.dbt.{self.cfg.app_name}",
            }

            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                cwd=project_dir,
                env=env,
            )
        except Exception as e:
            self.logger.error(f"dbtol execution failed: {e}")
            return self._task_result(
                status=False, command=" ".join(command), details=str(e)
            )

        if result.returncode != 0:
            self.logger.error(f"dbtol command failed:\n{result.stderr}")
            return self._task_result(
                status=False, command=" ".join(command), details=result.stderr
            )

        self.logger.info(f"dbt {tag} finished with success via dbtol")
        return self._task_result(
            status=True, command=" ".join(command), details=result.stdout
        )
