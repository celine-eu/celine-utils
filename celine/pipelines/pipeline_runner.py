import subprocess, os, datetime
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Job, Run

from celine.common.logger import get_logger
from celine.pipelines.pipeline_config import PipelineConfig
from celine.pipelines.lineage.meltano import MeltanoLineage

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
        self.client = OpenLineageClient()

    # ---------- Helpers ----------
    def _project_path(self, suffix: str = "") -> Optional[str]:
        root = Path(os.environ.get("PIPELINES_ROOT", "./"))
        if self.cfg.app_name:
            return str(root / "apps" / self.cfg.app_name / suffix.lstrip("/"))
        return None

    def _task_result(
        self, status: bool | str, command: str, details: Any = None
    ) -> Dict[str, Any]:
        result = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "command": command,
            "status": (
                TASK_RESULT_SUCCESS
                if status is True
                else (TASK_RESULT_FAILED if status is False else status)
            ),
        }
        if details is not None:
            result["details"] = details
        return result

    def _emit_event(
        self, job_name, state, run_id, inputs=None, outputs=None, facets=None
    ):
        try:
            event = RunEvent(
                eventType=state,
                eventTime=datetime.datetime.utcnow().isoformat(),
                run=Run(runId=run_id, facets=facets or {}),
                job=Job(namespace=f"celine.{self.cfg.app_name}", name=job_name),
                inputs=inputs or [],
                outputs=outputs or [],
                producer="celine-utils",
            )
            self.client.emit(event)
            self.logger.debug(f"Emitted {state} for {job_name} ({run_id})")
        except Exception as e:
            self.logger.warning(f"Failed to emit {state} for {job_name}: {e}")

    def _build_engine(self) -> Engine:
        url = (
            f"postgresql+psycopg2://{self.cfg.postgres_user}:"
            f"{self.cfg.postgres_password}@{self.cfg.postgres_host}:"
            f"{self.cfg.postgres_port}/{self.cfg.postgres_db}"
        )
        return create_engine(url, future=True)

    # ---------- Meltano ----------
    def run_meltano(self, command: str = "run import") -> Dict[str, Any]:
        run_id = str(uuid4())
        job_name = f"meltano:{command}"
        self._emit_event(job_name, RunState.START, run_id)

        project_root = self.cfg.meltano_project_root or self._project_path("/meltano")
        if not project_root:
            return self._task_result(False, command, "MELTANO_PROJECT_ROOT not set")

        try:
            result = subprocess.run(
                f"meltano {command}",
                shell=True,
                capture_output=True,
                text=True,
                cwd=project_root,
            )
            lineage = MeltanoLineage(self.cfg)
            inputs, outputs = lineage._collect_inputs_outputs(
                command.replace("run ", "")
            )

            if result.returncode == 0:
                self._emit_event(
                    job_name, RunState.COMPLETE, run_id, inputs=inputs, outputs=outputs
                )
                return self._task_result(True, command, result.stdout)
            else:
                facets = {"errorMessage": {"message": result.stderr}}
                self._emit_event(
                    job_name,
                    RunState.FAIL,
                    run_id,
                    inputs=inputs,
                    outputs=outputs,
                    facets=facets,
                )
                return self._task_result(False, command, result.stderr)
        except Exception as e:
            self._emit_event(
                job_name,
                RunState.ABORT,
                run_id,
                facets={"errorMessage": {"message": str(e)}},
            )
            return self._task_result(False, command, str(e))

    # ---------- dbt ----------
    def run_dbt(self, tag: str, job_name: str | None = None) -> Dict[str, Any]:
        run_id = str(uuid4())
        job_name = job_name or f"dbt:{tag}"

        # Prefect/orchestration START event (dbt-ol will emit its own detailed events)
        self._emit_event(job_name, RunState.START, run_id)

        project_dir = self.cfg.dbt_project_dir or self._project_path("/dbt")
        profiles_dir = self.cfg.dbt_profiles_dir or project_dir
        if not project_dir:
            return self._task_result(False, tag, "DBT_PROJECT_DIR not set")

        command = (
            ["dbt-ol", "run", "--select", tag] if tag != "test" else ["dbt-ol", "test"]
        )
        try:
            env = {
                **os.environ,
                "DBT_PROFILES_DIR": str(profiles_dir or ""),
                "OPENLINEAGE_NAMESPACE": f"celine.{self.cfg.app_name}",
                "OPENLINEAGE_DBT_JOB_NAME": job_name,
            }
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                cwd=project_dir,
                env=env,
            )

            # Only use exit code to decide success/failure
            success = result.returncode == 0
            status = TASK_RESULT_SUCCESS if success else TASK_RESULT_FAILED

            # Don’t duplicate COMPLETE/FAIL events, dbt-ol already does it.
            if success:
                self.logger.info(f"dbt {tag} succeeded")
            else:
                self.logger.error(
                    f"dbt {tag} failed with exit code {result.returncode}"
                )

            return self._task_result(
                status=success,
                command=" ".join(command),
                details=(result.stdout + "\n" + result.stderr).strip(),
            )

        except Exception as e:
            # Only orchestration-level ABORT
            self._emit_event(
                job_name,
                RunState.ABORT,
                run_id,
                facets={"errorMessage": {"message": str(e)}},
            )
            return self._task_result(False, " ".join(command), str(e))
