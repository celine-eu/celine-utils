import subprocess, os, datetime
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunEvent, Run, Job, InputDataset, OutputDataset
from openlineage.client.generated.base import EventType, RunFacet, JobFacet

from openlineage.client.generated.error_message_run import ErrorMessageRunFacet
from openlineage.client.generated.nominal_time_run import NominalTimeRunFacet
from openlineage.client.generated.environment_variables_run import (
    EnvironmentVariablesRunFacet,
    EnvironmentVariable,
)
from openlineage.client.generated.processing_engine_run import ProcessingEngineRunFacet

from celine.common.logger import get_logger
from celine.pipelines.pipeline_config import PipelineConfig
from celine.pipelines.lineage.meltano import MeltanoLineage
from celine.pipelines.lineage.dbt import DbtLineage

from celine.pipelines.utils import get_namespace
from celine.pipelines.const import (
    OPENLINEAGE_CLIENT_VERSION,
    TASK_RESULT_SUCCESS,
    TASK_RESULT_FAILED,
    PRODUCER,
    VERSION,
)


class PipelineRunner:
    """
    Orchestrates Meltano + dbt tasks for a given app pipeline,
    with logging, validation, and lineage integration.
    """

    _engine: Engine | None = None

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

    def _default_run_facets(self) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        return {
            "nominalTime": NominalTimeRunFacet(
                nominalStartTime=now.isoformat(), nominalEndTime=None
            ),
            "environmentVariables": EnvironmentVariablesRunFacet(
                environmentVariables=[
                    EnvironmentVariable(k, v)
                    for k, v in os.environ.items()
                    if k in ["PIPELINES_ROOT", "DBT_PROFILES_DIR"]
                ]
            ),
            "processingEngine": ProcessingEngineRunFacet(
                name=PRODUCER,
                version=VERSION,
                openlineageAdapterVersion=OPENLINEAGE_CLIENT_VERSION,
            ),
        }

    def _emit_event(
        self,
        job_name: str,
        state: EventType,
        run_id: str,
        inputs: list[InputDataset] | None = None,
        outputs: list[OutputDataset] | None = None,
        run_facets: dict[str, RunFacet] | None = None,
        job_facets: dict[str, JobFacet] | None = None,
        namespace: str | None = None,
    ):

        if not namespace:
            namespace = get_namespace(self.cfg.app_name)

        try:
            facets = self._default_run_facets()
            if run_facets:
                facets.update(run_facets)

            event = RunEvent(
                eventTime=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                producer=PRODUCER,
                run=Run(runId=run_id, facets=facets),
                job=Job(
                    namespace=namespace,
                    name=job_name,
                    facets=job_facets or {},
                ),
                eventType=state,
                inputs=inputs or [],
                outputs=outputs or [],
            )
            self.client.emit(event)
            self.logger.debug(f"Emitted {state.value} for {job_name} ({run_id})")
        except Exception:
            self.logger.exception(f"Failed to emit {state.value} for {job_name}")

    def _build_engine(self) -> Engine:
        if self._engine:
            return self._engine

        url = (
            f"postgresql+psycopg2://{self.cfg.postgres_user}:"
            f"{self.cfg.postgres_password}@{self.cfg.postgres_host}:"
            f"{self.cfg.postgres_port}/{self.cfg.postgres_db}"
        )
        self._engine = create_engine(url, future=True)
        return self._engine

    # ---------- Meltano ----------
    def run_meltano(self, command: str = "run import") -> Dict[str, Any]:
        run_id = str(uuid4())
        base_command = command.replace("run ", "")
        job_name = f"{self.cfg.app_name}:meltano:{base_command}"
        self._emit_event(job_name, EventType.START, run_id)

        project_root = self._project_path("/meltano")
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

            lineage = MeltanoLineage(self.cfg, project_root=project_root)
            inputs, outputs = lineage.collect_inputs_outputs(base_command)

            if result.returncode == 0:
                self._emit_event(
                    job_name, EventType.COMPLETE, run_id, inputs=inputs, outputs=outputs
                )
                return self._task_result(True, command, result.stdout)
            else:
                facets = self._get_error_facet(result.stderr)
                self._emit_event(
                    job_name,
                    EventType.FAIL,
                    run_id,
                    inputs=inputs,
                    outputs=outputs,
                    run_facets=facets,
                )
                return self._task_result(False, command, result.stderr)
        except Exception as e:
            self._emit_event(
                job_name,
                EventType.ABORT,
                run_id,
                run_facets={
                    "errorMessage": ErrorMessageRunFacet(
                        message=str(e), programmingLanguage="python", stackTrace=f"{e}"
                    )
                },
            )
            self.logger.exception("run_meltano failed")
            return self._task_result(False, command, str(e))

    def run_dbt(self, tag: str, job_name: str | None = None) -> Dict[str, Any]:

        if not self.cfg.app_name:
            raise Exception(f"Missing app_name {self.cfg.app_name}")

        run_id = str(uuid4())
        job_name = job_name or f"{self.cfg.app_name}:dbt:{tag}"

        self.logger.debug(f"start dbt job {job_name}")
        self._emit_event(job_name, EventType.START, run_id)

        project_dir = self.cfg.dbt_project_dir or self._project_path("/dbt")
        profiles_dir = self.cfg.dbt_profiles_dir or project_dir

        if not project_dir:
            return self._task_result(False, tag, "DBT_PROJECT_DIR not set")

        dbt_cmd = "dbt"
        command = (
            [dbt_cmd, "run", "--select", tag] if tag != "test" else [dbt_cmd, "test"]
        )
        try:
            env = {
                **os.environ,
                "DBT_PROFILES_DIR": str(profiles_dir or ""),
                "OPENLINEAGE_DBT_JOB_NAME": job_name,
            }
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                cwd=project_dir,
                env=env,
            )

            lineage = DbtLineage(project_dir, self.cfg.app_name, self._build_engine())
            inputs, outputs = lineage.collect_inputs_outputs(tag)

            success = result.returncode == 0
            if success:
                self._emit_event(
                    job_name, EventType.COMPLETE, run_id, inputs=inputs, outputs=outputs
                )
            else:
                facets = self._get_error_facet(result.stderr)
                self._emit_event(
                    job_name,
                    EventType.FAIL,
                    run_id,
                    inputs=inputs,
                    outputs=outputs,
                    run_facets=facets,
                )

            return self._task_result(
                status=success,
                command=" ".join(command),
                details=(result.stdout + "\n" + result.stderr).strip(),
            )

        except Exception as e:
            self._emit_event(
                job_name,
                EventType.ABORT,
                run_id,
                run_facets=self._get_error_facet(e),
            )
            return self._task_result(False, " ".join(command), str(e))

    def _get_error_facet(self, e: Exception | str) -> dict[str, RunFacet]:
        return {
            "errorMessage": ErrorMessageRunFacet(
                message=str(e), programmingLanguage="python", stackTrace=f"{e}"
            )
        }
