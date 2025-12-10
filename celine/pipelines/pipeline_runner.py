import traceback
import re
import subprocess, os, datetime
from pathlib import Path
from typing import Any, Optional, Tuple
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
from celine.pipelines.pipeline_result import PipelineTaskResult, PipelineTaskStatus

from celine.pipelines.lineage.meltano import MeltanoLineage
from celine.pipelines.lineage.dbt import DbtLineage
from celine.pipelines.governance import GovernanceResolver

from celine.pipelines.utils import get_namespace
from celine.pipelines.const import (
    OPENLINEAGE_CLIENT_VERSION,
    PRODUCER,
    VERSION,
)

MELTANO_LINE_RE = re.compile(
    r"^\s*\d{4}-\d{2}-\d{2}T[\d:\.]+Z\s*\[(?P<level>[a-zA-Z\s]+)\]\s*(?P<msg>.*)$"
)
DBT_ERROR_HEADER = re.compile(
    r".*\b(Failure|Database Error)\b in model (?P<name>[\w_]+) \((?P<path>.+?)\)",
    re.IGNORECASE,
)


class PipelineRunner:
    """
    Orchestrates Meltano + dbt tasks for a given app pipeline,
    with logging, validation, and lineage integration.
    """

    _engine: Engine | None = None

    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.logger = get_logger("celine.pipeline." + (cfg.app_name or "Pipeline"))
        self.client = OpenLineageClient()

    # ---------- Helpers ----------
    def _project_path(self, suffix: str = "") -> Optional[str]:
        root = Path(os.environ.get("PIPELINES_ROOT", "./"))
        if self.cfg.app_name:
            return str(root / "apps" / self.cfg.app_name / suffix.lstrip("/"))
        return None

    def _task_result(
        self, status: bool | PipelineTaskStatus, command: str, details: Any = None
    ) -> PipelineTaskResult:

        result = PipelineTaskResult(
            command=command,
            status=(
                "success"
                if status is True
                else ("failed" if status is False else status)
            ),
            details=details or "",
        )

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

    def _parse_meltano_log(self, line: str) -> Tuple[str, str | None]:
        match = MELTANO_LINE_RE.match(line)
        if not match:
            return line, None

        level = match.group("level").strip().lower()
        msg = match.group("msg").strip()

        return msg, level

    # ---------- Meltano ----------
    def run_meltano(self, command: str = "run import") -> PipelineTaskResult:
        run_id = str(uuid4())
        base_command = command.replace("run ", "")
        job_name = f"{self.cfg.app_name}:meltano:{base_command}"
        self._emit_event(job_name, EventType.START, run_id)

        project_root = self._project_path("/meltano")

        self.logger.debug(f"run_meltano: dir {project_root}")

        full_command = f"meltano {command}"

        if not project_root:
            return self._task_result(
                False, full_command, "MELTANO_PROJECT_ROOT not set"
            )

        try:
            cmd = full_command.split()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=project_root,
                env={**os.environ, "NO_COLOR": "1"},
            )

            success = result.returncode == 0
            error_logs: list[str] = []

            self.logger.debug("")
            self.logger.debug(f"run_meltano: meltano {full_command}")

            def log_results(std_result):
                logger_fn = self.logger.debug if success else self.logger.error
                for line in (std_result or "").splitlines():
                    if line.strip():
                        msg, level = self._parse_meltano_log(line)
                        logger_fn(f"\t {msg}")
                        if not success and level in ["error", "warning"]:
                            error_logs.append(f"{level.upper()} {msg}")

            if result.stdout:
                log_results(result.stdout or "")
            if result.stderr:
                log_results(result.stderr or "")

            gov_resolver = GovernanceResolver.auto_discover(
                app_name=self.cfg.app_name,
                project_dir=project_root,
            )
            lineage = MeltanoLineage(
                self.cfg,
                project_root=project_root,
                governance_resolver=gov_resolver,
            )
            inputs, outputs = lineage.collect_inputs_outputs(base_command)

            if success:
                self._emit_event(
                    job_name, EventType.COMPLETE, run_id, inputs=inputs, outputs=outputs
                )
                return self._task_result(True, full_command)
            else:
                error_msg = "\n".join(error_logs)
                facets = self._get_error_facet(error_msg)
                self._emit_event(
                    job_name,
                    EventType.FAIL,
                    run_id,
                    inputs=inputs,
                    outputs=outputs,
                    run_facets=facets,
                )
                return self._task_result(False, full_command, error_msg)
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
            return self._task_result(False, full_command, str(e))

    def _extract_dbt_error_block(self, output: str) -> list[str]:
        """
        Given full dbt output (stdout/stderr), extract only the relevant
        error message block(s) to display in our logs.
        """
        lines = output.splitlines()
        extracted: list[str] = []
        in_error_block = False

        for line in lines:
            # Strip timestamp if present
            cleaned = self._strip_timestamp(line)

            # Start of error block
            if DBT_ERROR_HEADER.match(cleaned):
                in_error_block = True
                extracted.append(cleaned)
                continue

            # If inside an error block, include indented or meaningful lines
            if in_error_block:
                if cleaned.strip() == "":
                    # blank line ends error block
                    in_error_block = False
                    continue

                # stop if we reach compiled file path (too verbose)
                if cleaned.strip().startswith("compiled code at "):
                    continue

                extracted.append(cleaned)

        return extracted

    def _strip_timestamp(self, line: str) -> str:
        """
        Remove leading `HH:MM:SS` timestamps used in dbt logs.
        """
        return re.sub(r"^\s*\d{2}:\d{2}:\d{2}\s+", "", line)

    def run_dbt(self, tag: str, job_name: str | None = None) -> PipelineTaskResult:
        if not self.cfg.app_name:
            raise Exception(f"Missing app_name {self.cfg.app_name}")

        job_name = job_name or f"{self.cfg.app_name}:dbt:{tag}"
        project_dir = self.cfg.dbt_project_dir or self._project_path("/dbt")
        profiles_dir = self.cfg.dbt_profiles_dir or project_dir

        if not project_dir:
            return self._task_result(False, tag, "DBT_PROJECT_DIR not set")

        command = (
            ["dbt", "--no-use-colors", "run", "--select", tag]
            if tag != "test"
            else ["dbt", "test"]
        )

        return self._run_dbt(
            job_name=job_name,
            command=command,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            lineage_tag=tag,
            collect_lineage=True,
        )

    def run_dbt_operation(
        self, macro: str, args: dict | None = None, job_name: str | None = None
    ) -> PipelineTaskResult:

        if not self.cfg.app_name:
            raise Exception(f"Missing app_name {self.cfg.app_name}")

        job_name = job_name or f"{self.cfg.app_name}:dbt:run-operation:{macro}"

        project_dir = self.cfg.dbt_project_dir or self._project_path("/dbt")
        profiles_dir = self.cfg.dbt_profiles_dir or project_dir

        if not project_dir:
            return self._task_result(False, macro, "DBT_PROJECT_DIR not set")

        command = ["dbt", "run-operation", macro]

        if args:
            import json

            command.extend(["--args", json.dumps(args)])

        return self._run_dbt(
            job_name=job_name,
            command=command,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            lineage_tag=None,
            collect_lineage=False,
        )

    def _run_dbt(
        self,
        job_name: str,
        command: list[str],
        project_dir: str,
        profiles_dir: str | None,
        lineage_tag: str | None,
        collect_lineage: bool,
    ) -> PipelineTaskResult:
        """
        Core dbt execution function used by both run_dbt and run_dbt_operation.

        Parameters:
        job_name       - Full OpenLineage job name
        command        - Full dbt command array (dbt ... ...)
        project_dir    - dbt project directory
        profiles_dir   - dbt profiles directory
        lineage_tag    - The tag passed to run_dbt(), or None for operations
        collect_lineage - Whether to collect lineage inputs/outputs
        """

        run_id = str(uuid4())
        command_str = " ".join(command)

        # Emit START event
        self._emit_event(job_name, EventType.START, run_id)

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

            success = result.returncode == 0

            # Logging output
            logging_fn = self.logger.debug if success else self.logger.error

            self.logger.debug(f"\ndbt_run: {command_str}\n")

            for line in (result.stdout or "").splitlines():
                if line.strip():
                    logging_fn("\t" + line)
            for line in (result.stderr or "").splitlines():
                if line.strip():
                    logging_fn("\t" + line)

            # Collect lineage only for actual model runs
            inputs, outputs = ([], [])
            if collect_lineage and lineage_tag is not None:
                gov_resolver = GovernanceResolver.auto_discover(
                    app_name=self.cfg.app_name,
                    project_dir=project_dir,
                )
                lineage = DbtLineage(
                    project_dir,
                    str(self.cfg.app_name),
                    self._build_engine(),
                    governance_resolver=gov_resolver,
                )
                inputs, outputs = lineage.collect_inputs_outputs(lineage_tag)

            # Error extraction
            cli_output = self.clean_output(
                (result.stdout + "\n" + result.stderr).strip()
            )

            error_msg = ""
            if not success:
                error_msg = "\n".join(self._extract_dbt_error_block(cli_output))
                facets = self._get_error_facet(
                    f"Command {command_str} exited with code {result.returncode}",
                    error_msg,
                )
                self._emit_event(
                    job_name,
                    EventType.FAIL,
                    run_id,
                    inputs=inputs,
                    outputs=outputs,
                    run_facets=facets,
                )
            else:
                self._emit_event(
                    job_name,
                    EventType.COMPLETE,
                    run_id,
                    inputs=inputs,
                    outputs=outputs,
                )

            return self._task_result(
                status=success,
                command=command_str,
                details=error_msg if not success else None,
            )

        except Exception as e:
            # Unexpected failure → ABORT
            self.logger.exception("dbt execution error")
            self._emit_event(
                job_name,
                EventType.ABORT,
                run_id,
                run_facets=self._get_error_facet(e, traceback.format_exc()),
            )
            return self._task_result(False, command_str, str(e))

    def _get_error_facet(
        self, e: Exception | str, stack_trace: str | None = ""
    ) -> dict[str, RunFacet]:
        return {
            "errorMessage": ErrorMessageRunFacet(
                message=str(e),
                programmingLanguage="python",
                stackTrace=stack_trace if stack_trace else "",
            )
        }

    def clean_output(self, output: str) -> str:
        """Remove ANSI escape codes from dbt's stdout output and ensure proper newlines."""
        ansi_escape = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")
        cleaned_output = re.sub(ansi_escape, "", output)
        return cleaned_output.replace(
            "\r", ""
        )  # Optional, removes any carriage return chars
