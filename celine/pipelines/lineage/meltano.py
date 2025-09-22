import os, json, yaml, datetime
from uuid import uuid4
from typing import Any
from sqlalchemy import text
from openlineage.client.run import RunEvent, RunState, Job, Run
from .client import get_client
from .datasets import make_dataset
from celine.common.logger import get_logger
from celine.datasets.client import DatasetClient
from celine.pipelines.pipeline_config import PipelineConfig
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

logger = get_logger(__name__)


class MeltanoLineage:
    def __init__(
        self,
        cfg: PipelineConfig,
        config_path: str = "meltano.yml",
        run_dir: str = ".meltano/run",
    ):
        self.cfg = cfg
        self.config_path = config_path
        self.run_dir = run_dir
        self.client = get_client()
        self.ds_client = DatasetClient()
        self.config = self._load_meltano_config()

    # ---------- Helpers ----------
    def _load_meltano_config(self) -> dict[str, Any]:
        logger.debug(f"Loading Meltano config from {self.config_path}")
        try:
            with open(self.config_path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load Meltano config at {self.config_path}: {e}")
            raise

    def _get_run_metadata(self, job_name: str):
        """
        Fetch the latest Meltano run metadata from the meltano DB, if configured.

        Returns:
            Row or None if not available.
        """
        if not self.cfg.meltano_database_uri:
            logger.info(
                "Skipping Meltano run metadata lookup: no meltano_database_uri configured"
            )
            return None

        logger.debug(
            f"Fetching latest run metadata for job={job_name} using {self.cfg.meltano_database_uri}"
        )

        stmt = text(
            """
            SELECT state, payload, started_at, ended_at, run_id
            FROM public.run
            WHERE job_name=:job_name
            ORDER BY started_at DESC LIMIT 1
            """
        )

        try:
            engine = create_engine(self.cfg.meltano_database_uri, future=True)
            with engine.connect() as conn:
                row = conn.execute(stmt, {"job_name": job_name}).fetchone()

            if not row:
                logger.warning(f"No Meltano run found for job={job_name}")
            else:
                logger.debug(
                    f"Meltano run metadata retrieved for job={job_name}: {row}"
                )

            return row
        except Exception as e:
            logger.error(f"Error querying Meltano run metadata for job={job_name}: {e}")
            return None

    def _collect_inputs_outputs(self, job_name: str):
        tap_name = job_name.split(":")[1].split("-to-")[0]
        inputs, outputs = [], []

        # Inputs from tap properties JSON
        props_file = os.path.join(self.run_dir, tap_name, "tap.properties.json")
        if os.path.exists(props_file):
            logger.debug(f"Reading tap properties from {props_file}")
            try:
                with open(props_file) as f:
                    props = json.load(f)
                for s in props.get("streams", []):
                    schema_props = s.get("schema", {}).get("properties", {})
                    inputs.append(
                        make_dataset(
                            namespace=f"tap.{tap_name}",
                            name=s["tap_stream_id"],
                            schema=schema_props,
                        )
                    )
            except Exception as e:
                logger.warning(f"Failed to parse {props_file}: {e}")

        # Outputs from schema_mapping in meltano.yml
        loaders = self.config.get("plugins", {}).get("loaders", [])
        for loader in loaders:
            if loader["name"].startswith("target-postgres"):
                schema_map = loader.get("schema_mapping", {})
                for tap, mapping in schema_map.items():
                    if tap == tap_name:
                        for _, tgt_schema in mapping.items():
                            outputs.append(
                                make_dataset(
                                    namespace="target.postgres",
                                    name=f"{tgt_schema}.{tap_name}",
                                )
                            )
        logger.debug(
            f"Collected {len(inputs)} inputs and {len(outputs)} outputs for job={job_name}"
        )
        return inputs, outputs

    # ---------- Emit ----------
    def emit_run(self, job_name: str):
        run_uuid = str(uuid4())
        logger.info(f"Emitting OpenLineage events for job={job_name}, runId={run_uuid}")

        try:
            # Emit START
            self.client.emit(
                RunEvent(
                    eventType=RunState.START,
                    eventTime=datetime.now(timezone.utc).isoformat(),
                    run=Run(runId=run_uuid),
                    job=Job(
                        namespace=f"celine.meltano.{self.cfg.app_name}", name=job_name
                    ),
                    inputs=[],
                    outputs=[],
                    producer="celine-utils",
                )
            )
            logger.debug(f"START event emitted for job={job_name}, runId={run_uuid}")
        except Exception as e:
            logger.error(f"Failed to emit START event for job={job_name}: {e}")
            raise

        # Collect results
        row = self._get_run_metadata(job_name)
        if not row:
            logger.error(
                f"No run metadata found for job={job_name}, skipping COMPLETE/FAIL event"
            )
            return

        state, payload, _, _, _ = row
        inputs, outputs = self._collect_inputs_outputs(job_name)

        # Prepare facets
        facets = {}
        if state != "SUCCESS":
            msg = None
            try:
                msg = (
                    payload.get("error") if isinstance(payload, dict) else str(payload)
                )
            except Exception:
                msg = str(payload)
            facets["errorMessage"] = {"message": msg}
            logger.warning(f"Meltano job={job_name} failed with error: {msg}")

        # Emit COMPLETE/FAIL
        try:
            self.client.emit(
                RunEvent(
                    eventType=(
                        RunState.COMPLETE if state == "SUCCESS" else RunState.FAIL
                    ),
                    eventTime=datetime.now(timezone.utc).isoformat(),
                    run=(
                        Run(runId=run_uuid, facets=facets)
                        if facets
                        else Run(runId=run_uuid)
                    ),
                    job=Job(
                        namespace=f"celine.meltano.{self.cfg.app_name}", name=job_name
                    ),
                    inputs=inputs,
                    outputs=outputs,
                    producer="celine-utils",
                )
            )
            logger.info(f"{state} event emitted for job={job_name}, runId={run_uuid}")
        except Exception as e:
            logger.error(f"Failed to emit {state} event for job={job_name}: {e}")
            raise
