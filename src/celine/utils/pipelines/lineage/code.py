# celine/pipelines/lineage/code.py
"""
Lineage tracking for pure-code (non-meltano / non-dbt) pipelines.

Usage::

    lineage = PipelineLineage(cfg, "my_app:code:my-flow")
    lineage.start()

    try:
        do_work()
    except Exception as e:
        lineage.fail(e)
        raise

    lineage.complete(
        inputs=[DatasetRef("datasets.ds_dev_silver.my_table")],
        outputs=[DatasetRef("datasets.ds_dev_gold.my_output")],
    )
"""

from __future__ import annotations

import datetime
import os
import traceback as tb
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent
from openlineage.client.generated.base import EventType, JobFacet, RunFacet
from openlineage.client.generated.schema_dataset import (
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from openlineage.client.generated.environment_variables_run import (
    EnvironmentVariable,
    EnvironmentVariablesRunFacet,
)
from openlineage.client.generated.error_message_run import ErrorMessageRunFacet
from openlineage.client.generated.nominal_time_run import NominalTimeRunFacet
from openlineage.client.generated.processing_engine_run import ProcessingEngineRunFacet

from celine.utils.common.logger import get_logger
from celine.utils.pipelines.const import (
    OPENLINEAGE_CLIENT_VERSION,
    PRODUCER,
    VERSION,
)
from celine.utils.pipelines.governance import GovernanceResolver
from celine.utils.pipelines.lineage.facets.governance import GovernanceDatasetFacet
from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.utils import get_namespace


@dataclass
class DatasetRef:
    """Lightweight descriptor for an input or output dataset.

    Args:
        name:      OpenLineage dataset name, e.g. ``"datasets.ds_dev_silver.meters_data"``.
        namespace: Overrides the tracker's default namespace when set.
        facets:    Optional extra dataset facets merged on top of the governance facet.
    """

    name: str
    namespace: str | None = None
    facets: dict[str, Any] | None = field(default=None)


class PipelineLineage:
    """Emit OpenLineage START / COMPLETE / FAIL events for pure-code pipelines.

    Each instance generates its own ``run_id`` so that task retries produce
    independent run records.  Governance metadata is loaded eagerly from
    ``governance.yaml`` (resolved via :func:`GovernanceResolver.auto_discover`)
    and attached as a ``GovernanceDatasetFacet`` to every declared dataset.

    When ``cfg.openlineage_enabled`` is ``False`` the object is fully silent —
    governance resolution still happens, but no events are emitted.

    Example::

        lineage = PipelineLineage(cfg, "my_app:code:my-flow",
                                  project_dir="/path/to/flows")
        lineage.start()
        try:
            do_work()
        except Exception as e:
            lineage.fail(e)
            raise
        lineage.complete(
            inputs=[DatasetRef("datasets.ds_dev_silver.meters_data")],
            outputs=[DatasetRef("datasets.ds_dev_gold.meters_energy_forecast")],
        )
    """

    def __init__(
        self,
        cfg: PipelineConfig,
        job_name: str,
        *,
        namespace: str | None = None,
        project_dir: str | None = None,
        engine: Engine | None = None,
    ) -> None:
        self.cfg = cfg
        self.job_name = job_name
        self._run_id = str(uuid4())
        self._namespace = namespace or get_namespace(cfg.app_name)
        self._started = False
        self.logger = get_logger("celine.pipeline.lineage." + (cfg.app_name or "code"))

        self.client: OpenLineageClient | None = (
            OpenLineageClient() if cfg.openlineage_enabled else None
        )
        if self.client is None:
            self.logger.debug("OpenLineage disabled — lineage events will not be emitted.")

        self.governance_resolver = GovernanceResolver.auto_discover(
            app_name=cfg.app_name,
            project_dir=project_dir,
        )

        self._engine: Engine | None = engine
        self._engine_built: bool = engine is not None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Emit an OpenLineage START event for this job."""
        if self._started:
            self.logger.warning(f"start() called more than once for {self.job_name}")
            return
        self._started = True
        self._emit_event(self.job_name, EventType.START, self._run_id)

    def complete(
        self,
        inputs: list[DatasetRef] | None = None,
        outputs: list[DatasetRef] | None = None,
    ) -> None:
        """Emit an OpenLineage COMPLETE event with dataset lineage and governance facets."""
        self._emit_event(
            self.job_name,
            EventType.COMPLETE,
            self._run_id,
            inputs=self._build_datasets(inputs or [], InputDataset),
            outputs=self._build_datasets(outputs or [], OutputDataset),
        )

    def fail(
        self,
        error: Exception | str,
        stack_trace: str = "",
    ) -> None:
        """Emit an OpenLineage FAIL event with an error facet."""
        if not stack_trace:
            stack_trace = tb.format_exc()
        self._emit_event(
            self.job_name,
            EventType.FAIL,
            self._run_id,
            run_facets=self._get_error_facet(error, stack_trace),
        )

    # ------------------------------------------------------------------
    # Internal helpers — identical to PipelineRunner counterparts
    # ------------------------------------------------------------------

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

    def _get_error_facet(
        self, e: Exception | str, stack_trace: str = ""
    ) -> dict[str, RunFacet]:
        return {
            "errorMessage": ErrorMessageRunFacet(
                message=str(e),
                programmingLanguage="python",
                stackTrace=stack_trace,
            )
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
    ) -> None:
        if self.client is None:
            return

        ns = namespace or self._namespace
        try:
            facets = self._default_run_facets()
            if run_facets:
                facets.update(run_facets)

            event = RunEvent(
                eventTime=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                producer=PRODUCER,
                run=Run(runId=run_id, facets=facets),
                job=Job(namespace=ns, name=job_name, facets=job_facets or {}),
                eventType=state,
                inputs=inputs or [],
                outputs=outputs or [],
            )
            self.client.emit(event)
            self.logger.debug(f"Emitted {state.value} for {job_name} ({run_id})")
        except Exception:
            self.logger.exception(f"Failed to emit {state.value} for {job_name}")

    # ------------------------------------------------------------------
    # Schema — mirrors DbtLineage._fetch_columns_from_db
    # ------------------------------------------------------------------

    def _get_engine(self) -> Engine | None:
        if self._engine_built:
            return self._engine
        self._engine_built = True  # attempt only once
        try:
            url = (
                f"postgresql+psycopg2://{self.cfg.postgres_user}:"
                f"{self.cfg.postgres_password}@{self.cfg.postgres_host}:"
                f"{self.cfg.postgres_port}/{self.cfg.postgres_db}"
            )
            self._engine = create_engine(url, future=True)
        except Exception as e:
            self.logger.warning(f"Could not build engine for schema introspection: {e}")
            self._engine = None
        return self._engine

    def _fetch_columns_from_db(
        self, schema: str, name: str
    ) -> list[SchemaDatasetFacetFields]:
        engine = self._get_engine()
        if not engine:
            return []
        try:
            with engine.connect() as conn:
                sql = text(
                    """
                    select column_name, data_type
                    from information_schema.columns
                    where table_schema = :schema
                      and table_name = :name
                    order by ordinal_position
                    """
                )
                rows = conn.execute(sql, {"schema": schema, "name": name}).fetchall()
            return [
                SchemaDatasetFacetFields(name=row[0], type=row[1], description=None)
                for row in rows
            ]
        except Exception as e:
            self.logger.warning(f"Failed to introspect {schema}.{name}: {e}")
            return []

    def _build_schema_facet(self, dataset_name: str) -> SchemaDatasetFacet | None:
        parts = dataset_name.split(".")
        if len(parts) < 2:
            return None
        schema, table = parts[-2], parts[-1]
        fields = self._fetch_columns_from_db(schema, table)
        if not fields:
            return None
        return SchemaDatasetFacet(fields=fields)

    # ------------------------------------------------------------------
    # Governance — identical to MeltanoLineage._build_governance_facet
    # ------------------------------------------------------------------

    def _build_governance_facet(
        self, dataset_name: str
    ) -> GovernanceDatasetFacet | None:
        rule = self.governance_resolver.resolve(dataset_name)
        owners = [o.name for o in rule.ownership] if rule.ownership else None
        tags = rule.tags or None

        if (
            not rule.license
            and not owners
            and not rule.access_level
            and not rule.access_requirements
            and not rule.classification
            and not tags
            and rule.retention_days is None
            and not rule.documentation_url
            and not rule.source_system
            and not rule.title
            and not rule.description
            and not rule.user_filter_column
        ):
            return None

        return GovernanceDatasetFacet(
            title=rule.title,
            description=rule.description,
            license=rule.license,
            attribution=rule.attribution,
            owners=owners,
            accessLevel=rule.access_level,
            accessRequirements=rule.access_requirements,
            classification=rule.classification,
            tags=tags,
            retentionDays=rule.retention_days,
            documentationUrl=rule.documentation_url,
            sourceSystem=rule.source_system,
            userFilterColumn=rule.user_filter_column,
        )

    def _build_datasets(
        self,
        refs: list[DatasetRef],
        cls: type,  # InputDataset or OutputDataset
    ) -> list:
        result = []
        for ref in refs:
            facets: dict[str, Any] = {}
            schema_facet = self._build_schema_facet(ref.name)
            if schema_facet:
                facets["schema"] = schema_facet
            gov = self._build_governance_facet(ref.name)
            if gov:
                facets["governance"] = gov
            if ref.facets:
                facets.update(ref.facets)
            result.append(
                cls(
                    namespace=ref.namespace or self._namespace,
                    name=ref.name,
                    facets=facets,
                )
            )
        return result
