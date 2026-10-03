from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.lineage.code import PipelineLineage, DatasetRef
from celine.utils.pipelines.pipeline_prefect import (
    dbt_run_gold,
    dbt_run_silver,
    dbt_run_staging,
    dbt_run_tests,
    dbt_seed,
    meltano_run_import,
    meltano_run,
    dbt_run,
    dbt_run_operation,
)
from celine.utils.pipelines.pipeline_runner import PipelineRunner
from celine.utils.pipelines.pipeline_result import (
    PipelineTaskResult,
    PipelineStatus,
)

from celine.utils.pipelines.context import flow_hooks

import os

# Scheduling switch, not a security posture: pipelines in celine-pipelines
# `serve()` their flow on its cron when true and run it once otherwise (the
# chart sets PREFECT_MODE=prod). It relaxes nothing, so it keeps its unset ⇒ dev
# default; the security posture is CELINE_ENV (see PipelineConfig).
DEV_MODE = os.getenv("PREFECT_MODE", "dev").lower() == "dev"


__all__ = [
    "dbt_run_gold",
    "dbt_run_silver",
    "dbt_run_staging",
    "dbt_run_tests",
    "meltano_run_import",
    "meltano_run",
    "dbt_run",
    "dbt_run_operation",
    "dbt_seed",
    "flow_hooks",
    "PipelineConfig",
    "PipelineRunner",
    "PipelineTaskResult",
    "PipelineStatus",
    "PipelineLineage",
    "DatasetRef",
]
