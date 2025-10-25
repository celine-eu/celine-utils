from celine.pipelines.pipeline_config import PipelineConfig
from celine.pipelines.pipeline_prefect import dbt_run_gold, dbt_run_silver, dbt_run_staging,dbt_run_tests, meltano_run_import, meltano_run, dbt_run
from celine.pipelines.pipeline_runner import PipelineRunner
import os 

DEV_MODE = os.getenv("PREFECT_MODE", "dev").lower() == "dev"