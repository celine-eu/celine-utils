from typing import Dict, Any
from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.pipeline_runner import PipelineRunner


def _get_config(cfg: dict | PipelineConfig) -> PipelineConfig:
    return cfg if isinstance(cfg, PipelineConfig) else PipelineConfig(**cfg)


def meltano_run(command: str = "run import", cfg: dict | PipelineConfig = {}):
    """
    Prefect task wrapper for PipelineRunner.run_meltano.
    """
    runner = PipelineRunner(_get_config(cfg))
    return runner.run_meltano(command)


def dbt_run(tag: str, cfg: dict | PipelineConfig = {}):
    """
    Prefect task wrapper for PipelineRunner.run_dbt.
    """
    runner = PipelineRunner(_get_config(cfg))
    return runner.run_dbt(tag)


def dbt_run_operation(
    macro: str, args: Dict[Any, Any] = {}, cfg: dict | PipelineConfig = {}
):
    """
    Prefect task wrapper for PipelineRunner.run_dbt.
    """
    runner = PipelineRunner(_get_config(cfg))
    return runner.run_dbt_operation(macro, args)


def meltano_run_import(cfg: dict | PipelineConfig = {}):
    return meltano_run("run import", cfg)


def dbt_run_staging(cfg: dict | PipelineConfig = {}):
    return dbt_run("staging", cfg)


def dbt_run_silver(cfg: dict | PipelineConfig = {}):
    return dbt_run("silver", cfg)


def dbt_run_gold(cfg: dict | PipelineConfig = {}):
    return dbt_run("gold", cfg)


def dbt_run_tests(cfg: dict | PipelineConfig = {}):
    return dbt_run("test", cfg)
