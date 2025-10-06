from typing import Dict, Any
from celine.pipelines.pipeline_config import PipelineConfig
from celine.pipelines.pipeline_runner import PipelineRunner


def _get_config(cfg: dict | PipelineConfig) -> PipelineConfig:
    return cfg if isinstance(cfg, PipelineConfig) else PipelineConfig(**cfg)


def meltano_run(
    command: str = "run import", cfg: dict | PipelineConfig = {}
) -> Dict[str, Any]:
    """
    Prefect task wrapper for PipelineRunner.run_meltano.
    """
    runner = PipelineRunner(_get_config(cfg))
    return runner.run_meltano(command)


def dbt_run(tag: str, cfg: dict | PipelineConfig = {}) -> Dict[str, Any]:
    """
    Prefect task wrapper for PipelineRunner.run_dbt.
    """
    runner = PipelineRunner(_get_config(cfg))
    return runner.run_dbt(tag)


def meltano_run_import(cfg: dict | PipelineConfig = {}) -> Dict[str, Any]:
    return meltano_run("run import", cfg)


def dbt_run_staging(cfg: dict | PipelineConfig = {}) -> Dict[str, Any]:
    return dbt_run("staging", cfg)


def dbt_run_silver(cfg: dict | PipelineConfig = {}) -> Dict[str, Any]:
    return dbt_run("silver", cfg)


def dbt_run_gold(cfg: dict | PipelineConfig = {}) -> Dict[str, Any]:
    return dbt_run("gold", cfg)


def dbt_run_tests(cfg: dict | PipelineConfig = {}) -> Dict[str, Any]:
    return dbt_run("test", cfg)
