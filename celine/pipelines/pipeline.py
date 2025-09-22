from typing import Dict, Any
from celine.pipelines.pipeline_config import PipelineConfig
from celine.pipelines.pipeline_runner import PipelineRunner


def meltano_run(command: str = "run import", cfg: dict = {}) -> Dict[str, Any]:
    """
    Prefect task wrapper for PipelineRunner.run_meltano.
    """
    runner = PipelineRunner(PipelineConfig(**cfg))
    return runner.run_meltano(command)


def meltano_run_import(cfg: dict = {}) -> Dict[str, Any]:
    return meltano_run("run import", cfg)


def validate_raw_data_task(
    cfg: dict = {}, tables: list[str] | None = None
) -> Dict[str, Any]:
    """
    Prefect task wrapper for PipelineRunner.validate_raw_data.
    """
    runner = PipelineRunner(PipelineConfig(**cfg))
    return runner.validate_raw_data(tables)


def dbt_run(tag: str, cfg: dict = {}) -> Dict[str, Any]:
    """
    Prefect task wrapper for PipelineRunner.run_dbt.
    """
    runner = PipelineRunner(PipelineConfig(**cfg))
    return runner.run_dbt(tag)


def dbt_run_staging(cfg: dict = {}) -> Dict[str, Any]:
    return dbt_run("staging", cfg)


def dbt_run_silver(cfg: dict = {}) -> Dict[str, Any]:
    return dbt_run("silver", cfg)


def dbt_run_gold(cfg: dict = {}) -> Dict[str, Any]:
    return dbt_run("gold", cfg)
