from prefect import flow
from prefect import task

from celine.pipelines.pipeline import (
    meltano_run_import,
    dbt_run_staging,
    dbt_run_silver,
    dbt_run_gold,
)


@task
def meltano_import(cfg):
    return meltano_run_import(cfg)


@task
def dbt_staging(cfg):
    return dbt_run_staging(cfg)


@task
def dbt_silver(cfg):
    return dbt_run_silver(cfg)


@task
def dbt_gold(cfg):
    return dbt_run_gold(cfg)


@flow
def medallion_flow(cfg: dict):
    # Bronze layer (Meltano ingestion)
    meltano_res = meltano_import(cfg)

    # Staging/Silver/Gold layer (dbt transformations)
    dbt_staging_res = dbt_run_staging(cfg)
    dbt_silver_res = dbt_run_silver(cfg)
    dbt_gold_res = dbt_run_gold(cfg)

    return {
        "meltano": meltano_res,
        "staging": dbt_staging_res,
        "silver": dbt_silver_res,
        "gold": dbt_gold_res,
    }
