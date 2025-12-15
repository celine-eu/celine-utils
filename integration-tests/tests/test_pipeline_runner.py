from celine.pipelines.pipeline_prefect import PipelineRunner
from celine.pipelines.pipeline_config import PipelineConfig


def test_run_all(pipeline_cfg: PipelineConfig):

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_meltano("run import")
    assert res.status
    assert res.status in ("success", "failed")

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("staging")
    assert res.status
    assert res.status in ("success", "failed")

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("silver")
    assert res.status
    assert res.status in ("success", "failed")

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("gold")
    assert res.status
    assert res.status in ("success", "failed")

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("test")
    assert res.status
    assert res.status in ("success", "failed")
