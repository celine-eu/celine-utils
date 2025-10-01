from celine.pipelines.pipeline import PipelineRunner
from celine.pipelines.pipeline_config import PipelineConfig


def test_run_all(pipeline_cfg: PipelineConfig):

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_meltano("run import")
    assert res["status"] in ("success", "failed")
    assert "command" in res

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("staging")
    assert "status" in res
    assert res["status"] in ("success", "failed")

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("silver")
    assert "status" in res
    assert res["status"] in ("success", "failed")

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("gold")
    assert "status" in res
    assert res["status"] in ("success", "failed")

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("test")
    assert "status" in res
    assert res["status"] in ("success", "failed")
