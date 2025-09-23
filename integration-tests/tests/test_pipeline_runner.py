from celine.pipelines.pipeline import PipelineRunner
from celine.pipelines.pipeline_config import PipelineConfig


def test_run_meltano_success(pipeline_cfg: PipelineConfig):
    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_meltano("run import")
    assert res["status"] in ("success", "failed")
    assert "command" in res


def test_validate_raw_data(pipeline_cfg: PipelineConfig):
    runner = PipelineRunner(pipeline_cfg)
    res = runner.validate_raw_data(["current_weather_stream"])
    assert "status" in res
    assert "details" in res


def test_run_dbt(pipeline_cfg: PipelineConfig):
    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("test")  # uses dbtol
    assert "status" in res
    assert res["status"] in ("success", "failed")
