from celine.pipelines.pipeline_prefect import PipelineRunner
from celine.pipelines.pipeline_config import PipelineConfig


def test_meltano_failure(pipeline_cfg: PipelineConfig):

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_meltano("run foo_bar")
    assert res["status"] in ("failed")
    assert "command" in res
