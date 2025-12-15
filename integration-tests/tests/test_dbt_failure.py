from celine.pipelines.pipeline_prefect import PipelineRunner
from celine.pipelines.pipeline_config import PipelineConfig


def test_dbt_model_failure(pipeline_cfg: PipelineConfig):
    """
    Validates that the failing model causes run_dbt to return a failed status.
    This relies on the failing model defined in the dbt project.
    """

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("failing_model")

    assert res.status == "failed"

    # Ensure the error is propagated into the result text
    details = str(res.details)
    assert "failure in model failing_model" in details.lower()
