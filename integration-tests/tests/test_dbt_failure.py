from celine.pipelines.pipeline_prefect import PipelineRunner
from celine.pipelines.pipeline_config import PipelineConfig


def test_dbt_model_failure(pipeline_cfg: PipelineConfig):
    """
    Validates that the failing model causes run_dbt to return a failed status.
    This relies on the failing model defined in the dbt project.
    """

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt("failing_model")

    assert "status" in res
    assert res["status"] == "failed", "failing model should cause dbt run to fail"

    # command + details always included
    assert "command" in res
    assert "details" in res

    # Ensure the error is propagated into the result text
    assert (
        "division" in res["details"].lower()
        or "zero" in res["details"].lower()
        or "error" in res["details"].lower()
    )
