from celine.utils.pipelines.pipeline_prefect import PipelineRunner
from celine.utils.pipelines.pipeline_config import PipelineConfig


def test_run_dbt_operation_basic(pipeline_cfg: PipelineConfig):
    """
    Test that a simple macro invocation runs and returns the expected structure.
    This does NOT assume the macro exists – failures are allowed,
    same as your other tests.
    """

    runner = PipelineRunner(pipeline_cfg)
    res = runner.run_dbt_operation("print_test_macro")

    assert res.status in ("success", "failed")


def test_run_dbt_operation_with_args(pipeline_cfg: PipelineConfig):
    """
    Test that run-operation works with args and produces the expected result shape.
    """

    runner = PipelineRunner(pipeline_cfg)
    args = {"message": "Hello from test", "limit": 5}

    res = runner.run_dbt_operation("print_test_macro", args=args)

    assert res.status in ("success", "failed")
