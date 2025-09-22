from celine.pipelines.lineage.meltano import MeltanoLineage
from celine.pipelines.pipeline_config import PipelineConfig


def test_emit_meltano_run(pipeline_cfg):
    lineage = MeltanoLineage(
        cfg=PipelineConfig(),
        config_path=f"{pipeline_cfg.meltano_project_root}/meltano.yml",
        run_dir=f"{pipeline_cfg.meltano_project_root}/.meltano/run",
    )
    # simulate lineage emission
    lineage.emit_run("prod:tap-openweathermap-valencia-to-target-postgres")
