# celine/cli/commands/pipeline/app.py
import typer
from celine.utils.cli.commands.pipeline.run import pipeline_run_app
from celine.utils.cli.commands.pipeline.init import pipeline_init_app

pipeline_app = typer.Typer(help="Pipeline execution utilities")
pipeline_app.add_typer(pipeline_run_app, name="run")
pipeline_app.add_typer(pipeline_init_app, name="init")
