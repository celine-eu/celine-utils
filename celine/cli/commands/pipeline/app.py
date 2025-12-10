# celine/cli/commands/pipeline/app.py
import typer
from celine.cli.commands.pipeline.run import pipeline_run_app

pipeline_app = typer.Typer(help="Pipeline execution utilities")
pipeline_app.add_typer(pipeline_run_app, name="run")
