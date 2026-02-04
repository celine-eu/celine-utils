# celine/cli/app.py
import typer
from celine.utils.cli.commands.admin.app import admin_app
from celine.utils.cli.commands.governance.app import governance_app
from celine.utils.cli.commands.pipeline.app import pipeline_app

app = typer.Typer(help="CELINE CLI")

# mount subcommands
app.add_typer(admin_app, name="admin")
app.add_typer(governance_app, name="governance")
app.add_typer(pipeline_app, name="pipeline")
