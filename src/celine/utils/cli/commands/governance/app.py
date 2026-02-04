import typer
from celine.utils.cli.commands.governance.generate import generate_app

governance_app = typer.Typer(help="Governance utilities")

governance_app.add_typer(generate_app, name="generate")
