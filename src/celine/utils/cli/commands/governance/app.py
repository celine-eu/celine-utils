import typer
from celine.utils.cli.commands.governance.generate import generate_app
from celine.utils.cli.commands.governance.graph import graph_command

governance_app = typer.Typer(help="Governance utilities")

governance_app.add_typer(generate_app, name="generate")
governance_app.command("graph")(graph_command)
