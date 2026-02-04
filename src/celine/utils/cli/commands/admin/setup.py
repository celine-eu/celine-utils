# celine/cli/commands/admin/setup.py
import typer
from celine.utils.admin.setup import run_setup

setup_app = typer.Typer(help="Setup operations")


@setup_app.command("run")
def setup_run():
    """Run entire admin setup (keycloak, superset...)"""
    run_setup()
    typer.echo("Setup completed")
