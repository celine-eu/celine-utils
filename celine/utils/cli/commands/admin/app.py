import typer
from .keycloak.app import keycloak_app
from .setup import setup_app

admin_app = typer.Typer(help="Admin commands")

admin_app.add_typer(keycloak_app, name="keycloak")
admin_app.add_typer(setup_app, name="setup")
