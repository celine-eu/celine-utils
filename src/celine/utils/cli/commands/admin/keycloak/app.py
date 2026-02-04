# celine/cli/commands/admin/keycloak/app.py
import typer
from .clients import client_app
from .accounts import accounts_app

keycloak_app = typer.Typer(help="Keycloak management")

keycloak_app.add_typer(client_app, name="client")
keycloak_app.add_typer(accounts_app, name="accounts")
