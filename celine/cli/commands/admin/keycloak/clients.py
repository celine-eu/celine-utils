# celine/cli/commands/admin/keycloak/clients.py
import typer
from celine.admin.clients import (
    get_client_secret,
    create_client,
    import_client,
)
from celine.cli.utils import load_json_config

client_app = typer.Typer(help="Keycloak client commands")


@client_app.command("get-secret")
def cli_get_secret(client_id: str = typer.Option(None, help="Client ID")):
    """Get Keycloak client secret"""
    typer.echo(get_client_secret(client_id))


@client_app.command("create")
def cli_create(
    client_id: str,
    redirect_uri: list[str] = typer.Option([], "--redirect-uri"),
    force: bool = typer.Option(False, "--force", "-f"),
):
    """Create a Keycloak client"""
    secret = create_client(
        client_id,
        redirect_uris=redirect_uri,
        recreate=force,
    )
    typer.echo(secret)


@client_app.command("import")
def cli_import(
    client_json: str,
    force: bool = typer.Option(False, "--force", "-f"),
    reset_secret: bool = typer.Option(False, "--reset-secret", "-p"),
):
    """Import Keycloak client from JSON"""
    client_dict = load_json_config(client_json)
    if reset_secret:
        client_dict["secret"] = None
    secret = import_client(client_dict, recreate=force)
    typer.echo(secret)
