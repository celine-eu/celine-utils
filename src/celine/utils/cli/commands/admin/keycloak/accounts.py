# celine/cli/commands/admin/keycloak/accounts.py
import typer
from celine.utils.admin.clients import import_accounts
from celine.utils.cli.utils import load_json_config

accounts_app = typer.Typer(help="Keycloak accounts (roles/groups/users)")


@accounts_app.command("import")
def cli_import_accounts(
    accounts_json: str,
    force: bool = typer.Option(False, "--force", "-f"),
):
    """Import accounts (roles, groups, users) from JSON"""
    cfg = load_json_config(accounts_json)
    import_accounts(cfg, recreate=force)
    typer.echo("Accounts imported")
