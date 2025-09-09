from .clients import add_commands as add_client_commands
from .accounts import add_commands as add_accounts_commands


def add_commands(subparsers):

    kc_parser = subparsers.add_parser("keycloak", help="Keycloak management commands")
    kc_subparsers = kc_parser.add_subparsers(
        title="Keycloak subcommands", dest="keycloak_command"
    )
    kc_subparsers.required = True

    add_client_commands(kc_subparsers)
    add_accounts_commands(kc_subparsers)
