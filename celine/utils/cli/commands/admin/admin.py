from . import keycloak
from . import setup


def add_commands(subparsers):

    admin_parser = subparsers.add_parser("admin", help="Admin commands")

    admin_subparsers = admin_parser.add_subparsers(
        title="Admin Commands", dest="admin_command"
    )
    admin_subparsers.required = True

    keycloak.add_commands(admin_subparsers)
    setup.add_commands(admin_subparsers)
