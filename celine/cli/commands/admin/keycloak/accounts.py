from celine.admin.clients import import_accounts
from celine.cli.utils import load_json_config


def command_admin_import_accounts(args):
    client_dict = load_json_config(args.accounts_json)
    import_accounts(
        client_dict,
        recreate=args.force,
    )


def add_commands(subparsers):
    # Import accounts
    parser_accounts = subparsers.add_parser(
        "import-accounts", help="Import accounts from JSON"
    )
    parser_accounts.add_argument(
        "accounts_json",
        help="File path. Use - to use stdin",
    )
    parser_accounts.add_argument(
        "-f", "--force", action="store_true", help="Force recreation", default=True
    )
    parser_accounts.set_defaults(func=command_admin_import_accounts)
