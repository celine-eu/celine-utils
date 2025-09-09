from celine.admin.clients import get_client_secret, create_client, import_client
from celine.cli.utils import load_json_config


def command_admin_get_secret(args):
    print(get_client_secret(args.client_id))


def command_admin_create_client(args):
    print(
        create_client(
            args.client_id,
            redirect_uris=args.redirect_uris,
            recreate=args.force,
        ),
    )


def command_admin_import_client(args):
    client_dict = load_json_config(args.client_json)
    if args.reset_secret:
        client_dict["secret"] = None
    print(
        import_client(
            client_dict,
            recreate=args.force,
        ),
    )


def add_commands(subparsers):
    # Get client secret
    parser_secret = subparsers.add_parser("get-secret", help="Get client secret")
    parser_secret.add_argument(
        "--client-id", required=False, help="Keycloak client id (name)"
    )
    parser_secret.set_defaults(func=command_admin_get_secret)

    # Create client
    parser_create = subparsers.add_parser("create-client", help="Create client")
    parser_create.add_argument("client_id", help="Keycloak client id (name)")
    parser_create.add_argument(
        "--redirect-uri",
        action="append",
        help="Redirect URI for the client (can be repeated)",
        dest="redirect_uris",
    )
    parser_create.add_argument(
        "-f", "--force", action="store_true", help="Force recreation"
    )
    parser_create.set_defaults(func=command_admin_create_client)

    # Import client
    parser_import = subparsers.add_parser(
        "import-client", help="Import client from JSON"
    )
    parser_import.add_argument(
        "client_json",
        help="File path. Use - to use stdin",
    )
    parser_import.add_argument(
        "-f", "--force", action="store_true", help="Force recreation", default=True
    )
    parser_import.add_argument(
        "-p", "--reset-secret", action="store_true", help="Reset secret", default=True
    )
    parser_import.set_defaults(func=command_admin_import_client)
