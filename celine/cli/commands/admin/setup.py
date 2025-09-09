from celine.admin.setup import run_setup


def command_admin_setup(args):
    run_setup()


def add_commands(subparsers):
    # Setup command
    parser_setup = subparsers.add_parser("setup", help="Run setup process")
    parser_setup.set_defaults(func=command_admin_setup)
