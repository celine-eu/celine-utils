import argparse
from celine.cli.commands.admin import admin


def main():
    parser = argparse.ArgumentParser(description="CELINE CLI")
    subparsers = parser.add_subparsers(title="Commands", dest="command")
    subparsers.required = True

    # Admin commands
    admin.add_commands(subparsers)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
