from celine.utils.cli.debugger import start_debugger_if_requested


def main():
    # Before the Typer app is imported, so `DEBUGGER_WAIT=1` can catch a breakpoint
    # in module-level code and not only inside a command body. Importing `app` at
    # module scope would defeat that — the import would already have run by the time
    # this function is called.
    start_debugger_if_requested()

    from celine.utils.cli.app import app

    app()


if __name__ == "__main__":
    main()
