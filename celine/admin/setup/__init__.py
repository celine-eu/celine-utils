from . import keycloak_setup, superset_setup


def run_setup():
    keycloak_setup.setup()
    # superset_setup.setup()
