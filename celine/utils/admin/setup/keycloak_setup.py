from celine.utils.common.keycloak import (
    KeycloakAdminConfig,
    KeycloakAdminClient,
    KeycloakClientConfig,
)
from celine.utils.common.logger import get_logger
from pathlib import Path

current_directory = Path.cwd()

logger = get_logger(__name__)


def update_keycloak_client_secret(env_path, new_secret):
    """
    Replace the value of KEYCLOAK_CLIENT_SECRET in the given .env file if it exists.

    :param env_path: Path to the .env file.
    :param new_secret: The new secret to set.
    """
    import re

    with open(env_path, "r") as file:
        lines = file.readlines()

    pattern = re.compile(r"^(KEYCLOAK_CLIENT_SECRET\s*=\s*).*$")
    updated = False

    for i, line in enumerate(lines):
        if pattern.match(line):
            lines[i] = f"KEYCLOAK_CLIENT_SECRET={new_secret}\n"
            updated = True
            break

    if updated:
        with open(env_path, "w") as file:
            file.writelines(lines)

    return updated  # True if replaced, False otherwise


def add_group_to_client_scope_mappers(
    admin_session: KeycloakAdminClient,
    client_scope_name: str,
    protocol_mapper_name: str = "groups",
    protocol_mapper_config: dict = {},
):
    """
    Add a group to the client scope mappers for a specific client scope.

    Args:
        admin_session: KeycloakAdminClient instance
        client_scope_name: Name of the client scope to add the mapper to
        group_name: The group name to add as a mapper
        protocol_mapper_name: Name of the protocol mapper (default: "groups")
        protocol_mapper_config: Additional configuration for the mapper

    Returns:
        bool: True if the mapper was added or already existed, False on error
    """
    try:
        added = admin_session.add_group_to_client_scope_mappers_if_not_exists(
            client_scope_name=client_scope_name,
            protocol_mapper_name=protocol_mapper_name,
            protocol_mapper_config=protocol_mapper_config,
        )
        if added:
            logger.info(
                f"Added group mapper '{protocol_mapper_name}' to client scope '{client_scope_name}'"
            )
        else:
            logger.info(
                f"Group mapper '{protocol_mapper_name}' already exists for client scope '{client_scope_name}'"
            )
        return True
    except Exception as e:
        logger.error(
            f"Failed to add group mapper '{protocol_mapper_name}' to client scope '{client_scope_name}': {str(e)}"
        )
        return False


def setup(config: dict[str, str] = {}):

    kc_admin_config = KeycloakAdminConfig().model_validate(config)
    kc_client_config = KeycloakClientConfig().model_validate(config)

    # Admin session
    admin_session = KeycloakAdminClient(kc_admin_config)

    try:
        logger.info(f"Ensure realm exists {kc_client_config.realm_name}")
        admin_session.create_realm_if_not_exists(
            kc_client_config.realm_name,
            enabled=True,
            displayName=kc_client_config.realm_name,
        )
        logger.info(f"Realm created {kc_client_config.realm_name}")
    except Exception as e:
        logger.error(e)
        return False

    # Add group to client scope mappers if needed
    client_scope_name = "profile"
    group_name = "groups"
    mapper_added = add_group_to_client_scope_mappers(
        admin_session=admin_session,
        client_scope_name=client_scope_name,
        protocol_mapper_name=group_name,
    )
    if not mapper_added:
        logger.warning(f"Failed to add group mapper for '{group_name}'")

    try:
        logger.info(f"Create client {kc_admin_config.client_id}")
        created, secret = admin_session.create_client_if_not_exists(
            client_id=kc_client_config.client_id,
            realm_name=kc_client_config.realm_name,
            secret=kc_client_config.client_secret,
            service_accounts_enabled=True,
            redirect_uris=["*"],
        )
        if created:
            logger.info(f"Client created with secret: {secret}")
        else:
            logger.info(f"Client already existed, secret: {secret}")

        env_file = f"{current_directory}/.env"
        updated = update_keycloak_client_secret(env_file, secret)
        if updated:
            logger.info(f"env file {env_file} updated")

    except Exception as e:
        logger.error(e)
        return False


if __name__ == "__main__":
    setup()
