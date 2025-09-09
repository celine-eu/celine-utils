from celine.common.keycloak import (
    KeycloakAdminConfig,
    KeycloakAdminClient,
)


def create_admin_session() -> KeycloakAdminClient:
    kc_admin_config = KeycloakAdminConfig()
    admin_session = KeycloakAdminClient(kc_admin_config)
    return admin_session


def get_token():
    admin_session = create_admin_session()

    return admin_session.get_token()


def get_client_secret(client_name: str | None = None):
    admin_session = create_admin_session()

    client_name = (
        client_name
        if client_name is not None and len(client_name) > 0
        else admin_session.config.client_id
    )
    secret = admin_session.get_client_secret(client_name)

    return secret


def create_client(
    client_name: str, redirect_uris: list[str] = [], recreate: bool = False
):
    admin_session = create_admin_session()

    if client_name == "":
        return None

    if recreate == True:
        admin_session.delete_client(client_name)

    created, secret = admin_session.create_client_if_not_exists(
        client_name, redirect_uris=redirect_uris
    )
    return secret


def import_client(client_dict: dict, recreate: bool = False):
    admin_session = create_admin_session()

    created, secret = admin_session.import_client(
        client_dict,
        skip_exists=False,
        force=recreate,
    )
    return secret


def import_accounts(accounts_dict: dict, recreate: bool = False):
    """Seed roles, groups (with role assignments), and users (with group memberships) from a JSON dict."""
    admin_session = create_admin_session()

    # 1. Create roles
    for role_name in accounts_dict.get("roles", []):
        if recreate:
            admin_session.delete_realm_role(role_name)
        admin_session.create_realm_role_if_not_exists(role_name)

    # 2. Create groups and assign roles to them
    for group in accounts_dict.get("groups", []):
        if recreate:
            admin_session.delete_group(group["name"])
        group_obj = admin_session.create_group_if_not_exists(group["name"])
        admin_session.assign_roles_to_group(group["name"], group.get("roles", []))

    # 3. Create users and assign to groups
    for user in accounts_dict.get("users", []):

        if recreate:
            admin_session.delete_user(user["username"])

        user_id = admin_session.create_user_with_password(
            username=user["username"],
            password=user["password"],
            email=user.get("email"),
            first_name=user.get("firstName"),
            last_name=user.get("lastName"),
            email_verified=user.get("emailVerified"),
        )
        if not user_id:
            # User exists: fetch their ID
            user_id = admin_session.admin.get_users(
                query={"username": user["username"]}
            )[0]["id"]
            admin_session.logger.info(
                f"User '{user['username']}' already exists. Updating group membership."
            )
        else:
            admin_session.logger.info(f"Created user '{user['username']}'.")

        admin_session.assign_user_to_groups(user_id, user.get("groups", []))
