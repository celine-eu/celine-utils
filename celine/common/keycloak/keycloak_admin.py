from celine.common.logger import get_logger
from .config import KeycloakAdminConfig

from typing import Tuple

from keycloak import KeycloakAdmin, KeycloakOpenID


class KeycloakAdminClient:

    _admin_client: KeycloakAdmin

    def __init__(self, config: KeycloakAdminConfig):
        self.logger = get_logger(__name__)
        self.config = config

    def admin(self) -> KeycloakAdmin:
        if not self._admin_client:
            self.login()
        return self._admin_client

    def login(self):
        self._admin_client = KeycloakAdmin(
            server_url=str(self.config.server_url),
            username=self.config.username,
            password=self.config.password,
            user_realm_name=self.config.admin_realm_name,
            realm_name=self.config.realm_name,
            # client_id=self.config.client_id if len(self.config.client_secret) else None,
            # client_secret_key=(
            #     self.config.client_secret if len(self.config.client_secret) else None
            # ),
            verify=self.config.verify,
        )
        return self._admin_client

    def get_clients(self):
        return self.admin().get_clients()

    def create_realm_if_not_exists(
        self, realm_name: str, enabled: bool = True, **kwargs
    ):
        """
        Create a realm if it doesn't exist.

        :param realm_name: Name of the realm to create
        :param enabled: Whether the realm is enabled
        :param kwargs: Any additional attributes to pass to the realm creation
        :return: True if created, False if already exists
        """
        realms = self.admin().get_realms()
        if any(r["realm"] == realm_name for r in realms):
            self.logger.debug(f"Realm '{realm_name}' already exists.")
            return False  # Already exists

        # Compose realm representation
        realm_rep = {"realm": realm_name, "enabled": enabled, **kwargs}
        self.admin().create_realm(payload=realm_rep, skip_exists=True)
        self.logger.debug(f"Realm '{realm_name}' created.")
        return True

    def create_client_if_not_exists(
        self,
        client_id: str,
        realm_name: str | None = None,
        redirect_uris: list | None = None,
        service_accounts_enabled: bool = True,
        standard_flow_enabled: bool = True,
        direct_access_grants_enabled: bool = False,
        **kwargs,
    ) -> Tuple[bool, str]:
        """
        Create a confidential client suitable for client_credentials if not exists.
        If 'secret' is provided, set the client secret after creation.
        Returns (created: bool, client_secret: str)
        """
        realm = realm_name or self.config.realm_name

        if len(client_id) == 0:
            raise Exception("client_id must not be empty")

        # Prepare client payload
        client_rep = {
            "clientId": client_id,
            "enabled": True,
            "protocol": "openid-connect",
            "publicClient": False,
            "serviceAccountsEnabled": service_accounts_enabled,
            "standardFlowEnabled": standard_flow_enabled,
            "directAccessGrantsEnabled": direct_access_grants_enabled,
            "redirectUris": redirect_uris or [],
            **kwargs,
        }

        # Create client
        self.admin().create_client(payload=client_rep, skip_exists=True)
        self.logger.debug(f"Client '{client_id}' created in realm '{realm}'.")

        # Get the newly created client to fetch the id
        clients = self.admin().get_clients()
        client = [c for c in clients if c.get("clientId") == client_id][0]
        client_id_internal = client["id"]

        client_secrets = self.admin().get_client_secrets(client_id=client_id_internal)[
            "value"
        ]

        return True, client_secrets

    def create_user_with_password(
        self,
        username: str,
        password: str,
        realm_name: str | None = None,
        email: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        enabled: bool = True,
        email_verified: bool = False,
        **kwargs,
    ):
        """
        Creates a user with the given password in the specified realm.
        Returns the user_id (uuid) if created, or None if user exists.
        """
        realm = realm_name or self.config.realm_name

        # Check if user exists
        users = self.admin().get_users(query={"username": username})
        if users:
            self.logger.debug(f"User '{username}' already exists in realm '{realm}'.")
            return None

        user_rep = {
            "username": username,
            "enabled": enabled,
            "email": email,
            "firstName": first_name,
            "lastName": last_name,
            "emailVerified": email_verified,
            **kwargs,
        }
        user_id = self.admin().create_user(payload=user_rep)
        self.logger.debug(f"User '{username}' created in realm '{realm}'.")

        # Set password
        self.admin().set_user_password(
            user_id=user_id, password=password, temporary=False
        )

        self.logger.debug(f"Password set for user '{username}'.")

        return user_id

    def update_user_password(
        self, username: str, password: str, realm_name: str | None = None
    ):
        """
        Updates the password for an existing user in the given realm.
        """
        realm = realm_name or self.config.realm_name

        users = self.admin().get_users(query={"username": username})
        if not users:
            raise ValueError(f"User '{username}' not found in realm '{realm}'.")
        user_id = users[0]["id"]
        self.admin().set_user_password(
            user_id=user_id, password=password, temporary=False
        )
        self.logger.debug(f"Password updated for user '{username}'.")
        return user_id

    def import_client(
        self,
        client_dict: dict[str, str],
        realm_name: str | None = None,
        skip_exists: bool = True,
        force: bool = False,
    ) -> Tuple[bool, str | None]:
        """
        Imports a client config from a dict (as from a JSON file).
        If force is True, removes the client if exists and recreates it.
        """
        realm = realm_name or self.config.realm_name

        client_id = client_dict.get("clientId")
        if not client_id:
            raise Exception("client definition is missing client_id")

        clients = self.admin().get_clients()
        exists = any(c.get("clientId") == client_id for c in clients)
        if exists:
            if force:
                self.delete_client(client_id=client_id, realm_name=realm)
                self.logger.debug(
                    f"Client '{client_id}' force deleted for import in realm '{realm}'."
                )
        if not skip_exists:
            self.admin().create_client(payload=client_dict)
            self.logger.debug(f"Client '{client_id}' imported into realm '{realm}'.")
        else:
            self.logger.debug(
                f"Client '{client_id}' already exists in realm '{realm}', skipping import."
            )

        secret = self.get_client_secret(client_id)

        return True, secret

    def delete_client(self, client_id: str, realm_name: str | None = None):
        """
        Deletes a client by client_id in the specified realm.
        Returns True if deleted, False if not found.
        """
        realm = realm_name or self.config.realm_name
        clients = self.admin().get_clients()

        client = next((c for c in clients if c.get("clientId") == client_id), None)
        if not client:
            self.logger.debug(f"Client '{client_id}' not found in realm '{realm}'.")
            return False

        self.admin().delete_client(client_id=client["id"])
        self.logger.debug(f"Client '{client_id}' deleted from realm '{realm}'.")

        return True

    def get_client_secret(self, client_name: str | None = None) -> str | None:

        client_name = (
            client_name if client_name is not None and client_name != "" else None
        )
        if client_name is None:
            raise Exception("client name is missing")

        clients = self.admin().get_clients()
        client = next((c for c in clients if c["clientId"] == client_name), None)
        if not client:
            self.logger.warning(f"client {client_name} not found")
            return None

        client_id = client["id"]
        # Now get the secret
        secret_obj = self.admin().get_client_secrets(client_id)
        if not secret_obj:
            self.logger.warning(f"failed to get secret for client {client_name}")
            return None

        return str(secret_obj["value"])

    def get_token(
        self,
    ) -> dict:
        """
        Get a Keycloak token after user login.

        Returns:
            dict: Token info (includes access_token, refresh_token, etc.)
        """
        keycloak_openid = KeycloakOpenID(
            server_url=self.config.server_url,
            client_id=self.config.client_id,
            realm_name=self.config.realm_name,
            client_secret_key=self.config.client_secret,
            verify=True,
        )

        # Get token using username/password grant
        token = keycloak_openid.token(self.config.username, self.config.password)
        return token

    def create_realm_role_if_not_exists(self, role_name: str):
        """Create a realm role if it does not exist."""
        roles = self.admin().get_realm_roles()
        if not any(r["name"] == role_name for r in roles):
            self.admin().create_realm_role({"name": role_name})
            self.logger.debug(f"Created realm role: {role_name}")
            return True
        else:
            self.logger.debug(f"Realm role '{role_name}' already exists.")
            return False

    def create_group_if_not_exists(self, group_name: str):
        """Create a group if it does not exist."""
        groups = self.admin().get_groups()
        group_obj = next((g for g in groups if g["name"] == group_name), None)
        if not group_obj:
            self.admin().create_group({"name": group_name})
            self.logger.debug(f"Created group: {group_name}")
            # Refresh to get the new group
            groups = self.admin().get_groups()
            group_obj = next((g for g in groups if g["name"] == group_name), None)
        else:
            self.logger.debug(f"Group '{group_name}' already exists.")
        return group_obj

    def assign_roles_to_group(self, group_name: str, role_names: list):
        """Assign one or more realm roles to a group."""
        groups = self.admin().get_groups()
        group_obj = next((g for g in groups if g["name"] == group_name), None)
        if not group_obj:
            raise ValueError(f"Group '{group_name}' not found.")

        # Get already assigned roles
        assigned_roles = self.admin().get_group_realm_roles(group_obj["id"])
        assigned_names = [r["name"] for r in assigned_roles]
        # Assign any missing roles
        for role_name in role_names:
            if role_name not in assigned_names:
                role_obj = next(
                    (
                        r
                        for r in self.admin().get_realm_roles()
                        if r["name"] == role_name
                    ),
                    None,
                )
                if role_obj:
                    self.admin().assign_group_realm_roles(
                        group_obj["id"], roles=[role_obj]
                    )
                    self.logger.debug(
                        f"Assigned role '{role_name}' to group '{group_name}'."
                    )

    def assign_user_to_groups(self, user_id: str, group_names: list):
        """Add a user to one or more groups by name."""
        all_groups = self.admin().get_groups()
        for group_name in group_names:
            group_obj = next((g for g in all_groups if g["name"] == group_name), None)
            if group_obj:
                self.admin().group_user_add(user_id=user_id, group_id=group_obj["id"])
                self.logger.debug(f"Added user '{user_id}' to group '{group_name}'.")

    def delete_group(self, group_name: str):
        """Delete a group by its name."""
        groups = self.admin().get_groups()
        group_obj = next((g for g in groups if g["name"] == group_name), None)
        if not group_obj:
            self.logger.info(f"Group '{group_name}' not found.")
            return False
        self.admin().delete_group(group_id=group_obj["id"])
        self.logger.info(f"Deleted group '{group_name}'.")
        return True

    def delete_user(self, username: str):
        """Delete a user by username."""
        users = self.admin().get_users(query={"username": username})
        if not users:
            self.logger.info(f"User '{username}' not found.")
            return False
        user_id = users[0]["id"]
        self.admin().delete_user(user_id=user_id)
        self.logger.info(f"Deleted user '{username}'.")
        return True

    def delete_realm_role(self, role_name: str):
        """Delete a realm role by name."""
        roles = self.admin().get_realm_roles()
        role_obj = next((r for r in roles if r["name"] == role_name), None)
        if not role_obj:
            self.logger.info(f"Realm role '{role_name}' not found.")
            return False
        self.admin().delete_realm_role(role_name=role_name)
        self.logger.info(f"Deleted realm role '{role_name}'.")
        return True

    def add_group_to_client_scope_mappers_if_not_exists(
        self,
        client_scope_name: str,
        protocol_mapper_name: str = "groups",
        protocol_mapper_type: str = "oidc-group-membership-mapper",
        protocol_mapper_config: dict | None = None,
        protocol: str = "openid-connect",
        realm_name: str | None = None,
    ):
        """
        Add a group to the client scope mappers if it doesn't already exist.
        This ensures the group is included in the token claims for the specified client scope.

        Args:
            client_scope_name: Name of the client scope to add the mapper to
            group_name: The group name to add as a mapper
            protocol_mapper_name: Name of the protocol mapper (default: "groups")
            protocol_mapper_type: Type of the protocol mapper (default: "oidc-group-membership-mapper")
            protocol_mapper_config: Additional configuration for the mapper
            protocol: Protocol type (default: openid-connect)
            realm_name: Optional realm name (defaults to configured realm)

        Returns:
            bool: True if the mapper was added, False if it already existed
        """
        realm = realm_name or self.config.realm_name

        # Get the client scope
        client_scope = self.admin().get_client_scope_by_name(
            client_scope_name=client_scope_name
        )

        if not client_scope:
            raise ValueError(
                f"Client scope '{client_scope_name}' not found in realm '{realm}'"
            )

        client_scope_id = client_scope["id"]

        # Get existing protocol mappers for this client scope
        existing_mappers = self.admin().get_mappers_from_client_scope(
            client_scope_id=client_scope_id
        )

        # Default configuration matching the UI payload
        default_config = {
            "full.path": "true",
            "introspection.token.claim": "true",
            "userinfo.token.claim": "true",
            "id.token.claim": "true",
            "lightweight.claim": "false",
            "access.token.claim": "true",
            "claim.name": "groups",
        }

        if protocol_mapper_config:
            default_config.update(protocol_mapper_config)

        # Check if a mapper with the same configuration already exists
        mapper_exists = any(
            mapper["name"] == protocol_mapper_name
            and mapper["config"] == default_config
            for mapper in existing_mappers
        )

        if mapper_exists:
            self.logger.debug(
                f"Group mapper '{protocol_mapper_name}' already exists for client scope '{client_scope_name}'"
            )
            return False

        # Create the new mapper
        mapper_rep = {
            "name": protocol_mapper_name,
            "protocol": protocol,
            "protocolMapper": protocol_mapper_type,
            "config": default_config,
        }

        # Add the mapper to the client scope
        self.admin().add_mapper_to_client_scope(
            client_scope_id=client_scope_id, payload=mapper_rep
        )

        self.logger.debug(
            f"Added group mapper '{protocol_mapper_name}' to client scope '{client_scope_name}'"
        )
        return True
