from pydantic import Field
from celine.utils.common.config.settings import AppBaseSettings


class KeycloakClientConfig(AppBaseSettings):
    server_url: str = Field(default="http://keycloak:8080", alias="KEYCLOAK_URL")
    realm_name: str = Field(default="celine", alias="KEYCLOAK_REALM")
    client_id: str = Field(default="", alias="KEYCLOAK_CLIENT_ID")
    client_secret: str = Field(default="", alias="KEYCLOAK_CLIENT_SECRET")
    verify: bool = Field(default=True, alias="KEYCLOAK_VERIFY")
