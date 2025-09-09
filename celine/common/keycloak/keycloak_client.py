from keycloak import KeycloakOpenID
import time

from .config import KeycloakClientConfig


class KeycloakClient:
    def __init__(self, config: KeycloakClientConfig):
        self.config = config
        self.openid = KeycloakOpenID(
            server_url=str(self.config.server_url),
            client_id=self.config.client_id,
            realm_name=self.config.realm_name,
            client_secret_key=self.config.client_secret,
            verify=self.config.verify,
        )
        self.token = None
        self.token_expiry = 0

    def _is_token_expiring(self, buffer=60):
        # buffer: refresh if less than buffer seconds to expire
        return not self.token or time.time() > (self.token_expiry - buffer)

    def get_access_token(self):
        if self._is_token_expiring():
            token = self.openid.token(grant_type="client_credentials")
            self.token = token["access_token"]
            # exp time is now + expires_in
            self.token_expiry = time.time() + token["expires_in"]
        return self.token
