"""Client-credentials access tokens, on ``requests``.

This used to be `python-keycloak`, for one POST. That dependency was declared only
in the `[admin]` extra while this module is imported by
`cli/commands/governance/generate.py`, which `cli/app.py` imports — so the **whole
CLI** needed an extras-only package, and `celine-utils[pipelines]` shipped a CLI that
raised `ImportError` on `--help`. Removing the admin surface removed the extra, and
the honest fix was to stop needing the dependency rather than to move it.

Deliberately **not** `celine.sdk.auth.OidcClientCredentialsProvider`, though that is
already a `[pipelines]` dependency: it is `async` and configures from `CELINE_OIDC_*`
via OIDC discovery, while this path configures from `KEYCLOAK_URL` + `KEYCLOAK_REALM`
+ `KEYCLOAK_CLIENT_ID` + `KEYCLOAK_CLIENT_SECRET`. Adopting it would change the
environment surface of `governance generate marquez` and put `asyncio.run` inside a
synchronous CLI — a behaviour change smuggled inside a dependency removal. If that
migration is wanted, it is its own change.
"""

from __future__ import annotations

import time

import requests

from celine.utils.common.keycloak.config import KeycloakClientConfig

#: Refresh this many seconds before the token actually expires, so a request issued
#: just under the wire does not arrive with a token that died in flight.
EXPIRY_BUFFER_SECONDS = 60


class KeycloakClient:
    """Fetch and cache a client-credentials access token."""

    def __init__(self, config: KeycloakClientConfig):
        self.config = config
        self.token: str | None = None
        self.token_expiry: float = 0.0

    @property
    def token_url(self) -> str:
        base = str(self.config.server_url).rstrip("/")
        return f"{base}/realms/{self.config.realm_name}/protocol/openid-connect/token"

    def _is_token_expiring(self, buffer: int = EXPIRY_BUFFER_SECONDS) -> bool:
        return not self.token or time.time() > (self.token_expiry - buffer)

    def get_access_token(self) -> str | None:
        """Return a valid access token, fetching a new one when the cached one ages out.

        Raises ``requests.HTTPError`` when the token endpoint rejects the request —
        the caller reports it, because a 401 here means misconfigured credentials and
        silently continuing unauthenticated produces a confusing 403 later.
        """
        if self._is_token_expiring():
            response = requests.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.config.client_id,
                    "client_secret": self.config.client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                verify=self.config.verify,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()

            self.token = payload["access_token"]
            # `expires_in` is seconds from now. Keycloak always sends it; the default
            # keeps a non-conforming provider from making every call fetch a token.
            self.token_expiry = time.time() + payload.get("expires_in", 60)

        return self.token
