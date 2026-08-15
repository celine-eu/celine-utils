"""The CLI's Keycloak token client, and the packaging bug it used to carry.

`celine.utils.common.keycloak` is imported by `governance generate marquez`, which
`cli/app.py` imports — so anything this module needs, the **whole CLI** needs. It
needed `python-keycloak`, which was declared only in the `[admin]` extra, so
`celine-utils[pipelines]` shipped a CLI that raised ImportError on `--help`.

These tests are the guard: the dependency must stay out, and the admin command tree
must stay removed.

The whole file skips on a core-only install (`task test:thin`), where `requests` and
`pydantic-settings` are absent by design.
"""

import sys
import time

import pytest

pytest.importorskip("requests", reason="[pipelines] extra not installed")
pytest.importorskip("pydantic_settings", reason="[pipelines] extra not installed")

from celine.utils.common.keycloak import KeycloakClient, KeycloakClientConfig  # noqa: E402


class BlockKeycloakPackage:
    """Meta-path finder that makes `import keycloak` fail.

    Uses `find_spec`; the legacy `find_module` protocol is ignored on modern Python,
    so a hook written that way silently does nothing and the test passes for the
    wrong reason.
    """

    def find_spec(self, name, path=None, target=None):
        if name == "keycloak" or name.startswith("keycloak."):
            raise ImportError(f"No module named {name!r} (python-keycloak absent)")
        return None


@pytest.fixture
def config():
    return KeycloakClientConfig(
        KEYCLOAK_URL="http://kc.example:8080/",
        KEYCLOAK_REALM="celine",
        KEYCLOAK_CLIENT_ID="svc",
        KEYCLOAK_CLIENT_SECRET="shh",
    )


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self._payload


def test_the_cli_does_not_need_python_keycloak(monkeypatch):
    """The regression that shipped a broken `celine-utils[pipelines]`.

    If someone reintroduces `from keycloak import ...` anywhere the CLI imports,
    this fails here rather than at a consumer's install.
    """
    for name in list(sys.modules):
        if name.startswith(("celine.utils.cli", "celine.utils.common.keycloak")):
            monkeypatch.delitem(sys.modules, name, raising=False)

    monkeypatch.setattr(sys, "meta_path", [BlockKeycloakPackage(), *sys.meta_path])

    import celine.utils.cli.app as cli_app

    assert cli_app.app is not None


def test_the_admin_command_tree_is_gone():
    """`celine-utils admin *` was removed in 3.0.0 — a dead replicable-setup tool,
    superseded by celine-policies' Keycloak CLI. Re-adding it here would reintroduce
    the extra and the dependency that came with it."""
    import celine.utils.cli.app as cli_app

    names = {group.name for group in cli_app.app.registered_groups}
    assert names == {"governance", "pipeline"}

    with pytest.raises(ImportError):
        import celine.utils.admin  # noqa: F401


def test_token_url_is_built_from_realm(config):
    assert config.server_url.endswith("/")  # trailing slash must not double up
    assert (
        KeycloakClient(config).token_url
        == "http://kc.example:8080/realms/celine/protocol/openid-connect/token"
    )


def test_fetches_a_token_with_client_credentials(monkeypatch, config):
    seen = {}

    def fake_post(url, data=None, headers=None, verify=None, timeout=None):
        seen.update(url=url, data=data, verify=verify)
        return FakeResponse({"access_token": "tok-1", "expires_in": 300})

    monkeypatch.setattr("requests.post", fake_post)

    assert KeycloakClient(config).get_access_token() == "tok-1"
    assert seen["data"]["grant_type"] == "client_credentials"
    assert seen["data"]["client_id"] == "svc"
    assert seen["data"]["client_secret"] == "shh"
    assert seen["verify"] is True


def test_token_is_cached_until_it_nears_expiry(monkeypatch, config):
    calls = []

    def fake_post(url, **kwargs):
        calls.append(url)
        return FakeResponse({"access_token": f"tok-{len(calls)}", "expires_in": 300})

    monkeypatch.setattr("requests.post", fake_post)

    client = KeycloakClient(config)
    assert client.get_access_token() == "tok-1"
    assert client.get_access_token() == "tok-1"
    assert len(calls) == 1


def test_token_is_refetched_inside_the_expiry_buffer(monkeypatch, config):
    """Refreshing early is the point: a token that expires in flight produces a 401
    the caller cannot distinguish from bad credentials."""
    calls = []

    def fake_post(url, **kwargs):
        calls.append(url)
        return FakeResponse({"access_token": f"tok-{len(calls)}", "expires_in": 300})

    monkeypatch.setattr("requests.post", fake_post)

    client = KeycloakClient(config)
    client.get_access_token()
    # 30s of life left — inside the 60s buffer.
    client.token_expiry = time.time() + 30

    assert client.get_access_token() == "tok-2"
    assert len(calls) == 2


def test_a_rejected_request_raises_rather_than_returning_none(monkeypatch, config):
    """A 401 means misconfigured credentials. Continuing unauthenticated turns it
    into a confusing 403 from Marquez much later."""
    import requests

    monkeypatch.setattr("requests.post", lambda url, **kw: FakeResponse({}, status=401))

    with pytest.raises(requests.HTTPError):
        KeycloakClient(config).get_access_token()
