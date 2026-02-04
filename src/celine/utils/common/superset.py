import requests

from celine.utils.common.config.superset import SupersetConfig


class SupersetClient:
    def __init__(self, config: dict[str, str] | None = None):
        self.config = SupersetConfig.model_validate(config)
        self.token = self._authenticate()
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _authenticate(self) -> str:
        url = f"{self.config.host}/api/v1/security/login"
        payload = {
            "username": self.config.username,
            "password": self.config.password,
            # "provider": self.config.provider,
        }
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()["access_token"]

    def list_databases(self):
        url = f"{self.config.host}/api/v1/database/"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json()

    def add_database(self, database_name: str, sqlalchemy_uri: str):
        url = f"{self.config.host}/api/v1/database/"
        payload = {"database_name": database_name, "sqlalchemy_uri": sqlalchemy_uri}
        resp = requests.post(url, json=payload, headers=self.headers)
        resp.raise_for_status()
        return resp.json()
