from pydantic import Field
from .settings import AppBaseSettings


class SupersetConfig(AppBaseSettings):
    host: str = Field("http://superset:8088", alias="SUPERSET_HOST")
    username: str = Field(default="admin", alias="SUPERSET_USER")
    password: str = Field(default="admin", alias="SUPERSET_PASS")
