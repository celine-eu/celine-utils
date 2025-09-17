from pydantic import Field
from celine.common.config.settings import AppBaseSettings


class PostgresConfig(AppBaseSettings):
    host: str = Field(default="datasets-db", alias="POSTGRES_HOST")
    port: int = Field(default=5432, alias="POSTGRES_PORT")
    user: str = Field(default="postgres", alias="POSTGRES_USER")
    password: str | None = Field(default=None, alias="POSTGRES_PASSWORD")
    db: str = Field(default="datasets", alias="POSTGRES_DB")
