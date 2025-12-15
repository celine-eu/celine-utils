from typing import Optional
from pydantic import Field

from celine.common.config.settings import AppBaseSettings


class PipelineConfig(AppBaseSettings):
    """Configuration object for data ingestion/transform pipelines."""

    app_name: Optional[str] = Field(default=None, alias="APP_NAME")

    # Project roots
    meltano_project_root: Optional[str] = Field(
        default=None, alias="MELTANO_PROJECT_ROOT"
    )
    dbt_project_dir: Optional[str] = Field(default=None, alias="DBT_PROJECT_DIR")
    dbt_profiles_dir: Optional[str] = Field(default=None, alias="DBT_PROFILES_DIR")

    # Database
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="datasets", alias="POSTGRES_DB")
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(default="postgres", alias="POSTGRES_PASSWORD")

    meltano_database_uri: str | None = Field(default=None, alias="MELTANO_DATABASE_URI")

    openlineage_url: str = Field(
        default="http://marquez-api:5000", alias="OPENLINEAGE_URL"
    )
    openlineage_api_key: str | None = Field(default=None, alias="OPENLINEAGE_API_KEY")
    openlineage_enabled: bool = Field(
        default=True,
        alias="OPENLINEAGE_ENABLED",
        description="Enable OpenLineage integration",
    )
