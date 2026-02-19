from typing import Optional
from pydantic import Field

from celine.utils.common.config.settings import AppBaseSettings
from celine.sdk.settings import OidcSettings, SdkSettings


class PipelineConfig(AppBaseSettings):
    """Configuration object for data ingestion/transform pipelines."""

    app_name: Optional[str] = Field(default=None, alias="APP_NAME")

    raise_on_failure: Optional[bool] = Field(
        default=True,
        alias="RAISE_ON_FAILURE",
        description="Raise exception when a task fails, resulting in a failed pipeline",
    )

    # Project roots
    meltano_project_root: Optional[str] = Field(
        default=None, alias="MELTANO_PROJECT_ROOT"
    )
    dbt_project_dir: Optional[str] = Field(default=None, alias="DBT_PROJECT_DIR")
    dbt_profiles_dir: Optional[str] = Field(default=None, alias="DBT_PROFILES_DIR")

    # Database
    postgres_host: str = Field(default="172.17.0.1", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=15432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="datasets", alias="POSTGRES_DB")
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(
        default="securepassword123", alias="POSTGRES_PASSWORD"
    )
    meltano_database_uri: str | None = Field(
        default="postgresql://postgres:securepassword123@172.17.0.1:15432/meltano",
        alias="MELTANO_DATABASE_URI",
    )

    openlineage_url: str = Field(
        default="http://172.17.0.1:5000", alias="OPENLINEAGE_URL"
    )
    openlineage_api_key: str | None = Field(default=None, alias="OPENLINEAGE_API_KEY")
    openlineage_enabled: bool = Field(
        default=True,
        alias="OPENLINEAGE_ENABLED",
        description="Enable OpenLineage integration",
    )

    # MQTT pipeline events (via celine-sdk)
    mqtt_events_enabled: bool = Field(
        default=True,
        alias="MQTT_EVENTS_ENABLED",
        description="Enable MQTT pipeline event publishing",
    )

    sdk: SdkSettings = SdkSettings(
        oidc=OidcSettings(
            audience="svc-pipelines",
            client_id="svc-pipelines",
            client_secret="svc-pipelines",
        )
    )

    @staticmethod
    def get_as_envs(cfg: "PipelineConfig") -> dict[str, str]:
        envs: dict[str, str] = {}

        for name, field in PipelineConfig.model_fields.items():
            alias = field.validation_alias

            if alias is None:
                continue

            value = getattr(cfg, name)

            if value is None:
                continue

            if isinstance(alias, (list, tuple)):
                env_key = alias[0]
            else:
                env_key = alias

            envs[str(env_key)] = str(value)

        return envs
