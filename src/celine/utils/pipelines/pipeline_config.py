from typing import Optional
from pydantic import Field

from celine.utils.common.config.settings import AppBaseSettings


def _default_sdk_settings():
    """Lazy import to avoid hard dependency on celine-sdk."""
    try:
        from celine.sdk.settings import SdkSettings

        return SdkSettings()
    except ImportError:
        return None


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
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="datasets", alias="POSTGRES_DB")
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(default="postgres", alias="POSTGRES_PASSWORD")

    meltano_database_uri: str | None = Field(default=None, alias="MELTANO_DATABASE_URI")

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
        default=False,
        alias="MQTT_EVENTS_ENABLED",
        description="Enable MQTT pipeline event publishing",
    )

    @property
    def sdk(self):
        """
        Access celine-sdk settings (MQTT, OIDC).

        Returns None if celine-sdk is not installed.
        Settings are loaded from CELINE_MQTT_* and CELINE_OIDC_* env vars.
        """
        if not hasattr(self, "_sdk"):
            self._sdk = _default_sdk_settings()
        return self._sdk
