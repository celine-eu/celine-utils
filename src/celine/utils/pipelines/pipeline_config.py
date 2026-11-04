import os
from typing import Annotated, Optional

from celine.sdk.posture import PostureGuard
from celine.sdk.settings import OidcSettings, SdkSettings
from pydantic import Field, field_validator, model_validator
from pydantic_settings import NoDecode

from celine.utils.common.config.settings import AppBaseSettings

#: The OIDC client every pipeline authenticates as (MQTT run events).
PIPELINES_CLIENT_ID = "svc-pipelines"


def _default_sdk_settings() -> SdkSettings:
    """SDK settings read when the config is built, not when this module is imported.

    The client secret falls back to the client id — the local realm's dev
    default. :meth:`PipelineConfig._check_posture` refuses that fallback outside
    ``CELINE_ENV=dev``.
    """
    return SdkSettings(
        oidc=OidcSettings(
            audience=PIPELINES_CLIENT_ID,
            client_id=PIPELINES_CLIENT_ID,
            client_secret=os.getenv("CELINE_OIDC_CLIENT_SECRET", PIPELINES_CLIENT_ID),
        )
    )


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
    postgres_host: str = Field(default="host.docker.internal", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=15432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="datasets", alias="POSTGRES_DB")
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(
        default="securepassword123", alias="POSTGRES_PASSWORD"
    )
    meltano_database_uri: str | None = Field(
        default="postgresql://postgres:securepassword123@host.docker.internal:15432/meltano",
        alias="MELTANO_DATABASE_URI",
    )

    openlineage_url: str = Field(
        default="http://host.docker.internal:5003", alias="OPENLINEAGE_URL"
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

    # Run events over HTTP: one relay per environment, no token (the relays'
    # boundary is the network). Set, this replaces the MQTT transport.
    run_events_urls: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        alias="RUN_EVENTS_URLS",
        description="Comma-separated relay URLs each run event is POSTed to",
    )
    run_events_timeout: float = Field(
        default=5.0,
        alias="RUN_EVENTS_TIMEOUT",
        description="Seconds per POST attempt to one relay (one retry follows a failure)",
    )

    sdk: SdkSettings = Field(default_factory=_default_sdk_settings)

    @field_validator("run_events_urls", mode="before")
    @classmethod
    def _split_run_events_urls(cls, value: object) -> object:
        """Read ``RUN_EVENTS_URLS`` as a comma-separated list of http(s) URLs.

        An empty value means no URL. An empty item or a URL that is not
        http(s) is refused, so a typo fails at start rather than losing every
        event in silence.
        """
        if value is None:
            return []
        if isinstance(value, str):
            if not value.strip():
                return []
            value = [item.strip() for item in value.split(",")]
        urls = list(value)  # type: ignore[arg-type]
        for url in urls:
            if not url or not str(url).startswith(("http://", "https://")):
                raise ValueError(
                    f"RUN_EVENTS_URLS: {url!r} is not an http(s) URL "
                    "(expected a comma-separated list, no empty items)"
                )
        return urls

    @model_validator(mode="after")
    def _check_posture(self) -> "PipelineConfig":
        """Refuse a client secret that is empty or equal to the client id outside dev.

        Only ``CELINE_ENV=dev`` (then ``ENVIRONMENT``; ``celine.sdk.posture``)
        accepts it, with a warning. Anywhere else — unset included — building the
        config raises :class:`celine.sdk.posture.InsecureConfiguration`, before a
        pipeline authenticates with a credential derivable from its client id.
        ``PREFECT_MODE`` is a scheduling switch and plays no part in this.
        """
        oidc = self.sdk.oidc if self.sdk is not None else None
        if oidc is None:
            return self
        guard = PostureGuard("celine-utils pipelines")
        guard.forbid_secret_equal_to_client_id(
            "CELINE_OIDC_CLIENT_SECRET",
            oidc.client_id,
            oidc.client_secret,
            remediation=(
                f"Export CELINE_OIDC_CLIENT_SECRET with the {oidc.client_id} client's "
                "real secret, or CELINE_ENV=dev on a local stack."
            ),
        )
        guard.enforce()
        return self

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

            if isinstance(value, list):
                if not value:
                    continue
                value = ",".join(str(v) for v in value)

            if isinstance(alias, (list, tuple)):
                env_key = alias[0]
            else:
                env_key = alias

            envs[str(env_key)] = str(value)

        return envs
