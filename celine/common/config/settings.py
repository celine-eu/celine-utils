from pydantic_settings import BaseSettings, SettingsConfigDict

env_files = (
    ".env",
    ".env.dev",
    ".env.prod",
)


class AppBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=env_files, extra="ignore")
