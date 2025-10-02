import os
from uuid import uuid4


def expand_envs(value: str | None):
    if value is None:
        return None
    for env_key in os.environ.keys():
        value = value.replace("${" + env_key + "}", os.environ[env_key])
    return value


def get_namespace(app_name: str | None):
    app_name = app_name if app_name else str(uuid4())
    return f"celine.{app_name}"
