import importlib.metadata
from uuid import uuid4

OPENLINEAGE_CLIENT_VERSION = importlib.metadata.version("openlineage-python")
TASK_RESULT_SUCCESS = "success"
TASK_RESULT_FAILED = "failed"
PRODUCER = "https://github.com/celine-eu/celine-utils"
VERSION = "v1.0.0"


def get_namespace(app_name: str | None):
    app_name = app_name if app_name else str(uuid4())
    return f"celine.{app_name}"
