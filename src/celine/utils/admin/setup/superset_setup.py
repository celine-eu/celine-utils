from celine.utils.common.superset import SupersetClient
from celine.utils.common.logger import get_logger

logger = get_logger(__name__)


def add_datasources():
    # Reads config from env or .env
    superset_client = SupersetClient()
    logger.debug(f"Databases: {superset_client.list_databases()}")

    logger.debug(f"Not implemented")


def setup():
    add_datasources()


if __name__ == "__main__":
    setup()
