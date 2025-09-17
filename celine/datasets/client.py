from sqlalchemy import create_engine, Engine, URL, MetaData, Table, inspect
from celine.common.logger import get_logger
from .config import PostgresConfig


class DatasetClient:
    logger = get_logger(__name__)
    engine: Engine | None = None

    def get_engine(self) -> Engine:
        if self.engine is None:
            config = PostgresConfig()
            self.logger.debug(f"Connecting {config.user}@{config.host}:{config.port}")
            try:
                self.engine = create_engine(
                    URL.create(
                        drivername="postgresql+psycopg2",
                        database=config.db,
                        host=config.host,
                        port=config.port,
                        username=config.user,
                        password=config.password,
                        query={},
                    ),
                )
            except Exception as e:
                self.logger.error(f"Failed to setup connection: {e}")
                raise

        return self.engine

    def get_database_schemas(self) -> dict[str, list[str]]:
        """
        Return a dict of { schema_name: [table_names] }
        """
        engine = self.get_engine()
        inspector = inspect(engine)

        schemas = {}
        for schema in inspector.get_schema_names():
            tables = inspector.get_table_names(schema=schema)
            if tables:  # only include if schema has tables
                schemas[schema] = tables

        return schemas

    def get_table_structure(self, schema: str, table: str) -> list[dict]:
        """
        Return the table fields names and types as a list of dicts:
        [{ "name": ..., "type": ..., "nullable": ..., "default": ...}, ...]
        """
        engine = self.get_engine()
        inspector = inspect(engine)

        columns = inspector.get_columns(table, schema=schema)
        return [
            {
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "default": col.get("default"),
            }
            for col in columns
        ]

    def get_model(self, schema: str, table: str) -> Table:
        """
        Return a SQLAlchemy Table object (reflected from DB).
        Can be used with ORM or Core queries.
        """
        engine = self.get_engine()
        metadata = MetaData(schema=schema)
        model = Table(table, metadata, autoload_with=engine)
        return model


if __name__ == "__main__":
    client = DatasetClient()

    # List schemas and tables
    schemas = client.get_database_schemas()
    print("Schemas and tables:", schemas)

    # Example: pick the first schema + table
    if schemas:
        schema, tables = next(iter(schemas.items()))
        table = tables[0]
        print(f"\nInspecting {schema}.{table}...")

        # Get table structure
        structure = client.get_table_structure(schema, table)
        print("Structure:", structure)

        # Get model and fetch rows
        model = client.get_model(schema, table)
        with client.get_engine().connect() as conn:
            result = conn.execute(model.select().limit(5))
            print("\nSample rows:")
            for row in result:
                print(row)
    else:
        print("No tables found in database.")
