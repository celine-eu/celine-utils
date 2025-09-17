from sqlalchemy import create_engine, Engine, URL, MetaData, Table, inspect, select, Row
from sqlalchemy.sql import Select
import pandas as pd
from celine.common.logger import get_logger
from .config import PostgresConfig
from typing import Sequence, Any


class ExecSelect:
    """
    Wrapper around SQLAlchemy Select with bound engine.
    """

    def __init__(self, engine: Engine, stmt: Select):
        self._engine = engine
        self._stmt = stmt

    def where(self, *criteria) -> "ExecSelect":
        return ExecSelect(self._engine, self._stmt.where(*criteria))

    def order_by(self, *clauses) -> "ExecSelect":
        return ExecSelect(self._engine, self._stmt.order_by(*clauses))

    def limit(self, n: int) -> "ExecSelect":
        return ExecSelect(self._engine, self._stmt.limit(n))

    def offset(self, n: int) -> "ExecSelect":
        return ExecSelect(self._engine, self._stmt.offset(n))

    def exec(self) -> Sequence[Row[Any]]:
        with self._engine.connect() as conn:
            result = conn.execute(self._stmt)
            return result.fetchall()

    def to_dataframe(self) -> pd.DataFrame:
        with self._engine.connect() as conn:
            result = conn.execute(self._stmt)
            rows = result.fetchall()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame([dict(r._mapping) for r in rows])


class DatasetClient:
    logger = get_logger(__name__)
    engine: Engine | None = None

    # ---------- Engine ----------
    def get_engine(self) -> Engine:
        if self.engine is None:
            config = PostgresConfig()
            self.logger.debug(f"Connecting {config.user}@{config.host}:{config.port}")
            self.engine = create_engine(
                URL.create(
                    drivername="postgresql+psycopg2",
                    database=config.db,
                    host=config.host,
                    port=config.port,
                    username=config.user,
                    password=config.password,
                )
            )
        return self.engine

    # ---------- Introspection ----------
    def get_database_schemas(self) -> dict[str, list[str]]:
        inspector = inspect(self.get_engine())
        schemas = {}
        for schema in inspector.get_schema_names():
            tables = inspector.get_table_names(schema=schema)
            if tables:
                schemas[schema] = tables
        return schemas

    def get_table_structure(self, schema: str, table: str) -> list[dict]:
        inspector = inspect(self.get_engine())
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
        metadata = MetaData(schema=schema)
        return Table(table, metadata, autoload_with=self.get_engine())

    # ---------- Query Builder ----------
    def select(self, schema: str, table: str) -> ExecSelect:
        model = self.get_model(schema, table)
        stmt = select(model)
        return ExecSelect(self.get_engine(), stmt)
