import uuid
import datetime
from typing import Generator, Type
from celine.common import model

_PY_TO_SQL_TYPE = {
    datetime.datetime: "TIME(6)",
    float: "DOUBLE",
    int: "INTEGER",
    str: "VARCHAR",
    uuid.UUID: "UUID",
}


def _get_catalog_schema_and_table(
    record_or_class: model.BaseRecord | Type[model.BaseRecord],
) -> tuple[str, str, str]:
    if isinstance(record_or_class, model.BaseRecord):
        catalog = record_or_class.catalog
        schema = record_or_class.schema
        table = record_or_class.table
    else:
        fields = record_or_class.model_fields
        catalog = fields.get("catalog").default
        schema = fields.get("schema").default
        table = fields.get("table").default
    return catalog, schema, table


def build_table_name(record_or_class: model.BaseRecord | Type[model.BaseRecord]) -> str:
    """Build the SQL table name"""
    catalog, schema, table = _get_catalog_schema_and_table(record_or_class)
    assert catalog and schema and table, "catalog, schema and table must be specified"
    return f"{catalog}.{schema}.{table}"


def build_create_schema(
    record_or_class: model.BaseRecord | Type[model.BaseRecord],
) -> str:
    """Build the `CREATE SCHEMA` statement"""
    catalog, schema, _ = _get_catalog_schema_and_table(record_or_class)
    assert catalog and schema, "catalog and schema must be specified"
    return f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} WITH (location = 's3://warehouse/{schema}/')"


def build_create_table(
    record_or_class: model.BaseRecord | Type[model.BaseRecord],
) -> str:
    """Build the `CREATE TABLE` statement. Only common fields are included"""
    fields = [
        f"{field_name} {_PY_TO_SQL_TYPE[field_type]}"
        for field_name, field_type in record_or_class.get_common_fields().items()
    ]
    fields = ",\n  ".join(fields)
    _, schema, table = _get_catalog_schema_and_table(record_or_class)
    return f"""CREATE TABLE IF NOT EXISTS {build_table_name(record_or_class)} (
        {fields}
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['_created'],
        location = 's3a://warehouse/{schema}/{table}'
    )"""


def build_alter_table(
    record_or_class: model.BaseRecord | Type[model.BaseRecord],
    custom_sql_types: dict[str, str] | None = None,
) -> Generator[str, None, None]:
    """Build `ALTER TABLE` statements for data fields

    Use `custom_sql_types` to override default Python-to-SQL type mapping for specific field names
    """
    custom_sql_types = custom_sql_types or {}
    table_name = build_table_name(record_or_class)
    for field_name, field_type in record_or_class.get_all_fields().items():
        sql_type = custom_sql_types.get(field_name) or _PY_TO_SQL_TYPE[field_type]
        yield f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {field_name} {sql_type}"


def build_insert(records: model.BaseRecord | list[model.BaseRecord]) -> str:
    """Build `INSERT` statement"""
    if not isinstance(records, list):
        records = [records]
    first_record = records[0]
    fields = first_record.get_all_fields()
    table_name = build_table_name(first_record)
    col_names = ", ".join(fields.keys())

    rows = []
    for record in records:
        values = []
        for name, field_type in fields.items():
            if name.startswith("_"):
                name = name[1:]
            if not (value := getattr(record, name, None)):
                continue
            if field_type in (str, datetime.datetime, uuid.UUID):
                value = f"'{value}'"
            values.append(str(value))
        rows.append(f'({", ".join(values)})')
    rows_str = ",\n  ".join(rows)
    return f"INSERT INTO {table_name} ({col_names})\nVALUES ({rows_str})"


def build_select(
    record_class: Type[model.BaseRecord],
    *,
    fields: list[str] | None = None,
    where: str = "",
) -> str:
    """Build `SELECT` statement

    Optionally you might specify a subset of fields and where condition.
    """
    fields = fields or list(record_class.get_all_fields().keys())
    fields = ",\n  ".join(fields)
    table_name = build_table_name(record_class)
    if where:
        where = f"\nWHERE\n  {where}"
    return f"SELECT\n  {fields}\nFROM\n  {table_name}{where}"
