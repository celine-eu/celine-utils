from __future__ import annotations

"""
DuckDB-based RAW cold-storage archiver.

- Discovers tables from a small YAML or dbt manifest.json (or both; YAML wins).
- Archives per-day partitions (default key: `_sdc_extracted_at` in UTC) to Parquet on S3/MinIO.
- Validates rowcounts, deletes from source, and writes a local `archive_manifest` table.
- Designed for ~10M rows scale (no Spark needed).

Package path: celine.pipelines.storage -> archiver.py
"""

import json
import textwrap
from pathlib import Path
from typing import List, Optional, Sequence

import duckdb

import yaml

from pydantic import Field, ValidationError
from typing_extensions import Literal
from pydantic_settings import SettingsConfigDict

from celine.common.logger import get_logger
from celine.common.config.settings import AppBaseSettings
from celine.datasets.config import PostgresConfig


logger = get_logger(__name__)


# -----------------------------
# Config models
# -----------------------------


class TableConfig(AppBaseSettings):
    """Per-table settings.

    If `date_column` is None, the archiver will fall back to the global
    `default_date_column` from ArchiverConfig (typically `_sdc_extracted_at`).

    Note: field `schema` would clash with BaseModel.schema(). We expose
    `table_schema` as the attribute name and alias it to `schema` for
    YAML/manifest compatibility.
    """

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")

    table_schema: str = Field(default="public", alias="schema")
    name: str
    date_column: Optional[str] = None
    retention_days: Optional[int] = None


class ArchiverConfig(AppBaseSettings):
    """Top-level configuration for the Archiver.

    You can load from env or pass values directly. For YAML-driven configs,
    point `config_file` to your cold_storage.yml.
    """

    # Data lake
    bucket: str = Field(default="datalake", alias="ARCHIVE_BUCKET")
    s3_endpoint: Optional[str] = Field(default=None, alias="S3_ENDPOINT")
    s3_use_ssl: bool = Field(default=True, alias="S3_USE_SSL")
    s3_region: str = Field(default="us-east-1", alias="S3_REGION")
    s3_access_key: Optional[str] = Field(default=None, alias="S3_ACCESS_KEY")
    s3_secret_key: Optional[str] = Field(default=None, alias="S3_SECRET_KEY")

    # Archival behavior
    compression: Literal["ZSTD", "Snappy"] = Field(default="ZSTD")
    retention_days: int = Field(default=90)
    extraction_timezone: str = Field(default="UTC")
    default_date_column: str = Field(default="_sdc_extracted_at")
    archive_prefix: str = Field(default="archive/raw")

    # Discovery inputs
    config_file: Optional[Path] = None  # YAML file with defaults/tables
    manifest_path: Optional[Path] = None  # dbt target/manifest.json

    # Control
    dry_run: bool = False

    # Tables
    tables: List[TableConfig] = Field(default_factory=list)

    # Postgres connection
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)


# -----------------------------
# Exceptions
# -----------------------------


class ArchiverError(Exception):
    """Base error for archiver failures."""


class ConfigurationError(ArchiverError):
    pass


class ExportError(ArchiverError):
    pass


class ValidationMismatchError(ArchiverError):
    pass


# -----------------------------
# Archiver class
# -----------------------------


class DuckDBArchiver:
    """Archive RAW tables to Parquet on S3/MinIO using DuckDB.

    Typical usage:

        cfg = ArchiverConfig(config_file=Path("cold_storage.yml"), dry_run=False)
        archiver = DuckDBArchiver(cfg)
        archiver.run()
    """

    def __init__(self, cfg: ArchiverConfig) -> None:
        self.cfg = cfg
        self._con: Optional[duckdb.DuckDBPyConnection] = None

    # ---------- public API ----------

    def run(self) -> None:
        """Entrypoint to execute the archival process for all configured tables."""
        try:
            self._load_yaml_if_any()
            discovered = self._discover_from_manifest_if_any()
            self._merge_tables(discovered)
            self._validate_tables()

            with self._connect_duckdb() as con:
                self._con = con
                self._prepare_duckdb(con)
                self._ensure_manifest_table(con)

                for t in self.cfg.tables:
                    self._archive_table(con, t)

        except Exception as exc:  # consolidate logging
            logger.exception("Archiver failed: %s", exc)
            raise
        finally:
            self._con = None

    # ---------- setup & discovery ----------

    def _load_yaml_if_any(self) -> None:
        """Load tables/defaults from YAML if `config_file` is set.

        YAML keys supported:
          defaults: { bucket, s3_endpoint, s3_use_ssl, compression, retention_days,
                      extraction_timezone, default_date_column }
          tables: [ {schema, name, date_column?, retention_days?}, ... ]
        """
        if not self.cfg.config_file:
            return

        path = self.cfg.config_file
        if not path.exists():
            raise ConfigurationError(f"YAML config not found: {path}")

        logger.info("Loading YAML config from %s", path)
        with path.open("r") as fh:
            data = yaml.safe_load(fh) or {}

        defaults = data.get("defaults", {}) or {}
        tables = data.get("tables", []) or []

        # Update top-level cfg defaults from YAML (only supported keys)
        for key in (
            "bucket",
            "s3_endpoint",
            "s3_use_ssl",
            "compression",
            "retention_days",
            "extraction_timezone",
            "default_date_column",
        ):
            if key in defaults and getattr(self.cfg, key, None) != defaults[key]:
                logger.debug("Overriding cfg.%s from YAML -> %r", key, defaults[key])
                setattr(self.cfg, key, defaults[key])

        # Load/extend tables
        yaml_tables: List[TableConfig] = []
        for raw in tables:
            try:
                yaml_tables.append(TableConfig(**raw))
            except ValidationError as ve:  # pragma: no cover
                raise ConfigurationError(f"Invalid table entry in YAML: {raw}\n{ve}")
        # Keep existing cfg.tables (e.g., env/constructor) and extend
        self.cfg.tables.extend(yaml_tables)

    def _discover_from_manifest_if_any(self) -> List[TableConfig]:
        if not self.cfg.manifest_path:
            return []
        mp = self.cfg.manifest_path
        if not mp.exists():
            raise ConfigurationError(f"dbt manifest not found: {mp}")

        logger.info("Reading dbt manifest: %s", mp)
        with mp.open("r") as fh:
            manifest = json.load(fh)

        out: List[TableConfig] = []
        for node in manifest.get("nodes", {}).values():
            if node.get("resource_type") not in {"model", "seed", "source"}:
                continue
            tags = set(node.get("tags", []) or [])
            meta = node.get("meta", {}) or {}
            cold = meta.get("cold_storage", {}) or {}
            if ("cold_archive" in tags) or cold:
                rel_name = node.get("alias") or node.get("name")
                schema_ = (
                    node.get("schema") or node.get("fqn", ["", "", ""])[-2] or "public"
                )
                out.append(
                    TableConfig(
                        schema=schema_,
                        name=rel_name,
                        date_column=cold.get("date_column"),
                        retention_days=cold.get("retention_days"),
                    )
                )
        return out

    def _merge_tables(self, discovered: Sequence[TableConfig]) -> None:
        """Merge discovered tables with cfg.tables; per-table YAML wins on conflicts."""
        # map by fully qualified name
        merged: dict[str, TableConfig] = {}
        for t in discovered:
            fq = f"{t.table_schema}.{t.name}"
            merged[fq] = t
        for t in self.cfg.tables:
            fq = f"{t.table_schema}.{t.name}"
            # YAML/explicit overwrites discovered
            merged[fq] = t
        self.cfg.tables = list(merged.values())
        logger.info(
            "Tables to process: %s",
            ", ".join(f"{t.table_schema}.{t.name}" for t in self.cfg.tables)
            or "<none>",
        )

    def _validate_tables(self) -> None:
        if not self.cfg.tables:
            raise ConfigurationError("No tables configured or discovered for archiving")

    # ---------- duckdb / postgres plumbing ----------

    def _connect_duckdb(self) -> duckdb.DuckDBPyConnection:
        logger.debug("Connecting DuckDB and attaching Postgres")
        con = duckdb.connect()
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # S3 creds
        if self.cfg.s3_access_key:
            con.execute(f"SET s3_access_key_id='{self.cfg.s3_access_key}'")
        if self.cfg.s3_secret_key:
            con.execute(f"SET s3_secret_access_key='{self.cfg.s3_secret_key}'")
        con.execute(f"SET s3_region='{self.cfg.s3_region}'")
        if self.cfg.s3_endpoint:
            con.execute(f"SET s3_endpoint='{self.cfg.s3_endpoint}'")
            con.execute(f"SET s3_use_ssl={'true' if self.cfg.s3_use_ssl else 'false'}")

        dsn = self._build_pg_dsn(self.cfg.postgres)
        con.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES);")
        return con

    @staticmethod
    def _prepare_duckdb(con: duckdb.DuckDBPyConnection) -> None:
        """Any per-session settings go here."""
        # Currently none; placeholder for e.g., threads config.
        pass

    @staticmethod
    def _build_pg_dsn(pg: PostgresConfig) -> str:
        parts = [
            f"host={pg.host}",
            f"dbname={pg.db}",
            f"user={pg.user}",
            f"port={pg.port}",
        ]
        if pg.password:
            parts.append(f"password={pg.password}")
        return " ".join(parts)

    # ---------- core logic ----------

    def _resolve_date_column(
        self, con: duckdb.DuckDBPyConnection, t: TableConfig
    ) -> str:
        if t.date_column:
            return t.date_column
        default_col = self.cfg.default_date_column
        exists = con.execute(
            f"""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='{t.table_schema}' AND table_name='{t.name}' AND column_name='{default_col}'
            LIMIT 1
            """
        ).fetchone()
        if exists:
            return default_col
        raise ConfigurationError(
            f"{t.table_schema}.{t.name}: no date_column specified and default '{default_col}' not present"
        )

    @staticmethod
    def _date_expr(col: str, tz: str) -> str:
        # Normalize to a date in the given timezone
        return f"DATE(({col} AT TIME ZONE '{tz}'))"

    def _eligible_partitions(
        self,
        con: duckdb.DuckDBPyConnection,
        schema: str,
        table: str,
        dt_expr: str,
        cutoff: str,
    ) -> List[str]:
        q = f"""
          SELECT DISTINCT {dt_expr} AS dt
          FROM pg.{schema}.{table}
          WHERE {dt_expr} < DATE '{cutoff}'
          ORDER BY 1
        """
        return [str(r[0]) for r in con.execute(q).fetchall()]

    @staticmethod
    def _parquet_rowcount(con: duckdb.DuckDBPyConnection, s3_prefix: str) -> int:
        res = con.execute(
            f"SELECT SUM(rows) FROM parquet_metadata('{s3_prefix}')"
        ).fetchone()
        v = res[0] if res else 0
        return int(v or 0)

    @staticmethod
    def _ensure_manifest_table(con: duckdb.DuckDBPyConnection) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS archive_manifest (
              table_schema TEXT,
              table_name   TEXT,
              partition_date DATE,
              row_count    BIGINT,
              s3_prefix    TEXT,
              archived_at  TIMESTAMPTZ DEFAULT now(),
              PRIMARY KEY (table_schema, table_name, partition_date)
            )
            """
        )

    @staticmethod
    def _already_archived(
        con: duckdb.DuckDBPyConnection, schema: str, table: str, dt_value: str
    ) -> bool:
        q = "SELECT 1 FROM archive_manifest WHERE table_schema=? AND table_name=? AND partition_date=? LIMIT 1"
        return con.execute(q, [schema, table, dt_value]).fetchone() is not None

    def _archive_table(self, con: duckdb.DuckDBPyConnection, t: TableConfig) -> None:
        date_col = self._resolve_date_column(con, t)
        tz = self.cfg.extraction_timezone
        dt_expr = self._date_expr(date_col, tz)
        retention = t.retention_days or self.cfg.retention_days

        cutoff = self._utc_today_minus(retention)
        base_prefix = f"s3://{self.cfg.bucket}/{self.cfg.archive_prefix}/{t.name}"

        logger.info(
            "Archiving %s.%s where %s < %s (retention=%sd)",
            t.table_schema,
            t.name,
            dt_expr,
            cutoff,
            retention,
        )

        parts = self._eligible_partitions(con, t.table_schema, t.name, dt_expr, cutoff)
        if not parts:
            logger.info("%s.%s: no eligible partitions", t.table_schema, t.name)
            return

        for part_dt in parts:
            s3_prefix = f"{base_prefix}/dt={part_dt}/"
            if self._already_archived(con, t.table_schema, t.name, part_dt):
                logger.debug(
                    "%s.%s %s already archived; skipping",
                    t.table_schema,
                    t.name,
                    part_dt,
                )
                continue

            # Counts in source
            src_count_q = f"SELECT COUNT(*) FROM pg.{t.table_schema}.{t.name} WHERE {dt_expr} = DATE '{part_dt}'"

            res = con.execute(src_count_q).fetchone()
            src_count = int(res[0]) if res else 0
            if src_count == 0:
                logger.info(
                    "%s.%s %s has zero rows; recording manifest and continuing",
                    t.table_schema,
                    t.name,
                    part_dt,
                )
                if not self.cfg.dry_run:
                    self._insert_manifest(
                        con, t.table_schema, t.name, part_dt, 0, s3_prefix
                    )
                continue

            export_sql = textwrap.dedent(
                f"""
                COPY (
                  SELECT * FROM pg.{t.table_schema}.{t.name}
                  WHERE {dt_expr} = DATE '{part_dt}'
                )
                TO '{s3_prefix}'
                (FORMAT PARQUET, COMPRESSION '{self.cfg.compression}', OVERWRITE_OR_IGNORE 1);
                """
            )

            logger.info(
                "%s.%s %s exporting %d rows -> %s",
                t.table_schema,
                t.name,
                part_dt,
                src_count,
                s3_prefix,
            )
            if not self.cfg.dry_run:
                try:
                    con.execute(export_sql)
                except Exception as e:  # pragma: no cover
                    raise ExportError(
                        f"Export failed for {t.table_schema}.{t.name} {part_dt}: {e}"
                    ) from e

                dst_count = self._parquet_rowcount(con, s3_prefix)
                if dst_count != src_count:
                    raise ValidationMismatchError(
                        f"Rowcount mismatch for {t.table_schema}.{t.name} {part_dt}: src={src_count} dst={dst_count}"
                    )

                # Delete from source and vacuum
                del_sql = textwrap.dedent(
                    f"""
                    CALL postgres_query('pg', $$
                      DELETE FROM {t.table_schema}.{t.name}
                      WHERE {dt_expr} = DATE '{part_dt}';
                      VACUUM ANALYZE {t.table_schema}.{t.name};
                    $$);
                    """
                )
                logger.debug(
                    "Deleting source rows for %s.%s %s", t.table_schema, t.name, part_dt
                )
                con.execute(del_sql)

                self._insert_manifest(
                    con, t.table_schema, t.name, part_dt, dst_count, s3_prefix
                )
            else:
                logger.info(
                    "(dry-run) would export, delete and manifest %s.%s %s",
                    t.table_schema,
                    t.name,
                    part_dt,
                )

    @staticmethod
    def _insert_manifest(
        con: duckdb.DuckDBPyConnection,
        schema: str,
        table: str,
        dt_value: str,
        row_count: int,
        s3_prefix: str,
    ) -> None:
        con.execute(
            """
            INSERT INTO archive_manifest (table_schema, table_name, partition_date, row_count, s3_prefix)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [schema, table, dt_value, int(row_count), s3_prefix],
        )

    @staticmethod
    def _utc_today_minus(days: int) -> str:
        from datetime import datetime, timedelta, timezone

        return (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()


# -----------------------------
# CLI helper (optional)
# -----------------------------

if __name__ == "__main__":  # pragma: no cover
    import argparse

    p = argparse.ArgumentParser(description="Archive RAW tables to S3/MinIO via DuckDB")
    p.add_argument(
        "--config-file", type=Path, help="Path to cold_storage.yml", default=None
    )
    p.add_argument(
        "--manifest-path", type=Path, help="Path to dbt manifest.json", default=None
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    cfg = ArchiverConfig(
        config_file=args.config_file,
        manifest_path=args.manifest_path,
        dry_run=args.dry_run,
    )
    DuckDBArchiver(cfg).run()
