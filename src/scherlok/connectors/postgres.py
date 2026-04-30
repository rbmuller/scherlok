"""PostgreSQL connector using psycopg2."""

from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.extras

from scherlok.connectors.base import BaseConnector


class PostgresConnector(BaseConnector):
    """Connector for PostgreSQL databases."""

    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)
        self._conn: Any = None

    def connect(self) -> bool:
        """Validate and establish connection to PostgreSQL."""
        try:
            self._conn = psycopg2.connect(self.connection_string)
            self._conn.autocommit = True
            return True
        except psycopg2.Error:
            return False

    def _cursor(self) -> Any:
        """Return a dict cursor."""
        return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def list_tables(self) -> list[str]:
        """List all user tables and views in the public schema."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' "
                "AND table_type IN ('BASE TABLE', 'VIEW') "
                "ORDER BY table_name"
            )
            return [row["table_name"] for row in cur.fetchall()]

    def get_row_count(self, table: str) -> int:
        """Return exact row count for a table."""
        with self._cursor() as cur:
            cur.execute(f"SELECT count(*) AS cnt FROM \"{table}\"")  # noqa: S608
            return cur.fetchone()["cnt"]

    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata from information_schema."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s "
                "ORDER BY ordinal_position",
                (table,),
            )
            return [
                {
                    "name": row["column_name"],
                    "type": row["data_type"],
                    "nullable": row["is_nullable"] == "YES",
                }
                for row in cur.fetchall()
            ]

    def get_column_stats(self, table: str, column: str) -> dict:
        """Calculate statistics for a numeric or text column."""
        stats: dict[str, Any] = {}
        with self._cursor() as cur:
            # Null and distinct counts (works for all types)
            cur.execute(
                f"SELECT "  # noqa: S608
                f"  count(*) FILTER (WHERE \"{column}\" IS NULL) AS null_count, "
                f"  count(DISTINCT \"{column}\") AS distinct_count "
                f"FROM \"{table}\""
            )
            row = cur.fetchone()
            stats["null_count"] = row["null_count"]
            stats["distinct_count"] = row["distinct_count"]

            # Numeric stats (mean, stddev, min, max)
            try:
                cur.execute(
                    f"SELECT "  # noqa: S608
                    f"  avg(\"{column}\")::float AS mean, "
                    f"  stddev(\"{column}\")::float AS stddev, "
                    f"  min(\"{column}\")::text AS min, "
                    f"  max(\"{column}\")::text AS max "
                    f"FROM \"{table}\""
                )
                num_row = cur.fetchone()
                stats["mean"] = num_row["mean"]
                stats["stddev"] = num_row["stddev"]
                stats["min"] = num_row["min"]
                stats["max"] = num_row["max"]
            except psycopg2.Error:
                stats["mean"] = None
                stats["stddev"] = None
                stats["min"] = None
                stats["max"] = None

            # Top values
            try:
                cur.execute(
                    f"SELECT \"{column}\"::text AS val, count(*) AS cnt "  # noqa: S608
                    f"FROM \"{table}\" "
                    f"WHERE \"{column}\" IS NOT NULL "
                    f"GROUP BY \"{column}\" ORDER BY cnt DESC LIMIT 5"
                )
                stats["top_values"] = [
                    {"value": r["val"], "count": r["cnt"]} for r in cur.fetchall()
                ]
            except psycopg2.Error:
                stats["top_values"] = []

        return stats

    def get_last_modified(self, table: str) -> datetime | None:
        """Return last known modification time from pg_stat_user_tables."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT GREATEST(last_autovacuum, last_autoanalyze, "
                "last_vacuum, last_analyze) AS last_mod "
                "FROM pg_stat_user_tables WHERE relname = %s",
                (table,),
            )
            row = cur.fetchone()
            if row and row["last_mod"]:
                return row["last_mod"]
            return None
