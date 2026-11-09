"""PostgreSQL connector using psycopg2."""

from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

import psycopg2
import psycopg2.extras
import psycopg2.sql

from scherlok.connectors.base import BaseConnector, RelationNamespace, quote_ansi_identifier

DEFAULT_SCHEMA = "public"


class PostgresConnector(BaseConnector):
    """Connector for PostgreSQL databases."""

    # pg_stat_user_tables only knows vacuum and analyze times, which move after
    # enough rows change, not on every write (#139).
    METADATA_IS_LAST_WRITE = False

    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)
        self._conn: Any = None
        self._dsn, self._schema = split_schema(connection_string)

    @property
    def schema(self) -> str:
        """The schema whose tables are listed and profiled."""
        return self._schema or DEFAULT_SCHEMA

    def connect(self) -> bool:
        """Validate and establish connection to PostgreSQL.

        A schema in the connection string (``postgresql://host/db/schema``)
        becomes the session's ``search_path``, so every unqualified table name
        resolves there.
        """
        try:
            self._conn = psycopg2.connect(self._dsn)
            self._conn.autocommit = True
            if self._schema is not None:
                with self._cursor() as cur:
                    cur.execute(
                        psycopg2.sql.SQL("SET search_path TO {}").format(
                            psycopg2.sql.Identifier(self._schema)
                        )
                    )
            return True
        except psycopg2.Error as exc:
            self._last_error = self._classify_error(str(exc))
            return False

    @staticmethod
    def _classify_error(message: str) -> str:
        """Map psycopg2 error text to a short, actionable hint."""
        msg = message.strip()
        lowered = msg.lower()
        if "could not connect to server" in lowered or "connection refused" in lowered:
            return (
                "connection refused — is the server reachable?\n"
                "  Hint: docker compose ps  /  pg_isready -h <host> -p <port>"
            )
        if "password authentication failed" in lowered or "authentication failed" in lowered:
            return "wrong username or password — check the credentials in your connection string"
        if "does not exist" in lowered and "database" in lowered:
            return (
                "database does not exist on the server\n"
                "  Hint: list databases with `psql -l -h <host> -p <port>`"
            )
        if "ssl" in lowered and "required" in lowered:
            return (
                "server requires SSL\n"
                "  Hint: append `?sslmode=require` to the connection string"
            )
        if "timeout" in lowered:
            return "connection timed out — check the host/port and any firewall in front"
        if "no such host" in lowered or "name or service not known" in lowered:
            return "host not found — check the hostname in your connection string"
        # Fallback: the raw message, trimmed and de-newlined for readability
        first_line = msg.splitlines()[0] if msg else "unknown error"
        return f"{first_line}"

    def _cursor(self) -> Any:
        """Return a dict cursor."""
        return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote a PostgreSQL identifier, doubling any embedded double quote."""
        return quote_ansi_identifier(identifier)

    def _fetch_one(self, sql: str) -> dict[str, Any]:
        """Run ``sql`` and return its single row keyed by column alias."""
        with self._cursor() as cur:
            cur.execute(sql)
            return dict(cur.fetchone())

    def namespace(self) -> RelationNamespace:
        """The database of the connection string and the schema it names, or public."""
        return RelationNamespace(database=database_name(self._dsn), schema=self.schema)

    def list_tables(self) -> list[str]:
        """List all user tables and views in the connection's schema (public by default)."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s "
                "AND table_type IN ('BASE TABLE', 'VIEW') "
                "ORDER BY table_name",
                (self.schema,),
            )
            return [row["table_name"] for row in cur.fetchall()]

    def get_row_count(self, table: str) -> int:
        """Return exact row count for a table."""
        quoted_table = self._quote_identifier(table)
        with self._cursor() as cur:
            cur.execute(f"SELECT count(*) AS cnt FROM {quoted_table}")  # noqa: S608
            return cur.fetchone()["cnt"]

    def get_columns(self, table: str) -> list[dict]:
        """Return column metadata from information_schema."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (self.schema, table),
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
        quoted_table = self._quote_identifier(table)
        quoted_column = self._quote_identifier(column)
        stats: dict[str, Any] = {}
        with self._cursor() as cur:
            # Null and distinct counts (works for all types)
            cur.execute(
                f"SELECT "  # noqa: S608
                f"  count(*) FILTER (WHERE {quoted_column} IS NULL) AS null_count, "
                f"  count(DISTINCT {quoted_column}) AS distinct_count "
                f"FROM {quoted_table}"
            )
            row = cur.fetchone()
            stats["null_count"] = row["null_count"]
            stats["distinct_count"] = row["distinct_count"]

            # Numeric stats (mean, stddev)
            try:
                cur.execute(
                    f"SELECT "  # noqa: S608
                    f"  avg({quoted_column})::float AS mean, "
                    f"  stddev({quoted_column})::float AS stddev "
                    f"FROM {quoted_table}"
                )
                num_row = cur.fetchone()
                stats["mean"] = num_row["mean"]
                stats["stddev"] = num_row["stddev"]
            except psycopg2.Error:
                stats["mean"] = None
                stats["stddev"] = None

        return stats

    def get_last_modified(self, table: str) -> datetime | None:
        """Return last known modification time from pg_stat_user_tables."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT GREATEST(last_autovacuum, last_autoanalyze, "
                "last_vacuum, last_analyze) AS last_mod "
                "FROM pg_stat_user_tables WHERE schemaname = %s AND relname = %s",
                (self.schema, table),
            )
            row = cur.fetchone()
            if row and row["last_mod"]:
                return row["last_mod"]
            return None


def split_schema(connection_string: str) -> tuple[str, str | None]:
    """Split ``postgresql://host/db/schema`` into the DSN libpq reads and the schema.

    Anything else, including a key/value DSN, is returned unchanged with no
    schema.
    """
    if "://" not in connection_string:
        return connection_string, None
    parts = urlsplit(connection_string)
    segments = parts.path.strip("/").split("/")
    if len(segments) != 2 or not all(segments) or "@" in parts.path:
        return connection_string, None
    database, schema = segments
    return urlunsplit(parts._replace(path=f"/{database}")), unquote(schema)


def database_name(dsn: str) -> str | None:
    """The database of a URL-form DSN, or None."""
    if "://" not in dsn:
        return None
    return unquote(urlsplit(dsn).path.strip("/")) or None
